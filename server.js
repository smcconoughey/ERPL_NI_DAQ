#!/usr/bin/env node
// WebSocket server that receives DAQ data from Python and broadcasts to browsers

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const net = require('net');
const path = require('path');
const fs = require('fs');
const { createWriteStream } = require('fs');

const WEB_PORT = 3000;
const TCP_PORT = 5001;  // Updated to avoid port conflicts
const MAX_CLIENTS = 100;
const HEARTBEAT_INTERVAL = 30000;
const BATCH_INTERVAL_MS = 100;

class DAQWebSocketServer {
    constructor() {
        this.app = express();
        this.server = http.createServer(this.app);
        this.wss = new WebSocket.Server({ 
            server: this.server,
            maxClients: MAX_CLIENTS
        });
        
        this.clients = new Set();
        this.lastMessage = null;           // last full merged frame
        this.lastPT = null;                // last PT-only frame
        this.lastLC = null;                // last LC-only frame
        this.messageCount = 0;
        // Batch state for compact transport to UI
        this.batch = { startTsUs: 0, dtUs: 10000, data: Object.create(null), channels: new Set(), count: 0 };
        this.lastSampleTsUs = 0;
        
        // CSV logging
        this.loggingEnabled = false;
        this.logStream = null;
        this.logFilename = null;
        this.logDataCount = 0;
        this.logStartTime = null;
        this.logHeader = null;
        this.headerBuilt = false;
        
        this.setupExpress();
        this.setupWebSocket();
        this.setupTCPServer();
        this.setupHeartbeat();
        this.setupBatchFlush();
    }
    
    setupExpress() {
        this.app.use(express.static(path.join(__dirname, 'public')));
        
        // Serve interface config file
        this.app.get('/interface_config.json', (req, res) => {
            res.sendFile(path.join(__dirname, 'interface_config.json'));
        });
        
        // API route for config
        this.app.get('/api/config', (req, res) => {
            res.sendFile(path.join(__dirname, 'interface_config.json'));
        });
        
        this.app.get('/api/status', (req, res) => {
            res.json({
                clients: this.clients.size,
                messages: this.messageCount,
                uptime: process.uptime(),
                lastMessage: this.lastMessage
            });
        });
        
        this.app.get('/', (req, res) => {
            res.sendFile(path.join(__dirname, 'public', 'index.html'));
        });
    }
    
    setupWebSocket() {
        this.wss.on('connection', (ws, req) => {
            console.log(`New WebSocket connection from ${req.socket.remoteAddress}`);
            
            this.clients.add(ws);
            
            ws.send(JSON.stringify({
                type: 'welcome',
                message: 'Connected to DAQ WebSocket server',
                timestamp: new Date().toISOString()
            }));
            
            if (this.lastMessage) {
                ws.send(JSON.stringify({
                    type: 'data',
                    data: this.lastMessage,
                    timestamp: new Date().toISOString()
                }));
            }
            
            ws.on('message', (message) => {
                try {
                    const msg = JSON.parse(message.toString());
                    
                    if (msg.action === 'start_logging') {
                        try {
                            const p = path.join(__dirname, 'start_logging.cmd');
                            fs.writeFileSync(p, 'start');
                            ws.send(JSON.stringify({ success: true, message: 'Requested Python to start logging' }));
                        } catch (e) {
                            ws.send(JSON.stringify({ success: false, message: e.message }));
                        }
                    } else if (msg.action === 'stop_logging') {
                        try {
                            const p = path.join(__dirname, 'stop_logging.cmd');
                            fs.writeFileSync(p, 'stop');
                            ws.send(JSON.stringify({ success: true, message: 'Requested Python to stop logging' }));
                        } catch (e) {
                            ws.send(JSON.stringify({ success: false, message: e.message }));
                        }
                    } else if (msg.action === 'get_logging_status') {
                        const status = this.getLoggingStatusFromFile();
                        ws.send(JSON.stringify(status));
                    } else if (msg.action === 'tare_lc') {
                        const result = this.tareLCs();
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'tare_pt') {
                        const result = this.tarePTs();
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'tare_lc_channel' && typeof msg.channel === 'number') {
                        const result = this.tareLCChannel(msg.channel);
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'tare_pt_channel' && typeof msg.channel === 'number') {
                        const result = this.tarePTChannel(msg.channel);
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'get_tare_config') {
                        const config = this.getTareConfig();
                        ws.send(JSON.stringify({ type: 'tare_config', data: config }));
                    } else {
                        console.log('Received message from client:', message.toString());
                    }
                } catch (error) {
                    console.error('Error handling client message:', error);
                }
            });
            
            ws.on('close', () => {
                this.clients.delete(ws);
                console.log(`WebSocket client disconnected. Active clients: ${this.clients.size}`);
            });
            
            ws.on('error', (error) => {
                console.error('WebSocket error:', error);
                this.clients.delete(ws);
            });
        });
        
        console.log('WebSocket server initialized');
    }
    
    setupTCPServer() {
        this.tcpServer = net.createServer((socket) => {
            console.log('Python client connected');
            
            socket.on('data', (data) => {
                try {
                    const messages = data.toString().split('\n').filter(msg => msg.trim());
                    
                    messages.forEach(msg => {
                        const parsedData = JSON.parse(msg);

                        const src = (parsedData && parsedData.source) ? String(parsedData.source) : '';

                        if (/Merged/i.test(src)) {
                            // Accept fully merged frame and ingest into batch
                            this.lastMessage = parsedData;
                            this.messageCount++;
                            this.ingestMerged(parsedData);
                            // Also broadcast real-time data for immediate UI updates
                            this.broadcast({ type: 'data', data: parsedData, timestamp: new Date().toISOString() });
                        } else {
                            // Merge PT and LC frames so UI gets unified payload
                            if (parsedData && parsedData.channels && parsedData.channels.length) {
                                if (/PT Card|Pressure/i.test(src)) {
                                    this.lastPT = parsedData;
                                } else if (/LC Card|Load/i.test(src)) {
                                    this.lastLC = parsedData;
                                }
                            }

                            // Build merged frame
                            let merged = { timestamp: new Date().toISOString(), channels: [] };
                            if (this.lastPT && Array.isArray(this.lastPT.channels)) {
                                merged.channels = merged.channels.concat(this.lastPT.channels);
                            }
                            if (this.lastLC && Array.isArray(this.lastLC.channels)) {
                                merged.channels = merged.channels.concat(this.lastLC.channels);
                            }
                            this.lastMessage = merged;
                            this.messageCount++;
                            this.ingestMerged(merged);
                        }
                    });
                    
                } catch (error) {
                    console.error('Error parsing data from Python:', error);
                }
            });
            
            socket.on('close', () => {
                console.log('Python client disconnected');
            });
            
            socket.on('error', (error) => {
                console.error('TCP socket error:', error);
            });
        });
        
        this.tcpServer.listen(TCP_PORT, () => {
            console.log(`TCP server listening on port ${TCP_PORT} for Python data`);
        });
    }
    
    setupHeartbeat() {
        setInterval(() => {
            this.broadcast({
                type: 'heartbeat',
                timestamp: new Date().toISOString(),
                clients: this.clients.size
            });
        }, HEARTBEAT_INTERVAL);
    }
    
    startLogging() { return { success: false, message: 'CSV logging handled by Python streamer' }; }
    
    stopLogging() { return { success: false, message: 'CSV logging handled by Python streamer' }; }
    
    getLoggingStatus() { return { active: false, filename: null, rows: 0, elapsed_sec: 0, message: 'CSV logging handled by Python streamer' }; }
    
    // Override getLoggingStatus to read Python's status file if present
    getLoggingStatusFromFile() {
        try {
            const statusPath = path.join(__dirname, 'logging_status.json');
            if (fs.existsSync(statusPath)) {
                const raw = fs.readFileSync(statusPath, 'utf-8');
                const data = JSON.parse(raw);
                return {
                    active: !!data.active,
                    filename: data.filename || null,
                    rows: data.rows || 0,
                    elapsed_sec: data.elapsed_sec || 0
                };
            }
        } catch (e) {
            console.error('Failed to read logging status:', e);
        }
        return { active: false, filename: null, rows: 0, elapsed_sec: 0 };
    }
    
    writeLogEntry() { /* disabled; logging in Python */ }
    
    tareLCs() {
        try {
            // Targeted LC tare command file to avoid PT overrides
            const tareCmdPath = path.join(__dirname, 'tare_lc.cmd');
            fs.writeFileSync(tareCmdPath, 'tare_lc');
            console.log('LC tare command sent to DAQ streamer');
            return { success: true, message: 'Tare command sent. Load cells will be tared on next data read.' };
        } catch (error) {
            console.error('Failed to send tare command:', error);
            return { success: false, message: error.message };
        }
    }

    tarePTs() {
        try {
            const tareCmdPath = path.join(__dirname, 'tare_pt.cmd');
            fs.writeFileSync(tareCmdPath, 'tare_pt');
            console.log('PT tare command sent to DAQ streamer');
            return { success: true, message: 'Tare command sent. PTs will be tared on next data read.' };
        } catch (error) {
            console.error('Failed to send PT tare command:', error);
            return { success: false, message: error.message };
        }
    }

    tareLCChannel(channel) {
        try {
            const tareCmdPath = path.join(__dirname, `tare_lc_ch${channel}.cmd`);
            fs.writeFileSync(tareCmdPath, `tare_lc_channel:${channel}`);
            console.log(`LC channel ${channel} tare command sent to DAQ streamer`);
            return { success: true, message: `LC${channel} will be tared on next data read.` };
        } catch (error) {
            console.error(`Failed to send LC channel ${channel} tare command:`, error);
            return { success: false, message: error.message };
        }
    }

    tarePTChannel(channel) {
        try {
            const tareCmdPath = path.join(__dirname, `tare_pt_ch${channel}.cmd`);
            fs.writeFileSync(tareCmdPath, `tare_pt_channel:${channel}`);
            console.log(`PT channel ${channel} tare command sent to DAQ streamer`);
            return { success: true, message: `PT${channel} will be tared on next data read.` };
        } catch (error) {
            console.error(`Failed to send PT channel ${channel} tare command:`, error);
            return { success: false, message: error.message };
        }
    }

    getTareConfig() {
        try {
            const tareConfigPath = path.join(__dirname, 'tare_config.json');
            if (fs.existsSync(tareConfigPath)) {
                const raw = fs.readFileSync(tareConfigPath, 'utf-8');
                return JSON.parse(raw);
            }
            // If file doesn't exist, return empty config
            return {
                pt_offsets: {},
                lc_offsets: {},
                timestamp: new Date().toISOString(),
                note: 'No tare configuration found'
            };
        } catch (error) {
            console.error('Failed to read tare config:', error);
            return {
                pt_offsets: {},
                lc_offsets: {},
                error: error.message
            };
        }
    }
    
    broadcast(message) {
        const messageStr = JSON.stringify(message);
        
        this.clients.forEach(ws => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(messageStr);
            } else {
                this.clients.delete(ws);
            }
        });
    }

    // Ingest a merged frame into batching structures
    ingestMerged(merged){
        try{
            const nowUs = (typeof merged.timestamp === 'number') ? Math.floor(merged.timestamp*1e6) : Math.floor(Date.now()*1000);
            if (this.batch.count === 0){
                this.batch.startTsUs = nowUs;
                // Try to estimate dt from previous sample; default 10ms
                const dt = (this.lastSampleTsUs>0) ? Math.max(1000, Math.min(50000, nowUs - this.lastSampleTsUs)) : 10000;
                this.batch.dtUs = dt;
            }
            this.lastSampleTsUs = nowUs;
            const chs = merged.channels || [];
            for (const ch of chs){
                // Derive a compact channel key
                let key = null;
                let value = null;
                if (typeof ch.channel === 'number' && ch.pressure_psi !== undefined) {
                    key = `PT${ch.channel}_psi`;
                    value = Number(ch.pressure_psi);
                } else if (typeof ch.channel === 'number' && ch.lbf !== undefined) {
                    key = `LC${ch.channel}_lbf`;
                    value = Number(ch.lbf);
                } else if (typeof ch.channel === 'number' && ch.temp_f !== undefined) {
                    key = `TC${ch.channel}_degF`;
                    value = Number(ch.temp_f);
                } else if (ch.name) {
                    key = ch.name.replace(/\s+/g,'_');
                }
                if (!key || value === null) continue;
                this.batch.channels.add(key);
                if (!this.batch.data[key]) this.batch.data[key] = [];
                this.batch.data[key].push(value);
            }
            this.batch.count++;
        }catch(e){/* ignore */}
    }

    setupBatchFlush(){
        setInterval(()=>{
            try{
                if (this.batch.count === 0) return;
                const payload = {
                    ts_unix_us: this.batch.startTsUs,
                    dt_us: this.batch.dtUs,
                    channels: Array.from(this.batch.channels),
                    data: this.batch.data
                };
                this.broadcast({ type: 'batch', data: payload, timestamp: new Date().toISOString()});
            }catch(e){ console.error('Batch flush error', e); }
            finally{
                // reset batch
                this.batch = { startTsUs: 0, dtUs: this.batch.dtUs, data: Object.create(null), channels: new Set(), count: 0 };
            }
        }, BATCH_INTERVAL_MS);
    }
    
    start() {
        this.server.listen(WEB_PORT, () => {
            console.log(`=== DAQ WebSocket Server Started ===`);
            console.log(`Web UI available at: http://localhost:${WEB_PORT}`);
            console.log(`WebSocket endpoint: ws://localhost:${WEB_PORT}`);
            console.log(`TCP data port: ${TCP_PORT}`);
            console.log(`Active clients: ${this.clients.size}`);
        });
    }
    
    stop() {
        console.log('Shutting down server...');
        this.tcpServer.close();
        this.wss.close();
        this.server.close();
    }
}

const server = new DAQWebSocketServer();
server.start();

process.on('SIGINT', () => {
    console.log('\nReceived SIGINT, shutting down gracefully...');
    server.stop();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\nReceived SIGTERM, shutting down gracefully...');
    server.stop();
    process.exit(0);
}); 