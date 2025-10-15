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

class DAQWebSocketServer {
    constructor() {
        this.app = express();
        this.server = http.createServer(this.app);
        this.wss = new WebSocket.Server({ 
            server: this.server,
            maxClients: MAX_CLIENTS
        });
        
        this.clients = new Set();
        this.lastMessage = null;
        this.messageCount = 0;
        
        // CSV logging
        this.loggingEnabled = false;
        this.logStream = null;
        this.logFilename = null;
        this.logDataCount = 0;
        this.logStartTime = null;
        this.logHeader = null;
        
        this.setupExpress();
        this.setupWebSocket();
        this.setupTCPServer();
        this.setupHeartbeat();
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
                        const result = this.startLogging();
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'stop_logging') {
                        const result = this.stopLogging();
                        ws.send(JSON.stringify(result));
                    } else if (msg.action === 'get_logging_status') {
                        const result = this.getLoggingStatus();
                        ws.send(JSON.stringify(result));
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
                        this.lastMessage = parsedData;
                        this.messageCount++;
                        
                        // Log data if enabled
                        if (this.loggingEnabled) {
                            this.writeLogEntry(parsedData);
                        }
                        
                        this.broadcast({
                            type: 'data',
                            data: parsedData,
                            timestamp: new Date().toISOString()
                        });
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
    
    startLogging() {
        if (this.loggingEnabled) {
            return { success: false, message: 'Logging already active', filename: this.logFilename };
        }
        
        try {
            const now = new Date();
            const dateFolder = `${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`;
            const timeStr = `${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}`;
            
            const logDir = path.join(__dirname, 'logs', dateFolder);
            if (!fs.existsSync(logDir)) {
                fs.mkdirSync(logDir, { recursive: true });
            }
            
            let logPath = path.join(logDir, `${timeStr}.csv`);
            let counter = 1;
            while (fs.existsSync(logPath)) {
                logPath = path.join(logDir, `${timeStr}_${counter}.csv`);
                counter++;
            }
            
            this.logStream = createWriteStream(logPath);
            this.logFilename = logPath;
            this.logDataCount = 0;
            this.logStartTime = Date.now();
            this.loggingEnabled = true;
            this.logHeader = null;
            
            console.log(`Started CSV logging: ${this.logFilename}`);
            return { success: true, message: 'Logging started', filename: this.logFilename };
        } catch (error) {
            console.error('Failed to start logging:', error);
            if (this.logStream) {
                this.logStream.end();
                this.logStream = null;
            }
            return { success: false, message: error.message };
        }
    }
    
    stopLogging() {
        if (!this.loggingEnabled) {
            return { success: false, message: 'Logging not active' };
        }
        
        try {
            this.loggingEnabled = false;
            if (this.logStream) {
                this.logStream.end();
                this.logStream = null;
            }
            
            console.log(`Stopped CSV logging. Wrote ${this.logDataCount} rows to ${this.logFilename}`);
            const filename = this.logFilename;
            const rows = this.logDataCount;
            this.logFilename = null;
            this.logDataCount = 0;
            this.logHeader = null;
            
            return { success: true, message: 'Logging stopped', rows: rows, filename: filename };
        } catch (error) {
            console.error('Failed to stop logging:', error);
            return { success: false, message: error.message };
        }
    }
    
    getLoggingStatus() {
        return {
            active: this.loggingEnabled,
            filename: this.loggingEnabled ? this.logFilename : null,
            rows: this.loggingEnabled ? this.logDataCount : 0,
            elapsed_sec: this.loggingEnabled ? (Date.now() - this.logStartTime) / 1000 : 0
        };
    }
    
    writeLogEntry(data) {
        if (!this.loggingEnabled || !this.logStream) {
            return;
        }
        
        try {
            const timestamp = new Date().toISOString();
            const elapsedMs = Date.now() - this.logStartTime;
            
            // Build CSV row
            const row = [timestamp, elapsedMs];
            
            if (data.channels && Array.isArray(data.channels)) {
                // Write header on first entry
                if (!this.logHeader) {
                    const header = ['timestamp', 'elapsed_ms'];
                    
                    // Determine device type from first channel
                    const firstCh = data.channels[0];
                    if (firstCh && 'pressure_psi' in firstCh) {
                        // PT data
                        for (let i = 0; i < 16; i++) {
                            header.push(`PT${i}_psi`);
                        }
                    } else if (firstCh && 'v_per_v' in firstCh) {
                        // LC data
                        for (let i = 0; i < 4; i++) {
                            header.push(`LC${i}_vv`);
                        }
                    }
                    
                    this.logHeader = header;
                    this.logStream.write(header.join(',') + '\n');
                }
                
                // Write data
                data.channels.forEach((ch, idx) => {
                    if ('pressure_psi' in ch) {
                        row.push(ch.pressure_psi || '');
                    } else if ('v_per_v' in ch) {
                        row.push(ch.v_per_v || '');
                    }
                });
            }
            
            this.logStream.write(row.join(',') + '\n');
            this.logDataCount++;
        } catch (error) {
            console.error('Failed to write log entry:', error);
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