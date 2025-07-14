#!/usr/bin/env node
// WebSocket server that receives DAQ data from Python and broadcasts to browsers

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const net = require('net');
const path = require('path');

const WEB_PORT = 3000;
const TCP_PORT = 5000;
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
        
        this.setupExpress();
        this.setupWebSocket();
        this.setupTCPServer();
        this.setupHeartbeat();
    }
    
    setupExpress() {
        this.app.use(express.static(path.join(__dirname, 'public')));
        
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
                console.log('Received message from client:', message.toString());
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