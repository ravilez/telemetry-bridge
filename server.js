const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { createClient } = require('redis');

const app = express();
app.use(express.json()); // Essential for parsing incoming raw JSON strings
app.use(cors());
const server = http.createServer(app);


// Configure precise cross-origin layout permissions
app.use(cors({
  origin: 'http://localhost:4200', // Change to your exact frontend Angular app development port address
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));

// 1. Initialize WebSocket Server for Angular Client
const io = new Server(server, {
    cors: { origin: "http://localhost:4200", methods: ["GET", "POST"] }
});

// 2. Connect to your Redis DB
const redisClient = createClient({ url: 'redis://127.0.0.1:6379' });
redisClient.on('error', (err) => console.error('Redis Client Error', err));
redisClient.connect().then(() => console.log('Connected to Redis DB successfully.'));

// 3. Connect to your Pi 5 MQTT Broker
const mqttBrokerUrl = 'mqtt://192.168.1.111:1883'; 
const mqttClient = mqtt.connect(mqttBrokerUrl);
const mqttTopic = 'NodeMCU-Portalegre-1/temperature';


mqttClient.on('connect', () => {
    console.log(`Connected to MQTT Broker at ${mqttBrokerUrl}`);
    mqttClient.subscribe(mqttTopic, (err) => {
        if (!err) console.log(`Subscribed cleanly to topic: ${mqttTopic}`);
    });
});

mqttClient.on('message', async (topic, payload) => {
    // 1. Log immediately when ANY raw data hits the network card
    console.log(`📡 [NETWORK RAW EVENT] Topic: "${topic}" | Bytes: ${payload.length}`);

    const rawData = payload.toString();
    console.log(`📝 [PARSED STRING]: "${rawData}"`);

    const telemetryPayload = {
        temperature: parseFloat(rawData) || 0,
        timestamp: new Date().toISOString()
    };

    try {
        // 2. Log database writes
        await redisClient.set('latest_telemetry', JSON.stringify(telemetryPayload));
        await redisClient.lPush('telemetry_history', JSON.stringify(telemetryPayload));
        await redisClient.lTrim('telemetry_history', 0, 999);
        console.log(`💾 [REDIS WRITER]: Successfully cached data row.`);

        // 3. Log frontend socket emissions
        io.emit('telemetry_update', telemetryPayload);
        console.log(`🚀 [SOCKET DISPATCH]: Broadcasted payload to Angular.`);

    } catch (error) {
        console.error('❌ [CRITICAL BACKEND ERROR]:', error);
    }
});

app.get('/api/devices', async (req, res) => {
  try {
    // 1. Fetch your device keys (ensure your pattern only hits devices, e.g., 'device:*')
    const keys = await redisClient.keys('device:*'); 
    const devices = [];

    for (const key of keys) {
      // 2. Ask Redis what type of data this key holds to prevent WRONGTYPE errors
      const keyType = await redisClient.type(key);
      let rawData;

      if (keyType === 'hash') {
        // If it's a Redis Hash, use hGetAll
        const hashData = await redisClient.hGetAll(key);
        devices.push(hashData);
        continue; // Move to next key
      } else if (keyType === 'string') {
        // If it's a simple string, use GET
        rawData = await redisClient.get(key);
      } else {
        // Skip lists, sets, or other telemetry types that might be caught in the pattern
        console.warn(`Skipping key ${key} because it is a ${keyType}, not a device.`);
        continue;
      }

      // 3. If it was a string, parse it safely
      if (rawData) {
        try {
          devices.push(JSON.parse(rawData));
        } catch (parseError) {
          // Fallback if the string isn't valid JSON
          devices.push({ id: key, name: rawData });
        }
      }
    }

    // 4. Send the successful array cleanly back to Angular
    res.status(200).json(devices);

  } catch (dbError) {
    console.error('Redis fetch operation failed:', dbError);
    res.status(500).json({ error: 'Internal Redis retrieval failure' });
  }
});


// POST endpoint for registering devices
app.post('/api/devices/register', async (req, res) => {
  const { id, name, type, location } = req.body;

  // Basic validation
  if (!id || !name || !type) {
    return res.status(400).json({ error: "Missing required fields (id, name, type)" });
  }

  try {
    const deviceKey = `device:${id}`;

    // 1. Save device metadata into a Redis Hash
    await redisClient.hSet(deviceKey, {
      id: id,
      name: name,
      type: type,
      location: location || 'Unknown',
      status: 'offline', // Default state until it connects via MQTT
      registeredAt: new Date().toISOString()
    });

    // 2. Add device ID to a Set tracking all registered devices
    await redisClient.sAdd('devices:all', id);

    console.log(`Device Registered in Redis: ${id}`);
    res.status(201).json({ success: true, message: `Device ${id} successfully registered.` });

  } catch (error) {
    console.error('Redis registration error:', error);
    res.status(500).json({ error: 'Internal Server Error saving to Redis' });
  }
});

// HTTP fallback endpoint to read history directly on demand
app.get('/api/history', async (req, res) => {
    try {
        const history = await redisClient.lRange('telemetry_history', 0, 40);
        const parsed = history.map(item => JSON.parse(item));
        res.json(parsed);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


// --- GET HISTORY: Fetch the latest 'X' records from the Redis List ---
app.get('/api/telemetry/history', async (req, res) => {
    try {
        // LRANGE key start stop 
        // 0 is the newest item. 99 fetches the next 99 items (100 total)
        const rawHistory = await redisClient.lRange('telemetry_history', 0, 99);

        // Convert the raw JSON strings back into readable JavaScript objects
        const parsedHistory = rawHistory.map(item => JSON.parse(item));

        res.json(parsedHistory);
    } catch (err) {
        console.error('Failed to fetch Redis history:', err);
        res.status(500).json({ error: 'Database read failure' });
    }
});

// --- CLEAR HISTORY: Optional endpoint to wipe the list clean ---
app.delete('/api/telemetry/history', async (req, res) => {
    try {
        await redisClient.del('telemetry_history');
        res.json({ message: 'Telemetry history wiped successfully.' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

server.listen(3001, () => console.log('Node.js backend running smoothly on port 3001'));
