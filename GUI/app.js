const express = require('express');
const path = require('path');
const neo4j = require('neo4j-driver');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

const kafka = new Kafka({
    clientId: 'kafka',
    brokers: ['localhost:29092'], 
});

const producer = kafka.producer();

async function connectToKafka() {
    await producer.connect();
    await producer.send({
        topic: 'test',
        messages: [
          { value: 'Hello KafkaJS user!' },
        ],
      })
    console.log('Connected to Kafka');
};

app.post('/send-kafka', async (req, res) => {
    const coordinates = req.body;

    try {
        // Send the coordinates to the Kafka topic
        //JSON.stringify({ coordinates })
        await producer.send({
            topic: 'test',
            messages: [{ value: JSON.stringify(coordinates) }],
        });

        console.log('Message sent to Kafka successfully');
        res.status(200).send('Message sent to Kafka successfully');
    } catch (error) {
        console.error('Error sending message to Kafka:', error);
        res.status(500).send('Error sending message to Kafka');
    }
});
// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// Define a route for the homepage
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

connectToKafka();

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
