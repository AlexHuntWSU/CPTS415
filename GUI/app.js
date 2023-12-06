const express = require('express');
const path = require('path');
const neo4j = require('neo4j-driver');
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

const kafka = new Kafka({
    clientId: 'kafka',
    brokers: ['localhost:29092'], 
});

const producer = kafka.producer();

const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("neo4j", "password"));

const mongoURI = 'mongodb://root:password@localhost:27017';

async function connectToKafka() {
    await producer.connect();
    // await producer.send({
    //     topic: 'test',
    //     messages: [
    //       { value: 'Hello KafkaJS user!' },
    //     ],
    //   })
    console.log('Connected to Kafka');
};

async function getNodesFromNeo4j(marker) {
    const session = driver.session()

    // Run the Neo4j query to find the nearest node
    const result = await session.run(`
      MATCH (n)
      WHERE n.lon IS NOT NULL AND n.lat IS NOT NULL
      RETURN n.id
      ORDER BY point.distance(
        point({ longitude: n.lon, latitude: n.lat }),
        point({ longitude: $lng, latitude: $lat })
      )
      LIMIT 1
    `, {
      lng: marker.lng,
      lat: marker.lat,
    });
  
    // Extract the Neo4j result
    const neo4jResult = result.records[0].get(0).toInt();
    // console.log(result.records[0].get(0).toInt());
  
    session.close();
    return neo4jResult;
};

async function producemessage(Result){
    await producer.send({
        topic: 'test',
        messages: [{ value: JSON.stringify(Result) }],
    });
};

async function getRoute(query){
    const client = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });
    try {
      await client.connect();
      console.log('Connected to MongoDB');
  
      const db = client.db('mongo');
      const collection = db.collection('routes');

      const result = await collection.findOne(query);

      if (result) {
        // Return the results if the route exists
        return result;
      } else {
        // Return null (or any other indicator) if the route doesn't exist
        return null;
      }
    } finally {
      await client.close();
      console.log('Disconnected from MongoDB');
    }
}

app.post('/getroute', async (req, res) => {
    //const coordinates = req.body;
    console.log('test');
    const { markers } = req.body;
    const results = await Promise.all(markers.map(getNodesFromNeo4j));
    console.log(results)
    const Result = markers.reduce((acc, marker, index) => {
        acc[`node${index + 1}`] = results[index]; // Assuming there is only one node in the result
        return acc;
      }, {});


    try {
        const query = Result;
        getRoute(query)
        .then(result => {
            if (result !== null) {
            console.log('Route found:', result);
            return res.status(200).json({ message: result.path });
            } else {
            console.log('Route does not exist.');
            const produce = producemessage(Result)
            }
        })
        .catch(error => console.error('Error:', error));
        // Send the coordinates to the Kafka topic
        //JSON.stringify({ coordinates })
        // await producer.send({
        //     topic: 'test',
        //     messages: [{ value: JSON.stringify(Result) }],
        // });

        const interval = setInterval(async () => {
            try {
              const query = Result;
              const result = await getRoute(query);
      
              if (result !== null) {
                console.log('Route found:', result);
                clearInterval(interval); // Stop the interval when the route is found
                return res.status(200).json({ message: result.path });
              } else {
                console.log('Route does not exist.');
              }
            } catch (error) {
              console.error('Error:', error);
            }
          }, 10000); // 10 seconds
      
          // Set up a timer to stop the interval after 10 minutes
          setTimeout(() => {
            clearInterval(interval);
          }, 600000); // 10 minutes

        console.log('Message sent to Kafka successfully');
        //res.status(500).send('Message sent to Kafka successfully');
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
