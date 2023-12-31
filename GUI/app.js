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

async function getCentralityFromNeo4j(bounds){
  const session = driver.session()
  const result = await session.run(
    `
    MATCH (start)-[edge]->(end)
    WHERE start.lat >= $minLat AND start.lat <= $maxLat
      AND start.lon >= $minLon AND start.lon <= $maxLon
      AND end.lat >= $minLat AND end.lat <= $maxLat
      AND end.lon >= $minLon AND end.lon <= $maxLon
      AND edge.centrality IS NOT NULL
    RETURN edge
    `,
    {
      minLat: bounds.southWest.lat,
      maxLat: bounds.northEast.lat,
      minLon: bounds.southWest.lng,
      maxLon: bounds.northEast.lng,
    }
  );

  const edges = result.records.map(record => {
    const edge = record.get('edge').properties;
    return {
      centrality: edge.centrality,
      road: edge.road
  }});

  //console.log(edges);
  session.close();
  return edges;
}

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
    //console.log(result.records[0])
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
        // Return null if the route doesn't exist
        return null;
      }
    } finally {
      await client.close();
      console.log('Disconnected from MongoDB');
    }
}

app.post('/getcentrality', async (req, res) => {
  const { bounds } = req.body;
  //console.log(bounds)
  try {
    const results = await getCentralityFromNeo4j(bounds);
    //console.log(results)
    return res.status(200).json({ message: results });
  }
  catch (error) {
    console.error('Error getting centrality:', error);
    res.status(500).send('Error getting centrality');
}
});

app.post('/getroute', async (req, res) => {
    //const coordinates = req.body;
    console.log('test');
    const { markers } = req.body;
    const results = await Promise.all(markers.map(getNodesFromNeo4j));
    if (results[0] < 0 || results[1] < 0){
      return res.status(500).send('Error finding closest node');
    }else{
      console.log('passed')
    }
    console.log(results)
    const Result = markers.reduce((acc, marker, index) => {
        acc[`node${index + 1}`] = results[index]; 
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
                clearInterval(interval); 
                return res.status(200).json({ message: result.path });
              } else {
                console.log('Route does not exist.');
              }
            } catch (error) {
              console.error('Error:', error);
            }
          }, 1000); // 1 second
      

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


app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

connectToKafka();

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
