from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

#Index on node id for faster queries used in shortest path
index = 'CREATE INDEX idx_nodes FOR (n:Node) on (n.id)'

#Example values
latitude = 47.64
longitude = -122.30

# Given a coordinate, find the closest node. This allows a user to choose any coordinates on a map and get the approximate shortest path
test = f'''
MATCH (n)
WHERE n.lon IS NOT NULL
AND n.lat IS NOT NULL
RETURN n.id
ORDER BY point.distance(point({{ longitude: n.lon, latitude: n.lat }}), point({{ longitude: {longitude}, latitude: {latitude} }}))
LIMIT 1;
'''

with driver.session(database='neo4j') as session:
    #session.run(index)
    x = session.run(test)
    print(x.data())