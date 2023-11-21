import heapq
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, lead, concat
from pyspark.sql import Window
import pymongo
from neo4j import GraphDatabase
from itertools import combinations
import time

#spark = SparkSession.builder.master('local[*]').getOrCreate()
spark = SparkSession.builder.master('spark://spark-master:7077').getOrCreate()
sc = spark.sparkContext

def get_node_sample(count):
    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
    query = f'MATCH (a) RETURN a.id as id ORDER BY rand() LIMIT {count}'
    with driver.session(database='neo4j') as session:
        result = session.run(query)
        data = result.data()
    return data

def get_neighbors(node):
    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
    query = f'''MATCH (source {{id: {node}}})-[r]->(next) RETURN DISTINCT next.id as id, r.distance as distance'''
    with driver.session(database='neo4j') as session:
        result = session.run(query)
        data = result.data()
    return data

def cache_shortest_path(x, y, path):
    client = pymongo.MongoClient("mongodb://root:password@mongodb:27017")
    db = client["mongo"]
    coll = db["paths"]

    insert = {'x':x, 'y':y, 'path':path}
    coll.insert_one(insert)

def check_cache(x, y):
    client = pymongo.MongoClient("mongodb://root:password@mongodb:27017")
    db = client["mongo"]
    coll = db["paths"]
    query = {"x": x, "y": y}
    try:
        result = coll.find(query)
        return result[0]['path']
    except:
        return None

def dijkstra(df):
    source = df.id1
    target = df.id2
    distance = {}
    distance[source] = 0
    path = [target]
    target_node = target
    prev = {}
    total_distance = 0

    x = check_cache(source, target)
    if x:
        cache_shortest_path(source, target, x)

    pq = [(0, source)]

    while pq:

        # This algorithm uses a priortity queue to find the nodes with the smallest distance
        # In this case, we don't have to loop over visited nodes to find the minimum distance (O(log N) vs O(N))
        dist, node = heapq.heappop(pq) 

        if node == target and target != None: #If target is None, the algorithm will find distances to all nodes. Needed for centrality algorithm
            break
        
        for neighbor in get_neighbors(node): #Get all neighbor nodes
            edge_weight = neighbor['distance'] #Get the weighted distance between the nodes and neighbor
            neighbor = neighbor['id']
            temp_distance = distance[node] + edge_weight
            if temp_distance < distance.get(neighbor, float('inf')): #Updates the temporary total distance
                distance[neighbor] = temp_distance
                total_distance = temp_distance
                prev[neighbor] = node #Used to trace the path
                # Add the neighbor to the priority queue with its updated distance
                heapq.heappush(pq, (temp_distance, neighbor))

    # Building shortest the path
    # prev = {'B': 'A', 'C': 'A', 'D': 'B'} = A -> B, B -> D; A -> C
    # Works backwards from target to the source node
    if target is not None:
        try:
            while target != source:
                target = prev[target]
                path.append(target)
            path.reverse()
        except:
            pass

    cache_shortest_path(source, target, path)
    #return path

data = get_node_sample(5)

ids = [row['id'] for row in data]
combinations_list = list(combinations(ids, 2))
combinations_list += [(id2, id1) for id1, id2 in combinations_list]
df = spark.createDataFrame(combinations_list, ["id1", "id2"])

df.show()

t = time.time()
df.foreach(dijkstra)
print(time.time() - t)
#1116 for 20 shortest paths