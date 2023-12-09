from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, lead, concat
from pyspark.sql import Window
import pymongo
from neo4j import GraphDatabase

spark = SparkSession.builder.master('spark://spark-master:7077').getOrCreate()

# Problems with the mongodb connector.
client = pymongo.MongoClient("mongodb://root:password@mongodb:27017")
db = client["mongo"]
coll = db["paths"]

l = []
for x in coll.find():
    #print(x)
    l.append((x['node1'], x['node2'], [int(i) for i in x['path']]))

#print(l)
df = spark.createDataFrame(l,schema=["node1", "node2", "path"])
#df.show()
window = Window.partitionBy("node1","node2").orderBy("node1","node2")

df_edges = df.select("node1","node2", explode("path").alias("node"))
df_edges = df_edges.withColumn("next_node", lead("node").over(window)).na.drop()
df_edges = df_edges.withColumn("edge", concat(lit("("), col("node"), lit(","), col("next_node"), lit(")")))
#df_edges.show()

df_edge_counts = df_edges.groupBy("edge").count()
#df_edge_counts.show()

total_paths = df.count()
df_edge_betweenness = df_edge_counts.withColumn(
    "edge_betweenness_centrality",
    col("count") / lit(total_paths)
)

df_edge_betweenness.show()

def updateNeo4j(df):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
    
    edge = df.edge.strip('(').strip(')').split(',')
    centrality = df.edge_betweenness_centrality
    a = edge[0]
    b = edge[1]
    
    query = f"""
    MATCH (a:Node)-[r:Road]->(b:Node) 
    WHERE a.id = {a}
    AND b.id = {b}
    SET r.centrality = {centrality}
    """

    with driver.session(database='neo4j') as session:
        result = session.run(query)    

df_edge_betweenness.foreach(updateNeo4j)
spark.stop()
# Install neo4j on worker nodes