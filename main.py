from multiprocessing import Pool
from py2neo import Graph, Node, Relationship
import pandas as pd
import time
import pyarrow.csv as csv
from io import StringIO
import matplotlib.pyplot as plt

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

def upload_nodes(nodes):
    tx = graph.begin()
    for index, row in nodes.iterrows():
        node = Node("Node", id=int(row['osmid']), lat=float(row['lat']), lon=float(row['lon']))
        tx.create(node)
    tx.commit()

def upload_edges(edges):
    tx = graph.begin()
    for index, row in edges.iterrows():
        x = graph.nodes.match("Node", id=row['x']).first()
        y = graph.nodes.match("Node", id=row['y']).first()
        relationship = Relationship(x, "Road", y, name=str(row['name']), distance=float(row['weighted_dist']), id=int(row['osmid_x']))
        tx.create(relationship)
        if row['oneway'] != 'Yes':
            relationship = Relationship(y, "Road", x, name=str(row['name']), distance=float(row['weighted_dist']), id=int(row['osmid_x']))
            tx.create(relationship)
    tx.commit()

chunksize = 20
batch_size = 20
num_workers = 10

if __name__ == '__main__':
    
#-------------------------------------Parallel Processing-------------------------------------

    t0 = time.time()
    nodes = pd.read_csv('nodes.csv', chunksize=chunksize)

    with Pool(num_workers) as pool:
        t1 = time.time()
        print('Uploading nodes')
        #node_batches = [nodes[i:i+batch_size] for i in range(0, chunksize, batch_size)]
        #with Pool(num_workers) as pool:
        pool.map(upload_nodes, nodes)
        #for batch in node_batches:
        #   upload_nodes(batch)
        t2 = time.time() - t1
        print(f'Nodes uploaded in {t2} seconds')

    node_time = time.time() - t0
    print(f'Processing completed in {node_time} seconds')

    t0 = time.time()
    edges = pd.read_csv('edges.csv', chunksize=chunksize)

    with Pool(num_workers) as pool:

        t1 = time.time()
        print('Uploading edges')
        #edge_batches = [edges[i:i+batch_size] for i in range(0, chunksize, batch_size)]
        #with Pool(num_workers) as pool:
        pool.map(upload_edges, edges)
        #for batch in edge_batches:
        #    upload_edges(batch)
        t2 = time.time() - t1
        print(f'Edges uploaded in {t2} seconds')

    edge_time = time.time() - t1
    print(f'Processing completed in {edge_time} seconds')

## Benchmarking

#---------------------------------Bulk Upload-------------------------------------------------------

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    reset = '''
    MATCH (n)
    DETACH DELETE n
    '''

    node_query = '''
    LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' as row
    CALL {
        with row
        MERGE (node:Node {id:toInteger(row.osmid), lat: toFloat(row.lat), lon: toFloat(row.lon)})
    } IN TRANSACTIONS OF 1000 ROWS
    '''

    node_count_query = 'MATCH (n:Node) return Count(*) as count'

    with driver.session(database='neo4j') as session:
        session.run(reset)
        t1 = time.time()
        session.run(node_query)
        bulk = time.time() - t1
        print(f'Nodes uploaded in {bulk} seconds')
        result = session.run(node_count_query)    
        data = result.data()
        cnt = data[0]['count']
        print("Number of nodes: \n",cnt)
        session.run(reset)

#----------------------------------Regular upload-----------------------------------

    t0 = time.time()
    nodes = pd.read_csv('nodes.csv', chunksize=chunksize)
    for chunk in nodes:
        upload_nodes(chunk)
    end = time.time() - t0
    print(f'Nodes uploaded in {end} seconds')

    print(str(node_time) + ' ' + str(bulk) + ' ' + str(end))

    data = {
        'Upload': ['Parallel', 'Bulk', 'Regular'],
        'Time (seconds)': [node_time, bulk, end]
    }

    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i]//2, y[i], ha = 'center')

    df = pd.DataFrame(data)
    ax = df.plot(x='Upload', y='Time (seconds)', kind='bar', legend=True)
    plt.xlabel('Upload Type')
    plt.ylabel('Time (seconds)')
    plt.title('Upload time for nodes (17,414 nodes)')
    addlabels(df['Upload'], df['Time (seconds)'])
    plt.savefig('upload_times.png', bbox_inches='tight')
    plt.show()