from multiprocessing import Pool
from py2neo import Graph, Node, Relationship
import pandas as pd
import time

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

if __name__ == '__main__':

    nodes = pd.read_csv('nodes.csv')
    edges = pd.read_csv('edges.csv')

    batch_size = 50
    num_workers = 10

    t1 = time.time()
    print('Uploading nodes')
    node_batches = [nodes[i:i+batch_size] for i in range(0, len(nodes), batch_size)]
    with Pool(num_workers) as pool:
        pool.map(upload_nodes, node_batches)
    t2 = time.time() - t1
    print(f'Nodes uploaded in {t2}')

    t1 = time.time()
    print('Uploading edges')
    edge_batches = [edges[i:i+batch_size] for i in range(0, len(edges), batch_size)]
    # with Pool(num_workers) as pool:
    #     pool.map(upload_edges, edge_batches)
    for batch in edge_batches:
        upload_edges(batch)
    t2 = time.time() - t1
    print(f'Edges uploaded in {t2}')
        
