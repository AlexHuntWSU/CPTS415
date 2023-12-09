from neo4j import GraphDatabase
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import osmnx as ox
import numpy as np
from shapely.wkt import loads

def highway(highway):
    if type(highway) == list:
        return highway[0]
    else:
        return highway
    
def max_speed(speed):
    try:
        if type(speed) == list:
            return int(np.mean(list(map(lambda x: int(x.split()[0]), speed))))
        elif pd.notna(speed):
            x = speed.split()[0]
            if int(x):
                return x
        else:
            return pd.NA
    except:
        return pd.NA
    
def weighted_distance(length, speed):
    return round(float(length)/float(speed), 2)

def convert_linestring(line):
    return str(list(loads(str(line)).coords))

def upload_edge(u, v, dist, road):
    return f'''MATCH (u:Node {{id:{u}}}), (v:Node {{id:{v}}})
              CREATE (u)-[:Road {{distance: {dist}, road: '{road}'}}]->(v)'''

def upload_node(id, lat, lon):
    return f'''CREATE (n:Node {{id: {id}, lat: {lat}, lon:{lon}}})'''

def upload_nodes(df_nodes, batch_size=100):
    query_template = (
        "UNWIND $batch AS row "
        "CREATE (n:Node {id: row.osmid, lat: row.y, lon: row.x})"
    )

    nodes = df_nodes[['osmid', 'y', 'x']].to_dict(orient='records')

    for i in range(0, len(nodes), batch_size):
        batch = nodes[i:i + batch_size]
        session.run(query_template, {'batch': batch})

def upload_edges(df_edges, batch_size=100):
    query_template = (
        "UNWIND $batch AS row "
        "MATCH (u:Node {id: row.u}), (v:Node {id: row.v}) "
        "CREATE (u)-[:Road {weighted_distance: row.weighted_distance, road: row.road}]->(v)"
    )

    edges = df_edges[['u', 'v', 'weighted_distance', 'road']].to_dict(orient='records')

    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        session.run(query_template, {'batch': batch})

############## Process Data ###################

#G = ox.graph_from_place("Washington State", network_type="drive")
G = ox.graph_from_place("Whitman, WA", network_type="drive")

df_nodes, df_edges = ox.graph_to_gdfs(G)
df_nodes.reset_index(inplace=True)
df_edges.reset_index(inplace=True)


df_edges['highway_updated'] = df_edges.apply(lambda row: highway(row['highway']), axis=1)
df_edges['maxspeed_updated'] = df_edges.apply(lambda row: max_speed(row['maxspeed']), axis=1)
df = df_edges[['highway_updated','maxspeed_updated']].dropna().astype({'maxspeed_updated': 'int32'}).groupby(['highway_updated']).mean().astype(int)

speed_map = df.to_dict()['maxspeed_updated']

df_edges['maxspeed_updated'] = df_edges['maxspeed_updated'].fillna(df_edges['highway_updated'].map(speed_map))
df_edges['weighted_distance'] = df_edges.apply(lambda row: weighted_distance(row['length'], row['maxspeed_updated']), axis=1)
df_edges['road'] = df_edges.apply(lambda row: convert_linestring(row['geometry']), axis=1)
df_edges = df_edges[['u', 'v', 'oneway', 'weighted_distance', 'road']]

################# Upload to Neo4j ###################

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password")) #Set the password to the one you set on http://localhost:7474/

index = 'CREATE INDEX idx_nodes FOR (n:Node) on (n.id)'

session = driver.session(database='neo4j')
session.run(index)

upload_nodes(df_nodes)
upload_edges(df_edges)

session.close()
