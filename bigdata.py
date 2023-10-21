from pyrosm import OSM, get_data
import pandas as pd
import pyarrow as pa

data = get_data("washington")
osm = OSM(data)

nodes, edges = osm.get_network(nodes=True, network_type='driving')

nodes.to_csv('WA_nodes.csv')
edges.to_csv('WA_edges.csv')