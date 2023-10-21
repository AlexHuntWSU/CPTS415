import xmltodict
import pandas as pd
import json
from geopy.distance import geodesic

with open("map.xml") as xml_file:    
    data_dict = xmltodict.parse(xml_file.read())
    
nodes = []
for node in data_dict['osm']['node']:
    row = {}
    try:
        for tag in node['tag']:
            row[tag['@k']] = tag['@v']
    except:
        pass
    
    row['osmid'] = node['@id']
    row['lat'] = node['@lat']
    row['lon'] = node['@lon']
    nodes.append(row)
    
df_nodes = pd.DataFrame(nodes)
df_nodes['point'] = df_nodes.apply(lambda row: tuple([row['lat'], row['lon']]), axis=1)

ways = []
for way in data_dict['osm']['way']:
    tags = {}
    try:
        for tag in way['tag']:
            tags[tag['@k']] = tag['@v']
    except:
        pass
    for i in range(len(way['nd'])):
        try:
            d = {}
            d['osmid'] = way['@id']
            d['x'] = way['nd'][i]['@ref']
            d['y'] = way['nd'][i+1]['@ref']
            #d['x_index'] = i
            #d['y_index'] = i + 1
            ways.append({**d, **tags})
        except:
            pass
df_ways = pd.DataFrame(ways)
df_ways = df_ways[df_ways.highway.notna()]


#Add the points from the two coordinates
df_edges = pd.merge(pd.merge(df_ways, df_nodes[['osmid', 'point']], left_on='x', right_on='osmid', how='inner'),
               df_nodes[['osmid', 'point']], left_on='y', right_on='osmid', how='inner')
#Create line from two points
df_edges['line'] = df_edges.apply(lambda row: ' '.join(row['point_x']) + ', ' + ' '.join(row['point_y']), axis=1)
#Get distance between two points
df_edges['distance'] = df_edges.apply(lambda row: geodesic(row['point_x'], row['point_y']).miles, axis=1)
#Drop unused columns
df_edges = df_edges.drop(['osmid_y', 'osmid', 'point_x', 'point_y'], axis=1)

def max_speed(highway, speed):
    if pd.notna(speed):
        return float(str(speed).strip('mph'))
    speeds = {'tertiary':30.0,
    'service':20,
    'residential':20,
    'secondary':25,
    'primary':60,
    'trunk':60,
    'trunk_link':60,
    'primary_link':60,
    'secondary_link':25}
    if highway not in speeds.keys():
        return 0.0
    return float(speeds[highway])

df_edges['maxspeed'] = df_edges.apply(lambda row: max_speed(row['highway'], row['maxspeed']), axis=1)
df_edges['weighted_dist'] = df_edges.apply(lambda row: (row['maxspeed'] if pd.notna(row['maxspeed']) else 0) * (row['distance'] if pd.notna(row['distance']) else 0), axis=1)

nodes = df_nodes[df_nodes['osmid'].isin(df_edges['x']) | df_nodes['osmid'].isin(df_edges['y'])]
nodes = nodes.loc[:,['osmid', 'lat', 'lon']]
edges = df_edges.loc[df_edges.weighted_dist != 0.0, ['osmid_x', 'x', 'y', 'name', 'weighted_dist', 'oneway', 'highway']]