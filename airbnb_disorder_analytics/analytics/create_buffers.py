import os
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from geopy import distance
import sys
import random
from tqdm import tqdm
import osmnx as ox
from shapely.geometry import Point, Polygon
from geopandas import GeoSeries
from shapely.ops import cascaded_union, polygonize_full
import geopandas as gpd
import shapely
from pprint import pprint

streetscore_data_folder = '/home/rchen/Documents/github/airbnb_crime/data/streetscore_dataset'
streetscore_boston_file = 'streetscore_boston.csv'
streetscore_nyc_file = 'streetscore_newyorkcity.csv'

review_nyc_file = 'rscore_nyc.csv'


class FixedSizeBuffer:
    def __init__(self, node, buffer_size):
        self.source_node = node
        self.buffer_size = buffer_size  # metric: meters
        self.neighborhood_gdf = ox.graph_to_gdfs(ox.graph_from_point(self.source_node, dist=self.buffer_size, network_type='all'), nodes=False, fill_edge_geometry=True)
        self.neighborhood_geometry = self.neighborhood_gdf['geometry'] # long first, lat second
        self.convert_lines_to_polygon()

    def convert_lines_to_polygon(self):
        vertices = []
        for line_string in self.neighborhood_geometry:
            first_point, last_point = line_string.boundary
            vertices.append(first_point)
            vertices.append(last_point)
        poly = Polygon(vertices)
        print(poly)
        # Transform bounding box into the original coorinate system of your data
        #bounding_box.transform(orig_srid)


    def find_nodes_in_buffer(self, node_list, return_index=True):
        dist, _ = self.btree.query(node_list, k=1)
        sorted_indices = np.argsort(dist)
        cutoff_index = None
        for index, pos in enumerate(sorted_indices):
            dist_m = distance.distance(target_node[0], node_list[pos]).m
            if dist_m > self.buffer_size:
                cutoff_index = index
                break
        if cutoff_index is None:
            cutoff_index = len(sorted_indices)-1
        if return_index:
            return sorted_indices[:cutoff_index+1]  # sorted by distance to source node, ascending
        else:
            return np.array(node_list)[sorted_indices[:cutoff_index+1]]  # sorted by distance to source node, ascending

def significantly_smaller(pair_of_number, threshold=0.1):
    if abs(pair_of_number[0]-pair_of_number[1]) / max(pair_of_number) < threshold:
        return -1
    else:
        return int(pair_of_number[0]>pair_of_number[1])

from collections import namedtuple
import numpy as np
import scipy.stats as st

TtestResults = namedtuple("Ttest", "T p")

def t_welch(x, y, tails=2):
    """
    Welch's t-test for two unequal-size samples, not assuming equal variances
    """
    assert tails in (1,2), "invalid: tails must be 1 or 2, found %s"%str(tails)
    x, y = np.asarray(x), np.asarray(y)
    nx, ny = x.size, y.size
    vx, vy = x.var(ddof=1), y.var(ddof=1)
    df = ((vx/nx + vy/ny)**2 / # Welch-Satterthwaite equation
            ((vx/nx)**2 / (nx - 1) + (vy/ny)**2 / (ny - 1)))
    t_obs = (x.mean() - y.mean()) / np.sqrt(vx/nx + vy/ny)
    p_value = tails * st.t.sf(abs(t_obs), df)
    return TtestResults(t_obs, p_value)

if __name__ == '__main__':
    street_df = pd.read_csv(os.path.join(streetscore_data_folder, streetscore_nyc_file)) # latitude, longitude, q-score
    review_df = pd.read_csv(review_nyc_file)

    street_np = street_df.to_numpy()
    review_np = review_df.to_numpy()

    street_nodes = [item[:2] for item in street_np]
    review_nodes = [item[:2] for item in review_np]

    right_count = 0
    wrong_count = 0
    for _ in range(10):
        two_target_nodes = random.choices(street_nodes, k=2)
        review_scores = []
        street_scores = []

        for target_node in two_target_nodes:
            fsb = FixedSizeBuffer(node=target_node, buffer_size=200)
            sys.exit()

            fsb.find_points_in_geometry(street_nodes)
            index_of_nodes_in_buffer = fsb.find_nodes_in_buffer(street_nodes, return_index=True)
            street_scores.append(street_np[index_of_nodes_in_buffer][:, 2])

            index_of_nodes_in_buffer = fsb.find_nodes_in_buffer(review_nodes, return_index=True)
            review_scores.append(review_np[index_of_nodes_in_buffer][:, 2])

        street_result = t_welch(street_scores[0], street_scores[1])
        if street_result[1] < 0.01:
            print('street small:', np.mean(street_scores[0]), np.mean(street_scores[1]))
            print('review small:', np.mean(review_scores[0]), np.mean(review_scores[1]))
