#!/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Import modules
import pandas as pd
import glob
import os
import math
from math import degrees, pi
import numpy as np
import geopy

import geopandas as gpd


def glob_dir_fn(export_dir):
    """  Search a specified directory and concatenate all records to a DataFrame.

        :param export_dir: list object containing the file faths within the directory.
        :return df_concat: pandas data frame object - all zonal stats csv files concatenated together."""

    print('glob_dir_fn started')
    # create an empty list
    list_df = []

    for file in glob.glob(export_dir + '\\*.csv'):
        # read in all zonal stats csv
        print('file: ', file)
        df = pd.read_csv(file, index_col=0)
        print('df: ', df)
        # append all zonal stats DataFrames to a list.
        list_df.append(df)
    print('list_df: ', len(list_df))
    # if list of dataframes is greater than 1 concatenate
    if len(list_df) <= 1:
        df_concat = list_df[0]
    else:
        df_concat = pd.concat(list_df)
        df_concat.dropna(axis=0, inplace=True)
    print('df_concat: ', df_concat)

    return df_concat


def select_features_fn(df):
    df_subset = df[
        ['feature', 'feat_label', 'index_date_time', 'datum',
         'lat1', 'lon1', 'acc1', 'dist1', 'bear1']]

    return df_subset


def epsg_fn(datum):
    """ define the epsg number based on the rows datum

    :param datum: string object containing the rows datum
    :return epsg: integer object containing th epsg code based on the datum variable.
    """
    if datum == 'WGS84':
        epsg = int(4326)
    elif datum == 'GDA94':
        epsg = int(4283)
    else:
        epsg = int(0000)
    return epsg


def projection_file_name_fn(epsg):
    """ Creates two crs_name and crs_output depending on a geo-DataFrames CRS.
    @param epsg:
    @param clean_odk_geo_df:
    @return:
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = 'GDA94z52'
        crs_output = {'init': 'EPSG:28352'}
    elif epsg_int == 28353:
        crs_name = 'GDA94z53'
        crs_output = {'init': 'EPSG:28353'}
    elif epsg_int == 4283:
        crs_name = 'GDA94'
        crs_output = {'init': 'EPSG:4283'}
    elif epsg_int == 32752:
        crs_name = 'WGS84z52'
        crs_output = {'init': 'EPSG:32752'}
    elif epsg_int == 32753:
        crs_name = 'WGS84z53'
        crs_output = {'init': 'EPSG:32753'}
    elif epsg_int == 3577:
        crs_name = 'Albers'
        crs_output = {'init': 'EPSG:3577'}
    elif epsg_int == 4326:
        crs_name = 'GCS_WGS84'
        crs_output = {'init': 'EPSG:4326'}
    else:
        crs_name = 'not_defined'
        new_dict = {'init': 'EPSG:' + str(epsg_int)}
        crs_output = new_dict

    return crs_name, crs_output


def re_project_geo_df_fn(epsg, geo_df):
    """ Creates two crs_name and crs_output depending on a geo-DataFrames CRS.
    @param epsg:
    @param clean_odk_geo_df:
    @return:
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = 'GDA94z52'
        crs_output = {'init': 'EPSG:28352'}
    elif epsg_int == 28353:
        crs_name = 'GDA94z53'
        crs_output = {'init': 'EPSG:28353'}
    elif epsg_int == 4283:
        crs_name = 'GDA94'
        crs_output = {'init': 'EPSG:4283'}
    elif epsg_int == 32752:
        crs_name = 'WGS84z52'
        crs_output = {'init': 'EPSG:32752'}
    elif epsg_int == 32753:
        crs_name = 'WGS84z53'
        crs_output = {'init': 'EPSG:32753'}
    elif epsg_int == 3577:
        crs_name = 'Albers'
        crs_output = {'init': 'EPSG:3577'}
    elif epsg_int == 4326:
        crs_name = 'GCS_WGS84'
        crs_output = {'init': 'EPSG:4326'}
    else:
        crs_name = 'not_defined'
        new_dict = {'init': 'EPSG:' + str(epsg_int)}
        crs_output = new_dict

    # Project DF to epsg value
    projected_df = geo_df.to_crs(epsg)

    return projected_df, crs_name


def export_shapefile_fn(df, epsg, directory, i, n, crs_name):
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df['lon' + n], df['lat' + n]), crs=epsg)
    shp_output = (directory + '\\' + str(i) + '_points' + n + '_' + crs_name + '.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output)
    return gdf


def export_shapefile_shift_fn(df, epsg, directory, lon, lat, crs_name):
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = (directory + '\\' + 'points_dest_' + crs_name + '.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output)
    return gdf


"""def move_lat_long_decimal_degrees(lat, lon, bearing, distance_m):
    import math

    R = 6378.1  # Radius of the Earth
    bearing_km = math.degrees(bearing)  # convert bearing(degrees) to radians.
    d = (distance_m/1000)  # Distance in km

    # lat2  52.20444 - the lat result I'm hoping for
    # lon2  0.36056 - the long result I'm hoping for.

    lat1 = math.radians(lat)  # Current lat point converted to radians
    lon1 = math.radians(lon)  # Current long point converted to radians

    lat2 = math.asin(math.sin(lat1) * math.cos(d / R) +
                     math.cos(lat1) * math.sin(d / R) * math.cos(bearing_km))

    lon2 = lon1 + math.atan2(math.sin(bearing_km) * math.sin(d / R) * math.cos(lat1),
                             math.cos(d / R) - math.sin(lat1) * math.sin(lat2))

    lat2 = (math.degrees(lat2) * -1)
    lon2 = math.degrees(lon2)

    print(lat2)
    print(lon2)
    return lat2, lon2"""


"""def geopy_shift_fn(lat_orig1, lon_orig1, bearing, distance_m):
    import geopy
    from geopy import distance
    # given: lat1, lon1, b = bearing in degrees, d = distance in kilometers
    distance_km = distance_m/1000

    origin = geopy.Point(lat_orig1, lon_orig1)
    destination = geopy.distance.geodesic(kilometers=distance_km).destination(origin, bearing)

    lat_shift, lon_shift = destination.latitude, destination.longitude

    return lat_shift, lon_shift"""


"""def easting_northing_fn(distance_m, bearing, orig_lat, orig_lon):

    print('orig: ', orig_lat)
    print('orig: ', orig_lon)

    dest_lon_bear = distance_m * math.sin(math.radians(bearing))
    print(dest_lon_bear)
    dest_lat_bear = distance_m * math.cos(math.radians(bearing))
    print(dest_lat_bear)

    dest_lon = float(orig_lon) + dest_lon_bear
    dest_lat = float(orig_lat) + dest_lat_bear

    print(dest_lon_bear, dest_lat_bear, dest_lon, dest_lat)
    return dest_lon_bear, dest_lat_bear, dest_lon, dest_lat"""


def easting_northing_fn(easting, northing, bearing, distance_m):
    print('orig: ', easting)
    print('orig: ', northing)

    l_sin = distance_m * math.sin(math.radians(bearing))
    print(l_sin)
    l_cos = distance_m * math.cos(math.radians(bearing))
    print(l_cos)

    dest_easting = float(easting) + l_sin
    dest_northing = float(northing) + l_cos

    print(l_sin, l_cos, dest_easting, dest_northing)
    return l_sin, l_cos, dest_easting, dest_northing


def extract_variables_for_destination_fn(gdf, lat, lon, dist, bear):

    distance_m = gdf[dist].iloc[0]
    if distance_m == np.nan:
        distance_m.replace(np.nan, '0')
    bearing = gdf[bear].iloc[0]
    if bearing == np.nan:
        bearing.replcae(np.nan, '0')
    lat_orig = gdf[lat].iloc[0]
    lon_orig = gdf[lon].iloc[0]

    return distance_m, bearing, lat_orig, lon_orig


def project_and_extract_easing_northing_fn(epsg_int, gdf):
    # project dataframe to to wgsz52 (32752)
    projected_gdf, crs_name = re_project_geo_df_fn(32752, gdf)
    print(projected_gdf.crs)

    # extract eastings and northing values for geometry feature
    projected_gdf['orig_easting'] = projected_gdf['geometry'].x
    projected_gdf['orig_northing'] = projected_gdf['geometry'].y

    # extract original easting and northing values following geo-dataframe projection to wgsz52
    orig_easting = projected_gdf['orig_easting'].iloc[0]
    orig_northing = projected_gdf['orig_northing'].iloc[0]

    return projected_gdf, orig_easting, orig_northing


def add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int):
    # add new values to geo-dataframe and add epsg value for reference
    projected_gdf['l_sin'] = l_sin
    projected_gdf['l_cos'] = l_cos
    projected_gdf['dest_easting'] = dest_easting
    projected_gdf['dest_northing'] = dest_northing
    projected_gdf['epsg'] = epsg_int

    return projected_gdf


def main_routine(temp_dir):

    print('step3_2_compile_points_infrastructure.py initiated')

    # create variable directory containing the path to previously exported csv files.
    directory = temp_dir + '\\' + 'infra_points'
    # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.
    df = glob_dir_fn(directory)

    # call the select_features_fn function to subset the dataframe.
    df_subset = select_features_fn(df)

    # add a uniqu identifier column (uid) to loop through.
    df_subset["uid"] = df.index + 1

    lat_orig_list = []
    lon_orig_list = []
    dest_lon_bear_list = []
    dest_lat_bear_list = []
    lat_dest_list = []
    lon_dest_list = []

    # loop though all unique observations from the data frame (uid)
    for i in df_subset['uid'].unique():

        # filter the dataframe based on the unique value index_date_time variable (i.e. 1 observation).
        df = df_subset[df_subset['uid'] == i]

        # extract the dataum from the observation and ensure it is all in upper case.
        datum = df.datum.iloc[0].upper()

        # call the epsg_fn function to extract the epsg value as an integer storing it in the variable epsg
        epsg = epsg_fn(datum)

        # call the projection_file_name to extract two variables crs_name string obgect for saving a file and the
        # crs_output epsg dictionary.
        crs_name, crs_output = projection_file_name_fn(epsg)


        # Create a point shapefile using the (n) lat and lon values.
        gdf = export_shapefile_fn(df, epsg, directory + '\\temp_shape', i, str(1), crs_name)
        print(gdf.crs)

        # extract for variables for shifting the point to a new location, based on

        distance_m, bearing, lat_orig, lon_orig = extract_variables_for_destination_fn(
            gdf, 'lat1', 'lon1', 'dist1', 'bear1')

        epsg_list = [32752, 32753]
        for epsg_int in epsg_list:

            # call the project_and_extract_easting_northing_fn function to extract and store the original eastings/nothings
            projected_gdf, orig_easting, orig_northing = project_and_extract_easing_northing_fn(epsg_int, gdf)

            # call the easting_northing_fn function to extract destination eastings and northings
            l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(
                orig_easting, orig_northing, bearing, distance_m)

            projected_gdf = add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int)

            # subset and reorder geo-dataframe
            projected_gdf_subset = projected_gdf[[
                'uid', 'feature', 'feat_label', 'index_date_time', 'datum', 'epsg',
                'orig_easting', 'orig_northing', 'l_sin', 'l_cos', 'dest_easting', 'dest_northing']]


            # calculate WGS84 destination
            from pyproj import Proj, transform
            inProj = Proj('epsg:' + str(epsg_int))
            outProj = Proj('epsg:4326')
            print(':::::::::::::::::::')
            print(inProj)
            dest_easting = projected_gdf_subset['dest_easting'].iloc[0]
            dest_northing = projected_gdf_subset['dest_northing'].iloc[0]
            dest_lat, dest_lon = transform(inProj, outProj, dest_easting, dest_northing)
            print(dest_lat, dest_lon)

            projected_gdf_subset['dest_lat_wgs84'] = dest_lat
            projected_gdf_subset['dest_lon_wgs84'] = dest_lon

            # calculate GDA94 destination
            inProj = Proj('epsg:' + str(epsg_int))
            outProj = Proj('epsg:4283')
            print(':::::::::::::::::::')
            print(inProj)
            dest_easting = projected_gdf_subset['dest_easting'].iloc[0]
            dest_northing = projected_gdf_subset['dest_northing'].iloc[0]
            dest_lat, dest_lon = transform(inProj, outProj, dest_easting, dest_northing)
            print(dest_lat, dest_lon)

            projected_gdf_subset['dest_lat_gda94'] = dest_lat
            projected_gdf_subset['dest_lon_gda94'] = dest_lon

            print('______________________________')
            print(list(projected_gdf_subset))

            gdf_epsg_int = gpd.GeoDataFrame(
                projected_gdf_subset, geometry=gpd.points_from_xy(projected_gdf_subset['dest_northing'],
                                                                  projected_gdf_subset['dest_easting']), crs=epsg_int)

            """gdf_epsg_int.to_file(directory + '\\' 'shapefile\\' + 'gdf_epsg_int' + gdf_epsg_int + '.shp',
                                 driver="ESRI Shapefile")"""
            gdf_4326 = gpd.GeoDataFrame(
                projected_gdf_subset, geometry=gpd.points_from_xy(projected_gdf_subset['dest_lon_wgs84'],
                                                                  projected_gdf_subset['dest_lat_wgs84']), crs=4326)

            gdf_4326.to_file(directory + '\\' 'shapefile\\' + 'gdf_epsg_int_gdf_4326.shp',
                                 driver="ESRI Shapefile")

            gdf_4283 = gpd.GeoDataFrame(
                projected_gdf_subset, geometry=gpd.points_from_xy(projected_gdf_subset['dest_lon_gda94'],
                                                                  projected_gdf_subset['dest_lat_gda94']), crs=4283)
            gdf_4283.to_file(directory + '\\' 'shapefile\\' + 'gdf_epsg_int_gdf_4283.shp',
                                 driver="ESRI Shapefile")


            # extract the crs_name for export filepath
            crs_name, crs_output = projection_file_name_fn(epsg_int)
            # export output as csv file
            projected_gdf_subset.to_csv(directory + '\\' 'shapefile\\' + 'projected_gdf_subset' + crs_name + '.csv')

            #todo all geodataframes fail to load points at reasonable positions even though the calculations are correct.


if __name__ == '__main__':
    main_routine()
