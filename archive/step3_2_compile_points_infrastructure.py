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

    return epsg, crs_name, crs_output


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


def move_lat_long_decimal_degrees(lat, lon, bearing, distance_m):
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
    return lat2, lon2


def geopy_shift_fn(lat_orig1, lon_orig1, bearing, distance_km):
    import geopy
    from geopy import distance
    # given: lat1, lon1, b = bearing in degrees, d = distance in kilometers

    origin = geopy.Point(lat_orig1, lon_orig1)
    destination = geopy.distance.geodesic(kilometers=distance_km).destination(origin, bearing)

    lat_shift, lon_shift = destination.latitude, destination.longitude

    return lat_shift, lon_shift


def easting_northing_fn(distance_m, bearing, orig_lat, orig_lon):

    print('orig: ', orig_lat)
    print('orig: ', orig_lon)

    dest_lon_bear = distance_m * math.sin(math.radians(bearing))
    print(dest_lon_bear)
    dest_lat_bear = distance_m * math.cos(math.radians(bearing))
    print(dest_lat_bear)

    dest_lon = float(orig_lon) + dest_lon_bear
    dest_lat = float(orig_lat) + dest_lat_bear

    print(dest_lon_bear, dest_lat_bear, dest_lon, dest_lat)
    return dest_lon_bear, dest_lat_bear, dest_lon, dest_lat


def easting_northing_fn(distance_m, bearing, easting, northing):
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


def main_routine(temp_dir):
    print('step3_2_compile_points_infrastructure.py initiated')

    # create subfolders within directory
    directory = temp_dir + '\\' + 'infra_points'
    print('directory: ', directory)
    df = glob_dir_fn(directory)

    # call the select_features_fn function to subset the dataframe.
    df_subset = select_features_fn(df)
    df_subset["uid"] = df.index + 1

    lat_orig_list = []
    lon_orig_list = []
    dest_lon_bear_list = []
    dest_lat_bear_list = []
    lat_dest_list = []
    lon_dest_list = []

    # loop though all unique observations from the data frame
    for i in df_subset['uid'].unique():
        print(i)
        # filter the dataframe based on the unique value index_date_time variable.
        df = df_subset[df_subset['uid'] == i]
        # collect the datum feature stored in the variable datum
        datum = df.datum.iloc[0].upper()
        print(datum)

        epsg = epsg_fn(datum)

        epsg, crs_name, crs_output = projection_file_name_fn(epsg)
        # extract the crs epsg string from the crs_output dictionary
        epsg = (crs_output['init'])

        # Create a point shapefile using the (n) lat and lon values.


        gdf = export_shapefile_fn(df, epsg, directory + '\\temp_shape', i, str(1), crs_name)
        # extract the latitude, longitude, bearing and distance(m) values
        gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\test_gda_z52_2.csv")


        # =======================================================================

        # convert to gda94z52
        epsg_int = 28352
        crs_name = 'GDA94z52'
        crs_output = {'init': 'EPSG:28352'}
        # set epsg to WGSz52.

        gdf_gda_52 = gdf.to_crs(epsg=epsg_int)

        #gdf_wgs_52, crs_name = re_project_geo_df_fn(epsg, gdf)
        print(gdf_gda_52.crs)
        gdf_gda_52.to_file(directory + '\\temp_shape\\infra_points_' + crs_name + '.shp', driver='ESRI Shapefile')
        print('::::::::::::::::::::::::::::::::::::::::::::::::::::::::')

        print(gdf_gda_52.geometry.iloc[0])
        data = gdf_gda_52.geometry.iloc[0]

        lat_lon = str(data)[7:-1]
        lat_z52, lon_z52 = lat_lon.rsplit(" ", 1)
        print('new lat: ', lat_z52)
        print('new lon: ', lon_z52)

        distance_m = gdf_gda_52.dist1.iloc[0]
        distance_km = (distance_m/1000)
        print(distance_km)
        bearing = gdf_gda_52.bear1.iloc[0]
        print(bearing)
         ## call the export_shapefile_fn function to export projected shapefile
        #export_shapefile_fn(gdf_wgs_52, epsg, directory + '\\temp_shape',  i, str(1), crs_name)


        #lat_dest, lon_dest = geopy_shift_fn(lat, lon, bearing, distance_m)
        # call the move_lat_long_decimal_degrees_fn function to extract shifted lat and lon values.
        #lat2, lon2 = move_lat_long_decimal_degrees(float(lat_z52), float(lon_z52), bearing, distance_m)



        dest_lon_bear, dest_lat_bear, dest_lon, dest_lat = easting_northing_fn(206.306, 305.9922, 2923151.9610, 1103222.9080)

        lat_orig_list.append(lon_z52)
        lat_orig_list.append(lat_z52)
        lat_dest_list.append(dest_lat)
        lon_dest_list.append(dest_lon)
        dest_lon_bear_list.append(dest_lon_bear)
        dest_lat_bear_list.append(dest_lat_bear)

        #export_shapefile_shift_fn(gdf_gda_52, epsg_int, directory, lon2, lat2, crs_name)

    # append shift lat and lon to new columns
    df_subset['lat_orig1'] = lat_z52
    df_subset['lon_orig1'] = lon_z52
    df_subset['lat_bear_r1'] = dest_lon_bear_list
    df_subset['lon_bear_r1'] = dest_lat_bear_list
    df_subset['lat_dest1'] = lat_dest_list
    df_subset['lon_dest1'] = lon_dest_list




    #easting_northing_fn(distance_m, bearing, lat_z52, lon_z53)

    print(df_subset)
    df_subset.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\test_gda_z52_3.csv")

    #print(datum, crs_name, crs_output)

    #export_shapefile_shift_fn(df_subset, epsg, directory + '\\shapefile', str(1), crs_name)

    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df_subset, geometry=gpd.points_from_xy(df_subset['lon_dest1'], df_subset['lat_dest1']), crs=28352)
    shp_output = (r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs" + '\\' + 'points_dest_gdaz52_2.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output)

    gdf['lon__'] = gdf['geometry'].x
    gdf['lat__'] = gdf['geometry'].y

    gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\test_gda_z52__new2.csv")

    distance_m = gdf.dist1.iloc[0]/1000
    bearing = gdf.bear1.iloc[0]
    easting = gdf.lat__.iloc[0]
    northing = gdf.lon__.iloc[0]
    print(distance_m, bearing, easting, northing)

    l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(distance_m, bearing, easting, northing)

    gdf['l_sin'] = l_sin
    gdf['l_cos'] = l_cos
    gdf['dest_east'] = dest_easting
    gdf['dest_north'] = dest_northing
    print('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF')
    print(list(gdf))
    print(l_sin, l_cos, dest_easting, dest_northing)

    gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\final_dest_point_gda_z52.csv")
    gdf_z52 = pd.read_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\final_dest_point_gda_z52.csv")

    new_gdf = gdf_z52[['feature', 'feat_label', 'lon__', 'lat__', 'l_sin', 'l_cos', 'dest_east', 'dest_north']]
    new_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\oh_my_god.csv")
    print(list(new_gdf))
    print(new_gdf)


    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    new_gdf_2 = gpd.GeoDataFrame(
        new_gdf, geometry=gpd.points_from_xy(new_gdf['dest_north'], new_gdf['dest_east']), crs=28352)
    shp_output = (r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs" + '\\' + 'points_final_dest_gdaz52___.shp')
    new_gdf_2.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output)

    """
    print(type(df_subset))
    print(list(df_subset))
    # todo up to here export as gd frame"""

    # set epsg to WGSz52.
    """epsg = 32752
    gdf_wgs_52, crs_name = re_project_geo_df_fn(epsg, gdf)
    print(gdf_wgs_52.crs)
    # call the export_shapefile_fn function to export projected shapefile
    export_shapefile_shift_fn(gdf_wgs_52, epsg, directory + '\\temp_shape',  str(1), crs_name)
"""
    # set epsg to WGSz52
    # epsg = 32753
    # gdf_wgs_53, crs_name = re_project_geo_df_fn(epsg, gdf)
    # print(gdf_wgs_53.crs)
    # export_shapefile_fn(gdf_wgs_53, epsg, temp_path, i, str(1), crs_name)

    # move_lat_long_decimal_degrees(107.31, 10.0)


if __name__ == '__main__':
    main_routine()
