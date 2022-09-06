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

import geopandas as gpd


def glob_dir_fn(export_dir):
    """  Search a specified directory and concatenate all records to a DataFrame.

        :param export_dir: list object containing the file faths within the directory.
        :return df_concat: pandas data frame object - all zonal stats csv files concatenated together."""

    # create an empty list
    list_df = []

    for file in glob.glob(export_dir + '\\*'):
        # read in all zonal stats csv
        print(file)
        df = pd.read_csv(file, index_col=0)
        # append all zonal stats DataFrames to a list.
        list_df.append(df)

    # if list of dataframes is greater than 1 concatenate
    if len(list_df) <= 1:
        df_concat = list_df[0]
    else:
        df_concat = pd.concat(list_df)
        df_concat.dropna(axis=0, inplace=True)

    print('glob_dir_fn function complete')
    return df_concat


def select_features_fn(df):
    df_subset = df[
        ['feature', 'line_type', 'feat_label', 'index_date_time', 'datum',
         'lat1', 'lon1', 'acc1', 'dist1', 'bear1', 'lat2', 'lon2', 'acc2', 'dist2', 'bear2', 'lat3', 'lon3',
         'acc3', 'dist3', 'bear3', 'lat4', 'lon4', 'acc4', 'dist4', 'bear4', 'lat5', 'lon5', 'acc5', 'dist5',
         'bear5', 'lat6', 'lon6', 'acc6', 'dist6', 'bear6', 'lat7', 'lon7', 'acc7', 'dist7', 'bear7', 'lat8',
         'lon8', 'acc8', 'dist8', 'bear8', 'lat9', 'lon9', 'acc9', 'dist9', 'bear9', 'lat10', 'lon10', 'acc10',
         'dist10', 'bear10']]

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
    @return:  """

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
    shp_output = (directory + '\\' + str(i) + '_line_' + n + '_' + crs_name + '.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output)
    return gdf


def move_lat_long_decimal_degrees(bearing, distance):
    import math

    R = 6378.1  # Radius of the Earth
    brng = math.degrees(bearing)# convert bearing(degrees) to radians.
    d = distance/1000  # Distance in km

    # lat2  52.20444 - the lat result I'm hoping for
    # lon2  0.36056 - the long result I'm hoping for.

    lat1 = math.radians(12.4640168)  # Current lat point converted to radians
    lon1 = math.radians(130.8406327)  # Current long point converted to radians

    lat2 = math.asin(math.sin(lat1) * math.cos(d / R) +
                     math.cos(lat1) * math.sin(d / R) * math.cos(brng))

    lon2 = lon1 + math.atan2(math.sin(brng) * math.sin(d / R) * math.cos(lat1),
                             math.cos(d / R) - math.sin(lat1) * math.sin(lat2))

    lat2 = (math.degrees(lat2)*-1)
    lon2 = math.degrees(lon2)

    print(lat2)
    print(lon2)


def main_routine(temp_dir):

    print('step3_1_compile_line_infrastructure.py initiated')

    directory = temp_dir + '\\' + 'infra_lines'
    df = glob_dir_fn(directory)

    # call the select_features_fn function to subset the dataframe.
    df_subset = select_features_fn(df)

    # loop though all unique observations from the data frame
    for i in df_subset.index_date_time.unique():
        # filter the dataframe based on the unique value index_date_time variable.
        df = df_subset[df_subset['index_date_time'] == i]
        # collect the datum
        datum = df.datum.iloc[0].upper()
        print(datum)
        epsg = epsg_fn(datum)

        epsg, crs_name, crs_output = projection_file_name_fn(epsg)
        # extract the crs epsg string from the crs_output dictionary
        epsg = (crs_output['init'])

        # Create a point shapefile using the (n) lat and lon values.
        for n in range(10):
            value_ = str(n + 1)
            lat = df['lat' + value_][0]
            print(lat)
            if lat != 0:
                print(lat)
                #gdf = export_shapefile_fn(df, epsg, temp_path, i, value_, crs_name)        # set epsg to WGSz52.
                # set epsg to WGSz52.
                #epsg = 32752
                #gdf_wgs_52, crs_name = re_project_geo_df_fn(epsg, gdf)
                #print(gdf_wgs_52.crs)
                # call the export_shapefile_fn function to export projected shapefile
                #export_shapefile_fn(gdf_wgs_52, epsg, temp_path, i, str(1), crs_name)

                # set epsg to WGSz52
                #epsg = 32753
                #gdf_wgs_53, crs_name = re_project_geo_df_fn(epsg, gdf)
                #print(gdf_wgs_53.crs)
                #export_shapefile_fn(gdf_wgs_53, epsg, temp_path, i, str(1), crs_name)

        move_lat_long_decimal_degrees(107.31, 10.0)


if __name__ == '__main__':
    main_routine()
