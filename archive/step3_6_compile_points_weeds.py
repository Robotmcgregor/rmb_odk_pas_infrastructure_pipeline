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

import geopandas as gpd


def glob_dir_fn(export_dir):
    """  Search a specified directory and concatenate all records to a DataFrame.

        :param export_dir: list object containing the file faths within the directory.
        :return df_concat: pandas data frame object - all zonal stats csv files concatenated together."""

    # create an empty list
    list_df = []

    for file in glob.glob(export_dir + "\\*.csv"):
        # read in all zonal stats csv

        df = pd.read_csv(file)

        # append all zonal stats DataFrames to a list.
        list_df.append(df)

    # if list of dataframes is greater than 1 concatenate
    if len(list_df) <= 1:

        df_concat = list_df[0]
    else:
        df_concat = pd.concat(list_df)
        df_concat.dropna(axis=0, inplace=True)
    # df_concat = None
    return df_concat


def select_features_fn(df):
    """

    :param df:
    :return:
    """
    df['weed_comm'] = "9999"
    df['rec_method'] = 'Single GPS'
    df['source'] = "Rangelands Monitoring Branch"
    df["treatment"] = 'No'
    df['seedlings'] = 'Not recorded'
    df['juveniles'] = 'Not recorded'
    df['adults'] = 'Not recorded'
    df['seed_press'] = 'Not recorded'
    df['past_treat'] = 'Not recorded'
    df['herbicide'] = 'Not recorded'

    df_subset = df[['feature', 'property', 'prop_code', 'weed_bot', 'weed_comm', 'date_rec', 'rec_method', 'source',
                    'weed_size', 'weed_den', 'treatment', 'seedlings', 'juveniles', 'adults', 'seed_press',
                    'past_treat', 'herbicide', 'year', 'photo1', 'photo2', 'photo3', 'comment', 'datum', 'lat1', 'lon1',
                    'acc1',
                    'dist1', 'bear1']]

    return df_subset


def epsg_fn(datum):
    """ define the epsg number based on the rows datum

    :param datum: string object containing the rows datum
    :return epsg: integer object containing th epsg code based on the datum variable.
    """
    if datum == "WGS84":
        epsg = int(4326)
    elif datum == "GDA94":
        epsg = int(4283)
    else:
        epsg = int(0000)
    return epsg


def projection_file_name_fn(epsg):
    """ Creates two crs_name and crs_output depending on a geo-DataFrames CRS.

    :param epsg:
    :return:
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = "GDA94z52"
        crs_output = {"init": "EPSG:28352"}
    elif epsg_int == 28353:
        crs_name = "GDA94z53"
        crs_output = {"init": "EPSG:28353"}
    elif epsg_int == 4283:
        crs_name = "GDA94"
        crs_output = {"init": "EPSG:4283"}
    elif epsg_int == 32752:
        crs_name = "WGS84z52"
        crs_output = {"init": "EPSG:32752"}
    elif epsg_int == 32753:
        crs_name = "WGS84z53"
        crs_output = {"init": "EPSG:32753"}
    elif epsg_int == 3577:
        crs_name = "Albers"
        crs_output = {"init": "EPSG:3577"}
    elif epsg_int == 4326:
        crs_name = "GCS_WGS84"
        crs_output = {"init": "EPSG:4326"}
    else:
        crs_name = "not_defined"
        new_dict = {"init": "EPSG:" + str(epsg_int)}
        crs_output = new_dict

    return crs_name, crs_output


def re_project_geo_df_fn(epsg, geo_df):
    """ Creates two crs_name and crs_output depending on a geo-DataFrames CRS.

    :param epsg:
    :param geo_df:
    :return:
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = "GDA94z52"
        crs_output = {"init": "EPSG:28352"}
    elif epsg_int == 28353:
        crs_name = "GDA94z53"
        crs_output = {"init": "EPSG:28353"}
    elif epsg_int == 4283:
        crs_name = "GDA94"
        crs_output = {"init": "EPSG:4283"}
    elif epsg_int == 32752:
        crs_name = "WGS84z52"
        crs_output = {"init": "EPSG:32752"}
    elif epsg_int == 32753:
        crs_name = "WGS84z53"
        crs_output = {"init": "EPSG:32753"}
    elif epsg_int == 3577:
        crs_name = "Albers"
        crs_output = {"init": "EPSG:3577"}
    elif epsg_int == 4326:
        crs_name = "GCS_WGS84"
        crs_output = {"init": "EPSG:4326"}
    else:
        crs_name = "not_defined"
        new_dict = {"init": "EPSG:" + str(epsg_int)}
        crs_output = new_dict

    # Project DF to epsg value
    projected_df = geo_df.to_crs(epsg)

    return projected_df, crs_name


def export_shapefile_fn(df, epsg, directory, i, n, crs_name):
    """

    :param df:
    :param epsg:
    :param directory:
    :param i:
    :param n:
    :param crs_name:
    :return:
    """
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon" + n], df["lat" + n]), crs=epsg)
    # shp_output = (directory + "\\" + str(i) + "_points" + n + "_" + crs_name + ".shp")
    # gdf.to_file(shp_output, driver="ESRI Shapefile")
    # print("-", shp_output)
    return gdf


def export_shapefile_shift_fn(df, epsg, directory, lon, lat, crs_name, i):
    """

    :param df:
    :param epsg:
    :param directory:
    :param lon:
    :param lat:
    :param crs_name:
    :param i:
    :return:
    """
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    # shp_output = (directory + "\\" + "points_dest" + str(i) + "_" + crs_name + ".shp")
    # gdf.to_file(shp_output, driver="ESRI Shapefile")
    # print("-", shp_output)
    return gdf


def export_shapefile_shift2_fn(df, epsg, directory, lon, lat, crs_name, orig):
    """

    :param df:
    :param epsg:
    :param directory:
    :param lon:
    :param lat:
    :param crs_name:
    :param orig:
    :return:
    """
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = (directory + "\\" + "points_" + orig + "_" + crs_name + ".shp")
    gdf.to_file(shp_output, driver="ESRI Shapefile")
    #print("-", shp_output)
    return gdf


def easting_northing_fn(easting, northing, bearing, distance_m):
    """

    :param easting:
    :param northing:
    :param bearing:
    :param distance_m:
    :return:
    """
    l_sin = distance_m * math.sin(math.radians(bearing))
    l_cos = distance_m * math.cos(math.radians(bearing))

    dest_easting = float(easting) + l_sin
    dest_northing = float(northing) + l_cos

    return l_sin, l_cos, dest_easting, dest_northing


def extract_variables_for_destination_fn(gdf, lat, lon, dist, bear):
    """

    :param gdf:
    :param lat:
    :param lon:
    :param dist:
    :param bear:
    :return:
    """
    distance_m = gdf[dist].iloc[0]
    if distance_m == np.nan:
        distance_m.replace(np.nan, "0")
    bearing = gdf[bear].iloc[0]
    if bearing == np.nan:
        bearing.replcae(np.nan, "0")
    lat_orig = gdf[lat].iloc[0]
    lon_orig = gdf[lon].iloc[0]

    return distance_m, bearing, lat_orig, lon_orig


def project_and_extract_easing_northing_fn(epsg_int, gdf):
    # project dataframe to to wgsz52 (32752)
    projected_gdf, crs_name = re_project_geo_df_fn(32752, gdf)

    # extract eastings and northing values for geometry feature
    projected_gdf["orig_easting"] = projected_gdf["geometry"].x
    projected_gdf["orig_northing"] = projected_gdf["geometry"].y

    # extract original easting and northing values following geo-dataframe projection to wgsz52
    orig_easting = projected_gdf["orig_easting"].iloc[0]
    orig_northing = projected_gdf["orig_northing"].iloc[0]

    return projected_gdf, orig_easting, orig_northing


def add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int):
    # add new values to geo-dataframe and add epsg value for reference
    projected_gdf["l_sin"] = l_sin
    projected_gdf["l_cos"] = l_cos
    projected_gdf["dest_easting"] = dest_easting
    projected_gdf["dest_northing"] = dest_northing
    projected_gdf["epsg"] = epsg_int

    return projected_gdf


def main_routine(temp_dir, feature, export_dir_path):
    print("step3_6_compile_points_weeds.py INITIATED")

    # create variable directory containing the path to previously exported csv files.
    directory = temp_dir + "\\" + "weeds"
    export_dir = export_dir_path + "\\" + "weeds"
    # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.
    for file in glob.glob(directory + "\\*.csv"):
        if file:
            df = pd.read_csv(file)

            # call the select_features_fn function to subset the dataframe.
            df_subset = select_features_fn(df)

            # add a unique identifier column (uid) to loop through.
            df_subset["uid"] = df.index + 1

            dest_gdf_wgsz52_list = []
            dest_gdf_wgsz53_list = []

            # loop though all unique observations from the data frame (uid)
            for i in df_subset["uid"].unique():

                # filter the dataframe based on the unique value index_date_time variable (i.e. 1 observation).
                df = df_subset[df_subset["uid"] == i]

                # extract the datum from the observation and ensure it is all in upper case.
                datum = df.datum.iloc[0].upper()


                # call the epsg_fn function to extract the epsg value as an integer storing it in the variable epsg
                epsg = epsg_fn(datum)

                # call the projection_file_name to extract two variables crs_name string object for saving a file and the
                # crs_output epsg dictionary.
                crs_name, crs_output = projection_file_name_fn(epsg)

                # Create a point shapefile using the (n) lat and lon values.
                gdf = export_shapefile_fn(df, epsg, directory + "\\temp_shape", i, str(1), crs_name)

                # extract for variables for shifting the point to a new location, based on

                distance_m, bearing, lat_orig, lon_orig = extract_variables_for_destination_fn(
                    gdf, "lat1", "lon1", "dist1", "bear1")

                epsg_list = [32752, 32753]
                for epsg_int in epsg_list:

                    # call the project_and_extract_easting_northing_fn function to extract and store the original eastings/nothings
                    projected_gdf, orig_easting, orig_northing = project_and_extract_easing_northing_fn(epsg_int, gdf)

                    # call the easting_northing_fn function to extract destination eastings and northings
                    l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(
                        orig_easting, orig_northing, bearing, distance_m)

                    projected_gdf = add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing,
                                                              epsg_int)

                    # subset and reorder geo-dataframe
                    projected_gdf_subset = projected_gdf[[
                        "uid", 'feature', 'property', 'prop_code', 'weed_bot', 'weed_comm', 'date_rec', 'rec_method', 'source',
                        'weed_size', 'weed_den', 'treatment', 'seedlings', 'juveniles', 'adults', 'seed_press',
                        'past_treat', 'herbicide', 'year', 'photo1', 'photo2', 'photo3', 'comment', 'datum', "epsg",
                        "orig_easting", "orig_northing", "l_sin", "l_cos", "dest_easting", "dest_northing"]]

                    # convert to pandas dataframe (i.e. remove geometry)
                    projected_subset = pd.DataFrame(projected_gdf_subset)

                    # call the projection_file_name_fn function to extract the crs_name
                    crs_name, crs_output = projection_file_name_fn(epsg_int)

                    dest_gdf = export_shapefile_shift_fn(projected_subset, epsg_int, directory + "\\shapefile\\",
                                                         "dest_easting", "dest_northing", crs_name, i)

                    if epsg_int == 32752:
                        dest_gdf_wgsz52_list.append(dest_gdf)
                    if epsg_int == 32752:
                        dest_gdf_wgsz53_list.append(dest_gdf)

            if len(dest_gdf_wgsz52_list) >= 1:
                df = pd.concat(dest_gdf_wgsz52_list)

                df_orig52 = df.copy()
                df_dest52 = df.copy()

                df_orig52.drop(['uid', 'geometry', 'l_sin', 'l_cos', 'dest_easting', 'dest_northing', 'datum', 'epsg', 'photo1',
                                'photo2', 'photo3', 'feature', 'property', 'prop_code'], axis=1, inplace=True)

                df_orig52.columns = ["WEED_NAME", "GENUS_SPP", "DATE_REC", "REC_METHOD", "ORG_NAME", "SIZE_DIA_M", "DENS_CAT",
                                     "TREATMENT", "SEEDLINGS", "JUVENILES", "ADULTS", "SEED_PRES", "PAST_TREAT", "HERBICIDE",
                                     "YEAR", "NOTES", 'orig_easting', 'orig_northing']
                export_shapefile_shift2_fn(df_orig52, 32752, directory + "\\shapefile\\",  "orig_easting", "orig_northing", "WGS84z52",
                                           "origin")

                df_orig52.to_crs("epsg:4283")

                df_orig52.insert(4, 'LONG_G94', df_orig52["geometry"].y)
                df_orig52.insert(4, 'LAT_G94', df_orig52["geometry"].y)
                df_orig52.insert(0, 'SITE_MON', np.nan)
                df_orig52.insert(0, 'SITE_ID', np.nan)
                df_orig52.insert(0, 'ID', np.nan)

                df_orig52.to_file(export_dir + "\\shapefile\\WMB_output_orig_52_to_GDA94.shp", driver="ESRI Shapefile")

                # --------------------------------------------------------------------------------------------------------------

                df_dest52.drop(['uid', 'l_sin', 'l_cos', 'orig_easting', 'orig_northing', 'datum', 'epsg', 'geometry', 'photo1',
                                'photo2', 'photo3', 'feature', 'property', 'prop_code'], axis=1, inplace=True)

                df_dest52.columns = ["WEED_NAME", "GENUS_SPP", "DATE_REC", "REC_METHOD", "ORG_NAME", "SIZE_DIA_M", "DENS_CAT",
                                     "TREATMENT", "SEEDLINGS", "JUVENILES", "ADULTS", "SEED_PRES", "PAST_TREAT", "HERBICIDE",
                                     "YEAR", "NOTES", "dest_easting", "dest_northing"]
                export_shapefile_shift2_fn(df_dest52, 32752, directory + "\\shapefile\\", "dest_easting", "dest_northing",
                                           "WGS84z52",
                                           "origin")
                df_dest52.to_crs("epsg:4283")
                df_dest52.to_file(export_dir + "\\shapefile\\WMB_dest_weeds_points_52_to_GDA94.shp", driver="ESRI Shapefile")

                df_dest52.insert(4, 'LONG_G94', df_dest52["geometry"].y)
                df_dest52.insert(4, 'LAT_G94', df_dest52["geometry"].y)
                df_dest52.insert(0, 'SITE_MON', np.nan)
                df_dest52.insert(0, 'SITE_ID', np.nan)
                df_dest52.insert(0, 'ID', np.nan)

                df_dest52.to_file(directory + "WMB_output_dest_52_to_GDA94.shp", driver="ESRI Shapefile")

            if len(dest_gdf_wgsz53_list) >= 1:
                df = pd.concat(dest_gdf_wgsz53_list)
                export_shapefile_shift2_fn(df, 32753, directory + "\\shapefile\\", "orig_easting", "orig_northing", "WGS84z53",
                                           "origin")
                export_shapefile_shift2_fn(df, 32753, directory + "\\shapefile\\", "dest_easting", "dest_northing", "WGS84z53",
                                           "destination")

                df_orig53 = df.copy()
                df_dest53 = df.copy()
                df_orig53.drop(['uid', 'geometry', 'l_sin', 'l_cos', 'dest_easting', 'dest_northing', 'datum', 'epsg', 'photo1',
                                'photo2', 'photo3', 'feature', 'property', 'prop_code'], axis=1, inplace=True)

                df_orig53.columns = ["WEED_NAME", "GENUS_SPP", "DATE_REC", "REC_METHOD", "ORG_NAME", "SIZE_DIA_M", "DENS_CAT",
                                     "TREATMENT", "SEEDLINGS", "JUVENILES", "ADULTS", "SEED_PRES", "PAST_TREAT", "HERBICIDE",
                                     "YEAR", "NOTES", 'orig_easting', 'orig_northing']
                export_shapefile_shift2_fn(df_orig53, 32753, directory + "\\shapefile\\", "orig_easting", "orig_northing",
                                           "WGS84z53",
                                           "origin")
                df_orig53.to_crs("epsg:4283")

                df_orig53.insert(4, 'LONG_G94', df_orig53["geometry"].y)
                df_orig53.insert(4, 'LAT_G94', df_orig53["geometry"].y)
                df_orig53.insert(0, 'SITE_MON', np.nan)
                df_orig53.insert(0, 'SITE_ID', np.nan)
                df_orig53.insert(0, 'ID', np.nan)

                df_orig53.to_file(export_dir + "\\shapefile\\WMB_output_orig_53_to_GDA94.shp", driver="ESRI Shapefile")
                # df_orig.to_file(directory + "final_orig_weeds_points_53_to_GDA94.shp", driver="ESRI Shapefile")
                # --------------------------------------------------------------------------------------------------------------

                df_dest53.drop(['uid', 'l_sin', 'l_cos', 'orig_easting', 'orig_northing', 'datum', 'epsg', 'geometry', 'photo1',
                                'photo2', 'photo3', 'feature', 'property', 'prop_code'], axis=1, inplace=True)

                df_dest53.columns = ["WEED_NAME", "GENUS_SPP", "DATE_REC", "REC_METHOD", "ORG_NAME", "SIZE_DIA_M", "DENS_CAT",
                                     "TREATMENT", "SEEDLINGS", "JUVENILES", "ADULTS", "SEED_PRES", "PAST_TREAT", "HERBICIDE",
                                     "YEAR", "NOTES", "dest_easting", "dest_northing"]
                export_shapefile_shift2_fn(df_dest53, 32753, directory + "\\shapefile\\", "dest_easting", "dest_northing",
                                           "WGS84z53",
                                           "origin")
                df_dest53.to_crs("epsg:4283")

                df_dest53.insert(4, 'LONG_G94', df_dest53["geometry"].y)
                df_dest53.insert(4, 'LAT_G94', df_dest53["geometry"].y)
                df_dest53.insert(0, 'SITE_MON', np.nan)
                df_dest53.insert(0, 'SITE_ID', np.nan)
                df_dest53.insert(0, 'ID', np.nan)

                df_dest53.to_file(export_dir + "\\shapefile\\WMB_output_dest_53_to_GDA94.shp", driver="ESRI Shapefile")

        else:
            print('there are no weeds files.')
            pass


if __name__ == "__main__":
    main_routine()
