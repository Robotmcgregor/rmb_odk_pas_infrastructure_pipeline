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
# import modules
import warnings

warnings.filterwarnings("ignore")


def select_infra_features_fn(df):
    """

    :param df:
    :return:
    """
    # df["length_m"] = 9999
    # df["area_km2"] = 9999
    df["source"] = "NTG Rangelands Monitoring Branch"
    df["display"] = "Yes"
    df["date_curr"] = 9999

    df_subset = df[[
        "feature_group", "feature", "label", "date_rec", "date_curr", "district", "property", "prop_code", "source",
        "display",
        "condition", "comment", "photo1", "photo2", "photo3", "datum", "lat1", "lon1", "acc1",
        "dist1", "bear1"]]  # "length_m", "area_km2",

    return df_subset


def select_weeds_features_fn(df):
    """

    :param df:
    :return:
    """
    # df["weed_comm"] = "9999"
    df["rec_method"] = "Single GPS"
    df["source"] = "NTG Rangelands Monitoring Branch"
    df["treatment"] = "No"
    df["seedlings"] = "Not recorded"
    df["juveniles"] = "Not recorded"
    df["adults"] = "Not recorded"
    df["seed_press"] = "Not recorded"
    df["past_treat"] = "Not recorded"
    df["herbicide"] = "Not recorded"

    df_subset = df[
        ["feature", "district", "property", "prop_code", "weed_bot", "weed_comm", "date_rec", "rec_method", "source",
         "recorder", "weed_size", "weed_den", "treatment", "seedlings", "juveniles", "adults", "seed_press",
         "past_treat", "herbicide", "year", "photo1", "photo2", "photo3", "comment", "datum", "lat1", "lon1",
         "acc1",
         "dist1", "bear1"]]

    return df_subset


def select_features_fn(df, subset_list):
    """

    :param df:
    :return:
    """

    df_subset = df[subset_list]

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
    shp_output = (directory + "\\" + str(i) + "_points" + n + "_" + crs_name + ".shp")
    gdf.to_file(shp_output, driver="ESRI Shapefile")
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
    shp_output = (directory + "\\" + "points_dest" + str(i) + "_" + crs_name + ".shp")
    gdf.to_file(shp_output, driver="ESRI Shapefile")
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
    """

    :param epsg_int:
    :param gdf:
    :return:
    """

    # project dataframe to to WGS UTM
    projected_gdf, crs_name = re_project_geo_df_fn(epsg_int, gdf)

    # extract eastings and northing values for geometry feature
    projected_gdf["orig_easting"] = projected_gdf["geometry"].x
    projected_gdf["orig_northing"] = projected_gdf["geometry"].y

    # extract original easting and northing values following geo-dataframe projection to wgsz52
    orig_easting = projected_gdf["orig_easting"].iloc[0]
    orig_northing = projected_gdf["orig_northing"].iloc[0]

    return projected_gdf, orig_easting, orig_northing


def add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int):
    """

    :param projected_gdf:
    :param l_sin:
    :param l_cos:
    :param dest_easting:
    :param dest_northing:
    :param epsg_int:
    :return:
    """
    # add new values to geo-dataframe and add epsg value for reference
    projected_gdf["l_sin"] = l_sin
    projected_gdf["l_cos"] = l_cos
    projected_gdf["dest_easting"] = dest_easting
    projected_gdf["dest_northing"] = dest_northing
    projected_gdf["epsg"] = epsg_int

    return projected_gdf


def main_routine(temp_dir, feature_name, export_dir_path, subset_list, projected_gdf_dest_list,
                 orig_drop_list, dest_drop_list, photo_subset_list, pastoral_estate, user_df):
    """

    :param feature_name:
    :param subset_list:
    :param projected_gdf_dest_list:
    :param orig_drop_list:
    :param dest_drop_list:
    :param photo_subset_list:
    :return:
    :param temp_dir:
    :param feature:
    :param export_dir_path:
    """

    infra_column_list = ["FEATGROUP", "FEATURE", "LABEL", "DATE_INSP", "DATE_CURR", "DISTRICT", "PROPERTY", "PROP_TAG",
                         "SOURCE", "MAPDISPLAY", "CONDITION", "NOTES", "geometry"]  # "LENGTH_M", "AREA_KM2",

    weed_column_dict = {"weed_bot": "WEED_NAME", "weed_comm": "GENUS_SPP", "date_rec": "DATE_REC",
                        "rec_method": "REC_METHOD", "source": "ORG_NAME", "weed_size": "SIZE_DIA_M",
                        "weed_den": "DENS_CAT", "treatment": "TREATMENT", "seedlings": "SEEDLINGS",
                        "juveniles": "JUVENILES", "adults": "ADULTS", "seed_press": "SEED_PRES",
                        "past_treat": "PAST_TREAT", "herbicide": "HERBICIDE", "year": "YEAR",
                        "comment": "NOTES"}

    # create variable directory containing the path to previously exported csv files.
    directory = temp_dir + "\\" + feature_name

    export_dir = export_dir_path + "\\" + feature_name

    # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.
    for file in glob.glob(directory + "\\*.csv"):

        if file:
            df = pd.read_csv(file)
            if feature_name == "infra_points" or feature_name == "infra_water_points":
                # call the select_features_fn function to subset the dataframe.
                df_subset = select_infra_features_fn(df)

            elif feature_name == "weeds":
                # call the select_features_fn function to subset the dataframe.
                df_subset = select_weeds_features_fn(df)


            else:

                # call the select_features_fn function to subset the dataframe.
                df_subset = select_features_fn(df, subset_list)

            # add a unique identifier column (uid) to loop through.
            df_subset["uid"] = df.index + 1
            # fill null values with 0 in the distance and bearing columns
            # df_subset.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\test" + feature_name + ".csv")
            df_subset["dist1"] = df_subset["dist1"].fillna(0)
            df_subset["bear1"] = df_subset["bear1"].fillna(0)
            # df_subset.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\test" + feature_name + ".csv")
            df_photo = df_subset[photo_subset_list]
            df_photo.to_csv(export_dir + "\\csv\\" + feature_name + "_photo.csv")

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

                    # call the project_and_extract_easting_northing_fn function to extract and store the original
                    # eastings/nothings
                    projected_gdf, orig_easting, orig_northing = project_and_extract_easing_northing_fn(epsg_int, gdf)
                    # call the easting_northing_fn function to extract destination eastings and northings
                    l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(
                        orig_easting, orig_northing, bearing, distance_m)
                    # projected_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\point_" + str(epsg_int) + "_" + str(i) + ".csv")
                    projected_gdf = add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing,
                                                              epsg_int)

                    # subset and reorder geo-dataframe
                    projected_gdf_subset = projected_gdf[projected_gdf_dest_list]

                    # convert to pandas dataframe (i.e. remove geometry)
                    projected_subset = pd.DataFrame(projected_gdf_subset)
                    # projected_subset.to_csv(
                    #    r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\point_proj_subset" + str(epsg_int) + "_" + str(
                    #        i) + ".csv")
                    # call the projection_file_name_fn function to extract the crs_name
                    crs_name, crs_output = projection_file_name_fn(epsg_int)

                    dest_gdf = export_shapefile_shift_fn(projected_subset, epsg_int, directory + "\\shapefile\\",
                                                         "dest_easting", "dest_northing", crs_name, i)

                    if epsg_int == 32752:
                        dest_gdf_wgsz52_list.append(dest_gdf)
                    if epsg_int == 32753:
                        dest_gdf_wgsz53_list.append(dest_gdf)

            if len(dest_gdf_wgsz52_list) >= 1:

                df = pd.concat(dest_gdf_wgsz52_list)
                # df.to_csv(export_dir + "\\csv\\df_orig52_concat.csv")
                # create two copies of the original df dataframe

                df_orig52 = df.copy()
                # df_orig52.to_csv(export_dir + "\\csv\\df_orig52_concat.csv")
                df_dest52 = df.copy()
                # df_dest52.to_csv(export_dir + "\\csv\\df_dest52_concat.csv")

                # ------------------------------------------ WGSz52 origin ---------------------------------------------

                """# drop working columns excluding the new output easting and northing
                df_orig52.drop(orig_drop_list, axis=1, inplace=True)

                # create a geo-dataframe using the original easting and northing in WGSz52
                export_shapefile_shift2_fn(df_orig52, 32752, directory + "\\shapefile\\", "orig_easting",
                                           "orig_northing",
                                           "WGS84z52",
                                           "origin")
                # convert to GDA94 geographic
                df_orig52_final, crs_name = re_project_geo_df_fn(4283, df_orig52)

                # drop the easting and northing values from the output.
                df_orig52_final.drop(["orig_easting", "orig_northing"], axis=1, inplace=True)
                # export shapefile to export directory

                # update feature headings if infrastructure
                if feature_name == "infra_points" or feature_name == "infra_water_points":
                    df_orig52_final.columns = infra_column_list
                    
                if feature_name == "weeds":
                    df_orig52_final.rename(columns=weed_column_dict, inplace=True)
                    print(list(df_orig52_final))
                    df_orig52_final.drop(columns=["feature", "property", "prop_code"], inplace=True)
                    df_orig52_final.insert(0, "SITE_ID", np.nan)
                    df_orig52_final.insert(1, "SITE_MON", np.nan)
                    print(list(df_orig52_final))

                # replace all 9999 values with np.nan
                df_orig52_final = df_orig52_final.replace(["9999", 9999], np.nan)

                df_orig52_final.to_file(export_dir + "\\shapefile\\" + feature_name + "_orig_52_to_GDA94.shp",
                                  driver="ESRI Shapefile")
                df_orig52_final.to_csv(export_dir + "\\csv\\" + feature_name + "_orig_52_to_GDA94.csv") #, index_col = False)"""

                # --------------------------------------- WGSz52 destination -------------------------------------------

                print(" - point feature: ", feature_name)
                # drop working columns excluding the new output easting and northing

                df_dest52.drop(dest_drop_list, axis=1, inplace=True)

                # create a geo-dataframe using the destination easting and northing in WGSz52
                export_shapefile_shift2_fn(df_dest52, 32752, directory + "\\shapefile\\", "dest_easting",
                                           "dest_northing",
                                           "WGS84z52",
                                           "dest")
                # convert to GDA94 geographic
                # df_dest52.to_crs(epsg=4283)
                df_dest52_final, crs_name = re_project_geo_df_fn(4283, df_dest52)
                # drop the easting and northing values from the output.
                df_dest52_final.drop(["dest_easting", "dest_northing"], axis=1, inplace=True)

                # update feature headings if infrastructure
                if feature_name == "infra_points" or feature_name == "infra_water_points":
                    df_dest52_final.columns = infra_column_list
                    df_dest52_final.insert(9, "CONFIDENCE", 2)
                    df_dest52_final.drop(columns=["CONDITION"], inplace=True)
                    df_dest52_final.insert(12, "DELETE", 0)
                    df_dest52_final.insert(13, 'STATUS', 'Raw')
                    #print('+'*50)
                    #print(list(df_dest52_final))

                if feature_name == "weeds":
                    df_dest52_final.rename(columns=weed_column_dict, inplace=True)

                    df_dest52_final.drop(columns=["feature", "property", "prop_code"], inplace=True)
                    df_dest52_final.insert(0, "SITE_ID", np.nan)
                    df_dest52_final.insert(1, "SITE_MON", np.nan)
                    df_dest52_final["SIZE_DIA_M"] = df_dest52_final["SIZE_DIA_M"].astype(int)
                    df_dest52_final["DENS_CAT"] = df_dest52_final["DENS_CAT"].astype(int)
                    df_dest52_final.insert(5, "LAT_94", df_dest52_final.geometry.y)
                    df_dest52_final.insert(6, "LONG_94", df_dest52_final.geometry.x)
                    # df_dest52_final.insert(8, "RECORDER", np.nan)
                    df_dest52_final.rename({"NOTES": "COMMENTS", "Recorder": "RECORDER"}, inplace=True)

                # replace all 9999 values with np.nan
                df_dest52_final = df_dest52_final.replace(["9999", 9999], np.nan)
                df_dest52_final = df_dest52_final.replace("Nan", np.nan)

                # export shapefile to export directory
                df_dest52_final.to_file(export_dir + "\\shapefile\\" + feature_name + "_dest_52_to_GDA94.shp",
                                        driver="ESRI Shapefile")
                # df_dest52_final['uid'] = df_dest52_final.index + 1
                df_dest52_final.to_csv(
                    export_dir + "\\csv\\" + feature_name + "_dest_52_to_GDA94.csv")  # , index_col = False)

            # ----------------------------------------------- WGSz53 -----------------------------------------------
            if len(dest_gdf_wgsz53_list) >= 1:

                df = pd.concat(dest_gdf_wgsz53_list)

                # create two copies of the original df dataframe
                df_orig53 = df.copy()
                df_dest53 = df.copy()

                # ------------------------------------------ WGSz53 origin -----------------------------------------

                """# drop working columns excluding the new output easting and northing
                df_orig53.drop(orig_drop_list, axis=1, inplace=True)

                # create a geo-dataframe using the original easting and northing in WGSz53
                export_shapefile_shift2_fn(df_orig53, 32753, directory + "\\shapefile\\", "orig_easting",
                                           "orig_northing",
                                           "WGS84z53",
                                           "origin")
                # convert to GDA94 geographic
                #df_orig53.to_crs(epsg=4283)
                df_orig53_final, crs_name = re_project_geo_df_fn(4283, df_orig53)

                # drop the easting and northing values from the output.
                df_orig53_final.drop(["orig_easting", "orig_northing"], axis=1, inplace=True)

                # update feature headings if infrastructure
                if feature_name == "infra_points" or feature_name == "infra_water_points":
                    df_orig53_final.columns = infra_column_list
                
                if feature_name == "weeds":
                    df_orig53_final.rename(columns=weed_column_dict, inplace=True)
                    print(list(df_orig53_final))
                    df_orig53_final.drop(columns=["feature", "property", "prop_code"], inplace=True)
                    df_orig53_final.insert(0, "SITE_ID", np.nan)
                    df_orig53_final.insert(1, "SITE_MON", np.nan)
                    print(list(df_orig53_final))

                # replace all 9999 values with np.nan
                df_orig53_final = df_orig53_final.replace(["9999", 9999], np.nan)

                # export shapefile to export directory
                df_orig53_final.to_file(export_dir + "\\shapefile\\" + feature_name + "_orig_53_to_GDA94.shp",
                                  driver="ESRI Shapefile")
                df_orig53_final.to_csv(export_dir + "\\csv\\" + feature_name + "_orig_53_to_GDA94.csv") #, index_col = False)"""

                # --------------------------------------- WGSz53 destination ---------------------------------------

                # drop working columns excluding the new output easting and northing

                df_dest53.drop(dest_drop_list, axis=1, inplace=True)

                # create a geo-dataframe using the destination easting and northing in WGSz53
                export_shapefile_shift2_fn(df_dest53, 32753, directory + "\\shapefile\\", "dest_easting",
                                           "dest_northing",
                                           "WGS84z53",
                                           "dest")
                # convert to GDA94 geographic
                # df_dest53.to_crs(epsg=4283)
                df_dest53_final, crs_name = re_project_geo_df_fn(4283, df_dest53)

                # drop the easting and northing values from the output.
                df_dest53_final.drop(["dest_easting", "dest_northing"], axis=1, inplace=True)

                # update feature headings if infrastructure
                if feature_name == "infra_points" or feature_name == "infra_water_points":
                    df_dest53_final.columns = infra_column_list
                    df_dest53_final.insert(9, "CONFIDENCE", 2)
                    df_dest53_final.drop(columns=["CONDITION"], inplace=True)
                    df_dest53_final.insert(12, "DELETE", 0)
                    df_dest53_final.insert(13, 'STATUS', 'Raw')
                    #print('+' * 50)
                    #print(list(df_dest52_final))

                if feature_name == "weeds":
                    df_dest53_final.rename(columns=weed_column_dict, inplace=True)

                    df_dest53_final.drop(columns=["feature", "property", "prop_code"], inplace=True)
                    df_dest53_final.insert(0, "SITE_ID", np.nan)
                    df_dest53_final.insert(1, "SITE_MON", np.nan)
                    df_dest53_final["SIZE_DIA_M"] = df_dest53_final["SIZE_DIA_M"].astype(int)
                    df_dest53_final["DENS_CAT"] = df_dest53_final["DENS_CAT"].astype(int)
                    df_dest53_final.insert(5, "LAT_94", df_dest53_final.geometry.y)
                    df_dest53_final.insert(6, "LONG_94", df_dest53_final.geometry.x)
                    # df_dest53_final.insert(8, "RECORDER", np.nan)
                    df_dest53_final.rename({"NOTES": "COMMENTS", "recorder": "RECORDER"}, inplace=True)

                # replace all 9999 values with np.nan
                df_dest53_final = df_dest53_final.replace(["9999", 9999], np.nan)
                df_dest53_final = df_dest53_final.replace("Nan", np.nan)

                # export shapefile to export directory
                df_dest53_final.to_file("{0}\\shapefile\\{1}_dest_53_to_GDA94.shp".format(export_dir, feature_name),
                                        driver="ESRI Shapefile")
                # df_dest53_final['uid'] = df_dest52_final.index + 1
                df_dest53_final.to_csv(
                    "{0}\\csv\\{1}_dest_53_to_GDA94.csv".format(export_dir, feature_name))  # , index_col = False)

            if feature_name == "unidentified":
                import step4_3_unidentified_doc
                step4_3_unidentified_doc.main_routine(df_dest52_final, df_dest53_final, export_dir, pastoral_estate,
                                                      user_df)

        else:
            pass

    return export_dir


if __name__ == "__main__":
    main_routine()
