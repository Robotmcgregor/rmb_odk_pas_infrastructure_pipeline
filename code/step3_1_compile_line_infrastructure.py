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
import numpy as np
import geopandas as gpd
import warnings

warnings.filterwarnings("ignore")


def glob_dir_fn(export_dir):
    """  Search a specified directory and concatenate all records to a DataFrame.

    :param export_dir: list object containing the file paths within the directory.
    :return df_concat: pandas data frame object - all zonal stats csv files concatenated together.
    """

    # create an empty list
    list_df = []

    for file in glob.glob(export_dir + "\\*.csv"):
        # read in all zonal stats csv
        df = pd.read_csv(file, index_col=0)
        # append all zonal stats DataFrames to a list.
        list_df.append(df)
    # if list of dataframes is greater than 1 concatenate
    if len(list_df) <= 1:
        df_concat = list_df[0]
    else:
        df_concat = pd.concat(list_df)
        df_concat.dropna(axis=0, inplace=True)

    return df_concat


def select_features_fn(df, uif_number):
    """ Create a dataframe based on lists of lists - each dataframe red in has been separated into a separate feature
    (uif_number). a list of lists is created one list per lat and long record. Null lat long values are removed.

    :param df: pandas dataframe object.
    :param uif_number: integer object containing the current unique identifiable feature number assigned
    :return df: pandas dataframe object
    """
    final_output_list = []

    for i in range(10):
        n = str(i + 1)
        uid_feature = uif_number
        district = df["district"].iloc[0]
        prop = df["property"].iloc[0]
        prop_code = df["prop_code"].iloc[0]
        feature_group = df["feature_group"].iloc[0]
        feature = df["feature"].iloc[0]
        label = df["label"].iloc[0]
        condition = df["condition"].iloc[0]
        comment = df["comment"].iloc[0]
        date_rec = df["date_rec"].iloc[0]
        date_curr = np.nan
        source = "NTG Rangelands Monitoring Branch"
        display = "Yes"
        length_m = 0
        # area_km2 = 0
        datum = df["datum"].iloc[0]
        lat = df["lat" + n].iloc[0]
        lon = df["lon" + n].iloc[0]
        acc = df["acc" + n].iloc[0]
        dist = df["dist" + n].iloc[0]
        bear = df["bear" + n].iloc[0]

        output_list = [uid_feature, feature_group, feature, label, district, prop, prop_code, date_rec, date_curr,
                       source,
                       length_m, display, comment, datum, lat, lon, acc, dist, bear]  # area_km2,

        final_output_list.append(output_list)

    df = pd.DataFrame(final_output_list,
                      columns=["uid_feature", "feature_group", "feature", "label", "district", "property", "prop_code",
                               "date_rec",
                               "date_curr", "source", "length_m", "display", "comment", "datum", "lat",
                               "lon", "acc",
                               "dist", "bear"])  # "area_km2",

    # Get index for rows with 0 lat i.e. no lon or lat taken
    observations = df[df["lat"] == 0].index

    # Delete these row (observations) from dataframe
    df.drop(observations, inplace=True)

    return df


def epsg_fn(datum):
    """ define the epsg number based on the rows datum.

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
    """ Creates two variables, crs_name and crs_output depending on a geo-dataframe's epsg code.

    :param epsg: integer object containing the relevant crs code
    :return crs_output: dictionary object containing the crs information.
    :return crs_name: string object containing the end filename relevant to the epsg crs.
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
    """ Creates two variables, crs_name and crs_output depending on a geo-dataframe's epsg code, and projects the
    current geo-dataframe.

    :param epsg: integer object containing the relevant crs code
    :param geo_df: geo-dataframe object.
    :return projected_df: geo-dataframe object that has been projected to the epsg crs.
    :return crs_name: string object containing the end filename relevant to the epsg crs.
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = "GDA94z52"
    elif epsg_int == 28353:
        crs_name = "GDA94z53"
    elif epsg_int == 4283:
        crs_name = "GDA94"
    elif epsg_int == 32752:
        crs_name = "WGS84z52"
    elif epsg_int == 32753:
        crs_name = "WGS84z53"
    elif epsg_int == 3577:
        crs_name = "Albers"
    elif epsg_int == 4326:
        crs_name = "GCS_WGS84"
    else:
        crs_name = "not_defined"

    # Project DF to epsg value
    projected_df = geo_df.to_crs(epsg)

    return projected_df, crs_name


def export_shapefile_fn(df, epsg, directory, i, crs_name):
    """ convert a Pandas dataframe to a geo-dataframe and export it as an ESRI shapefile.

    :param df: pandas dataframe object
    :param epsg: integer object containing the crs information
    :param directory: string object containing the path to the current working directory
    :param crs_name: string object containing the file ending for the relevant crs
    :param i: integer object containing the current uid from the current for loop
    :return gdf: geo-dataframe object created from the pandas dataframe.
    """

    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=epsg)
    shp_output = ("{0}\\{1}_points{2}.shp".format(directory, str(i), crs_name))
    gdf.to_file(shp_output, driver="ESRI Shapefile")
    # print("-", shp_output, str(i))
    return gdf


def export_shapefile_shift_fn(df, epsg, directory, lon, lat, crs_name, i):
    """ convert a Pandas dataframe to a geo-dataframe and export it as an ESRI shapefile.

    :param df: pandas dataframe object
    :param epsg: integer object containing the crs information
    :param directory: string object containing the path to the current working directory
    :param lon: string object containing the latitude column name for the dataframe (df)
    :param lat: string object containing the longitude column name for the dataframe (df)
    :param crs_name: string object containing the file ending for the relevant crs
    :param i: integer object containing the current uid from the current for loop
    :return gdf: geo-dataframe object created from the pandas dataframe.
    """
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = ("{0}\\points_dest_{1}_{2}.shp".format(directory, str(i), crs_name))
    gdf.to_file(shp_output, driver="ESRI Shapefile")
    # print("-", shp_output)
    return gdf


def easting_northing_fn(easting, northing, bearing, distance_m):
    """ Calculate the location of an individual point based on its original location, bearing and distance.

    :param easting: float object containing an individual points easting UTM information.
    :param northing: float object containing an individual points northing UTM information.
    :param bearing: float object containing an individual points bearing information.
    :param distance_m: float object containing on individual points distance from capture information.
    :return l_sin: float object the amount that the original easting point needs to be moved.
    :return l_cos: float object the amount that the original northing point needs to be moved.
    :return dest_easting: float object containing the final easting UTM information.
    :return dest_northing: float object containing the final northing UTM information.
    """

    l_sin = distance_m * math.sin(math.radians(bearing))
    l_cos = distance_m * math.cos(math.radians(bearing))

    dest_easting = float(easting) + l_sin
    dest_northing = float(northing) + l_cos

    return l_sin, l_cos, dest_easting, dest_northing


def extract_variables_for_destination_fn(gdf, lat, lon, dist, bear):
    """ extract a points lat and lon information, converting nan to 0

    :param gdf: geo-dataframe object.
    :param lat: string object containing the geo-dataframes latitude column heading.
    :param lon: string object containing the geo-dataframes longitude column heading.
    :param dist: string object containing the geo-dataframes distance column heading.
    :param bear: string object containing the geo-dataframes bearing column heading.
    :return distance_m: float object containing the points distance from capture information.
    :return bearing: float object containing the points bearing from capture information.
    :return lat_orig: float object containing the points latitude information.
    :return lon_orig: float object containing the points longitude information.
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


def project_and_extract_easting_northing_fn(epsg_int, gdf):
    """ Project the geo-dataframe extract the projected easting and northing information.

    :param epsg_int: integer object containing the epsg crs code.
    :param gdf: geo-dataframe object.
    :return projected_gdf: geo-dataframe in new projection.
    :return orig_easting: float object containing the x value from the geometry column.
    :return orig_northing: float object containing the y value from the geometry column.
    """
    # call the re_project_geo_df_fn function to project dataframe
    projected_gdf, crs_name = re_project_geo_df_fn(epsg_int, gdf)

    # extract eastings and northing values for geometry feature
    projected_gdf["orig_easting"] = projected_gdf["geometry"].x
    projected_gdf["orig_northing"] = projected_gdf["geometry"].y

    # extract original easting and northing values following geo-dataframe projection to wgsz52
    orig_easting = projected_gdf["orig_easting"].iloc[0]
    orig_northing = projected_gdf["orig_northing"].iloc[0]

    return projected_gdf, orig_easting, orig_northing


def add_destination_variables_fn(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int):
    """ Add variables to the geo-dataframe.

    :param projected_gdf: geo-dataframe object.
    :param l_sin: float object the amount that the original easting point needs to be moved.
    :param l_cos: float object the amount that the original northing point needs to be moved.
    :param dest_easting: float object containing the final easting UTM information.
    :param dest_northing: float object containing the final northing UTM information.
    :param epsg_int: float object containing the crs code.
    :return projected_gdf: geo-dataframe.
    """
    # add new values to geo-dataframe and add epsg value for reference
    projected_gdf["l_sin"] = l_sin
    projected_gdf["l_cos"] = l_cos
    projected_gdf["dest_easting"] = dest_easting
    projected_gdf["dest_northing"] = dest_northing
    projected_gdf["epsg"] = epsg_int

    return projected_gdf


def export_shapefile_shift2_fn(df, epsg, directory, lon, lat, crs_name, orig):
    """

    :param df: geo-dataframe object.
    :param epsg: integer object containing the epsg crs code.
    :param directory: string object containing the path to the export drive.
    :param lon: string object for the dataframes longitude column header.
    :param lat: string object for the dataframes latitude column header.
    :param crs_name: string object containing crs file name.
    :param orig:
    :return:
    """

    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = ("{0}\\points_{1}_{2}.shp".format(directory, orig, crs_name))
    gdf.to_file(shp_output, driver="ESRI Shapefile")
    # print("-", shp_output)
    return gdf


def convert_to_line_fn(df_subset, directory, uif_number, Point, LineString, epsg_int, location):
    """ Convert a dataframe of points into a singe line.

    :param df_subset: geo-dataframe object.
    :param directory: string object containing the output directory path
    :param uif_number: integer object containing the unique variable from the geo-dataframe
    :param Point:
    :param LineString:
    :param epsg_int:
    :param location:
    :return:
    """
    # zip the coordinates into a point object and convert to a GeoData Frame
    geometry = [Point(xy) for xy in zip(df_subset[location + "_easting"], df_subset[location + "_northing"])]
    geo_df = gpd.GeoDataFrame(df_subset, geometry=geometry, crs=epsg_int)

    geo_df2 = \
    geo_df.groupby(["uid_feature", "feature_group", "feature", "label", "date_rec", "district", "property", "prop_code",
                    "source", "length_m", "display", "comment"])["geometry"].apply(
        lambda x: LineString(x.tolist()))  # "area_km2",

    geo_df2 = gpd.GeoDataFrame(geo_df2, geometry="geometry", crs=epsg_int)

    geo_df2.to_file("{0}\\shapefile\\line_{1}_{2}_{3}.shp".format(directory, str(uif_number), str(epsg_int),
                                                                  str(location)), driver="ESRI Shapefile")

    return geo_df2


def concat_list_to_df_fn(list_a):
    """ Define the method used to concatenate the data to a dataframe based on the length of the list.

    :param list_a: list object or a list of list object.
    :return df_concat: pandas dataframe object crated from the input list or list of lists (list_a):
    """

    if len(list_a) <= 1:
        # df_concat = pd.DataFrame(list_a[0]).transpose()
        df_concat = pd.concat(list_a)

    else:
        df_concat = pd.concat(list_a)

    return df_concat


def get_distances(coordinates):
    """ Calculate the distance between the each point and create a dataframe.

    :param coordinates: geo-dataframe object.
    :return pd.DataFrame(distances): pandas dataframe object
    """

    traces = len(coordinates) - 1
    distances = [None] * traces
    for i in range(traces):
        start = [coordinates.iloc[i]["dest_northing"], coordinates.iloc[i]["dest_easting"]]
        finish = [coordinates.iloc[i + 1]["dest_northing"], coordinates.iloc[i + 1]["dest_easting"]]
        distance = math.sqrt(((start[0] - finish[0]) ** 2) + ((start[1] - finish[1]) ** 2))
        distances[i] = {
            "start": start,
            "finish": finish,
            "path distance": distance,
        }

    return pd.DataFrame(distances)


def main_routine(temp_dir, feature, export_dir_path):
    # import modules
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point, LineString

    # create variable directory containing the path to previously exported csv files.
    directory_lines_point = "{0}\\lines_point".format(temp_dir)

    os.mkdir(directory_lines_point)

    # create variable directory containing the path to previously exported csv files.
    directory = "{0}\\infra_lines".format(temp_dir)
    export_dir = "{0}\\infra_lines".format(export_dir_path)

    # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.

    for file in glob.glob("{0}\\*.csv".format(directory)):
        if file:

            df = pd.read_csv(file)

            # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.
            df = glob_dir_fn(directory)

            # insert uid_feature column at the beginning of the data frame
            df.insert(0, "uid_feature", "")
            df["uid_feature"] = df.index + 1

            df_photo = df[
                ["uid_feature", "feature", "date_rec", "district", "property", "prop_code", "photo1", "photo2",
                 "photo3"]]
            df_photo.to_csv("{0}\\csv\\infra_lines_photo.csv".format(export_dir))

            df_list = []

            # for loop through unique identifier column variables to transform dataframe shape.
            for uif_number in df.uid_feature.unique():

                # filter the data frame based on uid_feature variable
                uid_subset_df = df[df["uid_feature"] == uif_number]

                # call the select_features_fn function to subset the dataframe.
                df_subset = select_features_fn(uid_subset_df, uif_number)
                # remove any feature that is actually a point less than two points

                if len(df_subset.index) <= 1:

                    df_subset.to_csv("{0}\\lines_not_lines_df_orig{1}.csv".format(directory_lines_point,
                                                                                  str(uif_number)))
                else:

                    df_subset.to_csv("{0}\\concat_df_orig{1}.csv".format(directory, str(uif_number)))

                    df_list.append(df_subset)
            # concatenate all outputs to one data frame.
            if len(df_list) > 0:
                # print('df_list: ', df_list)
                df_concat2 = pd.concat(df_list)

                # --------------------------------------- project dataframes -------------------------------------------

                dest_gdf_WGS84z52_list = []
                dest_gdf_WGS84z53_list = []

                # insert uid_feature column at the beginning of the data frame
                df_concat = df_concat2.reset_index(drop=True)
                df_concat.insert(0, "uid", "")
                df_concat["uid"] = df_concat.index + 1
                df_concat["dist"] = df_concat["dist"].fillna(0)
                df_concat["bear"] = df_concat["bear"].fillna(0)

                # loop though all unique observations from the data frame (uid)
                for i in df_concat["uid"].unique():

                    # filter the dataframe based on the unique value index_date_time variable (i.e. 1 observation).
                    df = df_concat[df_concat["uid"] == i]
                    # extract the datum from the observation and ensure it is all in upper case.
                    datum = df.datum.iloc[0].upper()

                    # call the epsg_fn function to extract the epsg value as an integer storing it in the variable epsg
                    epsg = epsg_fn(datum)

                    # call the projection_file_name to extract two variables crs_name string object for saving a file and the
                    # crs_output epsg dictionary.
                    crs_name, crs_output = projection_file_name_fn(epsg)

                    # Create a point shapefile using the (n) lat and lon values.
                    gdf = export_shapefile_fn(df, epsg, directory + "\\temp_shape", i, crs_name)

                    # extract for variables for shifting the point to a new location, based on

                    distance_m, bearing, lat_orig, lon_orig = extract_variables_for_destination_fn(
                        gdf, "lat", "lon", "dist", "bear")

                    epsg_list = [32752, 32753]
                    for epsg_int in epsg_list:

                        # call the project_and_extract_easting_northing_fn function to extract and store the original
                        # eastings/nothings
                        projected_gdf, orig_easting, orig_northing = project_and_extract_easting_northing_fn(epsg_int, gdf)

                        # call the easting_northing_fn function to extract destination eastings and northings
                        l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(
                            orig_easting, orig_northing, bearing, distance_m)

                        projected_gdf = add_destination_variables_fn(projected_gdf, l_sin, l_cos, dest_easting,
                                                                     dest_northing,
                                                                     epsg_int)
                        # subset and reorder geo-dataframe

                        projected_gdf_subset = projected_gdf[[
                            "uid", "uid_feature", "feature_group", "feature", "label", "district", "property",
                            "prop_code", "date_rec",
                            "date_curr", "source", "length_m", "display", "comment", "datum", "lat", "lon",
                            "acc",
                            "dist", "bear", "epsg", "orig_easting", "orig_northing", "l_sin", "l_cos", "dest_easting",
                            "dest_northing"]]  # "area_km2",

                        # convert to pandas dataframe (i.e. remove geometry)
                        projected_subset = pd.DataFrame(projected_gdf_subset)

                        # call the projection_file_name_fn function to extract the crs_name
                        crs_name, crs_output = projection_file_name_fn(epsg_int)

                        dest_gdf = export_shapefile_shift_fn(projected_subset, epsg_int, directory + "\\shapefile\\",
                                                             "dest_easting", "dest_northing", crs_name, i)
                        if epsg_int == 32752:
                            dest_gdf_WGS84z52_list.append(dest_gdf)
                        if epsg_int == 32753:
                            dest_gdf_WGS84z53_list.append(dest_gdf)
                # ------------------------------------------------------------------------------------------------------

                df_concat_WGS84z52 = concat_list_to_df_fn(dest_gdf_WGS84z52_list)
                df_concat_WGS84z53 = concat_list_to_df_fn(dest_gdf_WGS84z53_list)

                # -------------------------------------- calculate length of line --------------------------------------

                dest_length_wgs84z52_list = []

                for uid in df_concat_WGS84z52.uid_feature.unique():
                    coords = df_concat_WGS84z52[df_concat_WGS84z52["uid_feature"] == uid]
                    distances = get_distances(coords)

                    distances["total distance"] = distances["path distance"].cumsum()
                    # extract the total line length and fill the length_m feature.
                    coords["length_m"] = round((distances["total distance"].iloc[-1]), 2)

                    dest_length_wgs84z52_list.append(coords)

                df_concat_length_WGS84z52 = concat_list_to_df_fn(dest_length_wgs84z52_list)

                df_concat_length_WGS84z52.to_csv("{0}\\complete_line_wgs52.csv".format(directory))

                # ------------------------------------------------ 53 ------------------------------------------------------

                dest_length_wgs84z53_list = []

                for uid in df_concat_WGS84z53.uid_feature.unique():
                    coords = df_concat_WGS84z53[df_concat_WGS84z53["uid_feature"] == uid]

                    distances = get_distances(coords)
                    distances["total distance"] = distances["path distance"].cumsum()
                    # extract the total line length and fill the length_m feature.
                    coords["length_m"] = round((distances["total distance"].iloc[-1]), 2)

                    dest_length_wgs84z53_list.append(coords)

                df_concat_length_WGS84z53 = concat_list_to_df_fn(dest_length_wgs84z53_list)

                df_concat_length_WGS84z53.to_csv("{0}\\complete_line_wgs53.csv".format(directory))

                # ----------------------------------------- convert to line ----------------------------------------------

                orig_wgs84z52_list = []
                dest_wgs84z52_list = []
                orig_wgs84z53_list = []
                dest_wgs84z53_list = []

                location_list = ["orig", "dest"]
                for location in location_list:
                    for uif_number in df_concat_length_WGS84z52.uid_feature.unique():

                        subset_df = df_concat_length_WGS84z52[df_concat_length_WGS84z52["uid_feature"] == uif_number]
                        # remove any risk of a single point

                        if len(subset_df.index) > 1:

                            df = pd.DataFrame(subset_df)
                            df.drop("geometry", axis="columns", inplace=True)
                            df.to_csv("{0}\\drop_geom{1}.csv".format(directory, str(uif_number)))

                            lines_gdf = convert_to_line_fn(df, directory, uif_number, Point, LineString, 32752, location)
                            if location == "orig":
                                orig_wgs84z52_list.append(lines_gdf)
                            else:
                                dest_wgs84z52_list.append(lines_gdf)

                            lines_gdf.to_file("{0}\\line_output_{1}_WGS84z52.shp".format(directory, location),
                                              driver="ESRI Shapefile")

                    for uif_number in df_concat_length_WGS84z53.uid_feature.unique():
                        subset_df = df_concat_length_WGS84z53[df_concat_length_WGS84z53["uid_feature"] == uif_number]
                        # remove any risk of a single point
                        if len(subset_df.index) > 1:

                            df = pd.DataFrame(subset_df)
                            df.drop("geometry", axis="columns", inplace=True)

                            lines_gdf = convert_to_line_fn(df, directory, uif_number, Point, LineString, 32753, location)
                            if location == "orig":
                                orig_wgs84z53_list.append(lines_gdf)
                            else:
                                dest_wgs84z53_list.append(lines_gdf)
                            lines_gdf.to_file("{0}\\line_output_{1}_WGS84z53.shp".format(directory, location),
                                              driver="ESRI Shapefile")

                # --------------------------------------------- Origin -----------------------------------------------------

                """# call the concat_list_to_df_fn function to concatenate the list of dataframes.
                df_concat_line_orig_wgs84z52 = concat_list_to_df_fn(orig_wgs84z52_list)
                print("list column names 660: ", list(df_concat_line_orig_wgs84z52))
                df_concat_line_orig_wgs84z52.to_file(directory + "\\origin_line_WGS84z52.shp",
                                                     driver="ESRI Shapefile")
    
                # project to geographics GDA94
                projected_df = df_concat_line_orig_wgs84z52.to_crs(epsg=4283)
                projected_df.to_csv(export_dir + "\\csv\\infra_line_orig_52_to_GDA94.csv")
                print("projected_df column names 670: ", list(projected_df))
                projected_df.to_file(temp_dir + "\\temp_infra_line_orig_52_to_GDA94.shp",
                                     driver="ESRI Shapefile")
    
                new_proj = gpd.read_file(temp_dir + "\\temp_infra_line_orig_52_to_GDA94.shp")
                print(list(new_proj))
                new_proj.drop(["uid_featur"], axis=1, inplace=True)
                new_proj.insert(4, "DATECURR", np.nan)
                print(list(new_proj))
                new_proj.columns = ["FEATGROUP", "FEATURE", "LABEL", "DATE_INSP", "DATE_CURR", "PROPERTY",
                                    "PROP_TAG",
                                    "SOURCE",
                                    "LENGTH_M", "MAPDISPLAY", "NOTES",
                                    "geometry"] # "AREA_KM2", 
                print(list(new_proj))
                new_proj.to_file(export_dir + "\\shapefile\\infra_line_orig_52_to_GDA94.shp",
                                 driver="ESRI Shapefile")
    
    
                df_concat_line_orig_wgs84z53 = concat_list_to_df_fn(orig_wgs84z53_list)
                df_concat_line_orig_wgs84z53.to_file(directory + "\\orig_line_output_WGS84z53.shp",
                                                     driver="ESRI Shapefile")
    
                projected_df = df_concat_line_orig_wgs84z53.to_crs(epsg=4283)
                projected_df.to_csv(export_dir + "\\csv\\infra_line_orig_53_to_GDA94.csv")
                projected_df.to_file(temp_dir + "\\temp_infra_line_orig_53_to_GDA94.shp",
                                     driver="ESRI Shapefile")
    
                new_proj = gpd.read_file(temp_dir + "\\temp_infra_line_orig_53_to_GDA94.shp")
                print(list(new_proj))
                new_proj.drop(["uid_featur"], axis=1, inplace=True)
                new_proj.insert(4, "DATECURR", np.nan)
                print(list(new_proj))
                new_proj.columns = ["FEATGROUP", "FEATURE", "LABEL", "DATE_INSP", "DATE_CURR", "PROPERTY",
                                    "PROP_TAG",
                                    "SOURCE",
                                    "LENGTH_M", "MAPDISPLAY", "NOTES",
                                    "geometry"] #  "AREA_KM2",
                print(list(new_proj))
                new_proj.to_file(export_dir + "\\shapefile\\infra_line_orig_53_to_GDA94.shp",
                                 driver="ESRI Shapefile")
    
                #projected_df.to_csv(export_dir + "\\csv\\infra_line_orig_53_to_GDA94.csv")
                #projected_df.to_csv(export_dir + "\\csv\\infra_line_orig_53_to_GDA94.csv")"""

                # --------------------------------------------- Destination ------------------------------------------------

                # print('lines zone 52')
                df_concat_line_dest_wgs84z52 = concat_list_to_df_fn(dest_wgs84z52_list)
                df_concat_line_dest_wgs84z52.to_file("{0}\\destination_line__WGS84z52.shp".format(directory),
                                                     driver="ESRI Shapefile")

                projected_df52 = df_concat_line_dest_wgs84z52.to_crs(epsg=4283)
                # print('projected_df52: ', list(projected_df52.columns))
                projected_df52.to_csv("{0}\\csv\\infra_line_dest_52_to_GDA94.csv".format(export_dir))
                projected_df52.to_file("{0}\\temp_infra_line_dest_52_to_GDA94.shp".format(temp_dir),
                                       driver="ESRI Shapefile")

                new_proj52 = gpd.read_file("{0}\\temp_infra_line_dest_52_to_GDA94.shp".format(temp_dir))

                new_proj52.drop(["uid_featur"], axis=1, inplace=True)
                new_proj52.insert(4, "DATECURR", np.nan)

                new_proj52.columns = ["FEATGROUP", "FEATURE", "LABEL", "DATE_INSP", "DATE_CURR", "DISTRICT", "PROPERTY",
                                      "PROP_TAG",
                                      "SOURCE",
                                      "LENGTH_M", "MAPDISPLAY", "NOTES",
                                      "geometry"]  # "AREA_KM2",

                # print('new_project52: ', list(new_proj52.columns))
                new_proj52.insert(9, "CONFIDENCE", 2)
                new_proj52.insert(13, "DELETE", 0)
                new_proj52.insert(14, 'STATUS', 'Raw')

                # print('new_project52 insert: ', list(new_proj52.columns))
                new_proj52.to_file("{0}\\shapefile\\infra_line_dest_52_to_GDA94.shp".format(export_dir),
                                   driver="ESRI Shapefile")


                # print('lines zone 53')
                df_concat_line_dest_wgs84z53 = concat_list_to_df_fn(dest_wgs84z53_list)
                df_concat_line_dest_wgs84z53.to_file("{0}\\destination_line_WGS84z53.shp".format(directory),
                                                     driver="ESRI Shapefile")

                projected_df53 = df_concat_line_dest_wgs84z53.to_crs(epsg=4283)
                projected_df53.to_csv(export_dir + "\\csv\\infra_line_dest_53_to_GDA94.csv")
                # export shapefile to temp folder - not allowing column names to be changed.
                projected_df53.to_file(temp_dir + "\\temp_infra_line_dest_53_to_GDA94.shp",
                                       driver="ESRI Shapefile")

                new_proj53 = gpd.read_file("{0}\\temp_infra_line_dest_53_to_GDA94.shp".format(temp_dir))

                new_proj53.drop(["uid_featur"], axis=1, inplace=True)
                new_proj53.insert(4, "DATECURR", np.nan)

                new_proj53.columns = ["FEATGROUP", "FEATURE", "LABEL", "DATE_INSP", "DATE_CURR", "DISTRICT", "PROPERTY",
                                      "PROP_TAG",
                                      "SOURCE",
                                      "LENGTH_M", "MAPDISPLAY", "NOTES",
                                      "geometry"]  # "AREA_KM2",
                new_proj53.insert(9, "CONFIDENCE", 2)
                # print('new_project53: ', list(new_proj53.columns))
                new_proj53.insert(13, "DELETE", 0)
                new_proj53.insert(14, 'STATUS', 'Raw')
                # print('new_project53 insert: ', list(new_proj53.columns))
                new_proj53.to_file("{0}\\shapefile\\infra_line_dest_53_to_GDA94.shp".format(export_dir),
                                   driver="ESRI Shapefile")

                '''import fiona
                corporate_lines = fiona.open(r"E:\DENR\code\rangeland_monitoring\rmb_mapping_pipeline\assets\shapefiles\output_templates\mapping_pipeline_lines.shp")
                corp_schema = corporate_lines.schema
                print(corp_schema)'''

                new_proj53.to_file(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\test53.shp",
                                   driver="ESRI Shapefile")  # , schema=corp_schema)
                # ----------------------------------------- distance of shapefile ------------------------------------------

                """import distance
                distance.main_routine(projected_df)"""

            else:
                print(' -- insufficient number of points to make a line.')
                pass

    else:
        pass

    import step4_2_photo_url_csv
    step4_2_photo_url_csv.main_routine(export_dir, "infra_lines")


if __name__ == "__main__":
    main_routine()
