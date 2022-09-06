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

    # ('glob_dir_fn started')
    # create an empty list
    list_df = []

    for file in glob.glob(export_dir + '\\*.csv'):
        # read in all zonal stats csv
        print('file: ', file)
        df = pd.read_csv(file, index_col=0)
        # print('df: ', df)
        # append all zonal stats DataFrames to a list.
        list_df.append(df)
    print('list_df: ', len(list_df))
    # if list of dataframes is greater than 1 concatenate
    if len(list_df) <= 1:
        df_concat = list_df[0]
        # df_concat.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\df_concat.csv")
    else:
        df_concat = pd.concat(list_df)
        df_concat.dropna(axis=0, inplace=True)
    # print('df_concat: ', df_concat)
    # print('list_df: ', list_df)
    return df_concat


def select_features_fn(df, directory, uif_number):
    # create lists for data extraction
    group = []
    feature = []
    cond_label = []
    date_rec = []
    datum = []
    lat = []
    lon = []
    acc = []
    dist = []
    bear = []
    final_output_list = []

    for i in range(10):
        n = str(i + 1)
        uid_feature = uif_number
        group = df['group'].iloc[0]
        feature = df['feature'].iloc[0]
        cond_label = df['cond_label'].iloc[0]
        date_rec = df['date_rec'].iloc[0]
        datum = df['datum'].iloc[0]
        lat = df['lat' + n].iloc[0]
        lon = df['lon' + n].iloc[0]
        acc = df['acc' + n].iloc[0]
        dist = df['dist' + n].iloc[0]
        bear = df['bear' + n].iloc[0]

        output_list = [uid_feature, group, feature, cond_label, date_rec, datum, lat, lon, acc, dist, bear]
        print(uif_number, '======', output_list)
        final_output_list.append(output_list)

    df = pd.DataFrame(final_output_list,
                      columns=['uid_feature', 'group', 'feature', 'cond_label', 'date_rec', 'datum', 'lat',
                               'lon', 'acc', 'dist', 'bear'])

    # Get index for rows with 0 lat i.e. no lon or lat taken
    observations = df[df['lat'] == 0].index

    # Delete these row (observations) from dataframe
    df.drop(observations, inplace=True)
    print(df)
    # df.to_csv(directory + '\\shapefile\\test_loop_outputs' + str(f_number) + '.csv')
    return df


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


def export_shapefile_fn(df, epsg, directory, i, crs_name):
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs=epsg)
    shp_output = (directory + '\\' + str(i) + '_points_' + crs_name + '.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    print('-', shp_output, str(i))
    return gdf


def export_shapefile_shift_fn(df, epsg, directory, lon, lat, crs_name, i):
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = (directory + '\\' + 'points_dest_' + str(i) + '-' + crs_name + '.shp')
    gdf.to_file(shp_output, driver='ESRI Shapefile')
    # print('-', shp_output)
    return gdf


def move_lat_long_decimal_degrees(lat, lon, bearing, distance_m):
    import math

    R = 6378.1  # Radius of the Earth
    bearing_km = math.degrees(bearing)  # convert bearing(degrees) to radians.
    d = (distance_m / 1000)  # Distance in km

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

    # print(lat2)
    # print(lon2)
    return lat2, lon2


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

    print("easting_northing_fn completed")
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

    print('extract_variables_for_destination_f completed')
    return distance_m, bearing, lat_orig, lon_orig


'''def project_and_extract_easting_northing_fn(epsg_int, gdf):


    # project dataframe to to wgsz52 (32752)
    projected_gdf, crs_name = re_project_geo_df_fn(32752, gdf)
    print('projected_gdf: ', projected_gdf.crs)

    orig_easting_list = []
    orig_northing_list = []

    for i in projected_gdf.uid.unique():
        print(i)

        # extract eastings and northing values for geometry feature
        orig_easting = projected_gdf['geometry'].x
        orig_northing = projected_gdf['geometry'].y

        orig_easting_list.append(orig_easting)
        orig_northing_list.append(orig_northing)

    projected_gdf['orig_easting'] = orig_easting_list
    projected_gdf['orig_northing'] = orig_northing_list

    projected_gdf.to_file(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\projected.shp")
    df = pd.DataFrame(projected_gdf)
    df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\projected.csv")
    return projected_gdf, orig_easting, orig_northing'''


def project_and_extract_easing_northing_fn(epsg_int, gdf):
    # project dataframe to to wgsz52 (32752)
    projected_gdf, crs_name = re_project_geo_df_fn(epsg_int, gdf)
    print(projected_gdf.crs)

    # extract eastings and northing values for geometry feature
    projected_gdf["orig_easting"] = projected_gdf["geometry"].x
    projected_gdf["orig_northing"] = projected_gdf["geometry"].y

    # extract original easting and northing values following geo-dataframe projection to wgsz52
    orig_easting = projected_gdf["orig_easting"].iloc[0]
    orig_northing = projected_gdf["orig_northing"].iloc[0]

    return projected_gdf, orig_easting, orig_northing


def add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing, epsg_int):
    # add new values to geo-dataframe and add epsg value for reference
    projected_gdf['l_sin'] = l_sin
    projected_gdf['l_cos'] = l_cos
    projected_gdf['dest_easting'] = dest_easting
    projected_gdf['dest_northing'] = dest_northing
    projected_gdf['epsg'] = epsg_int

    return projected_gdf


def export_shapefile_shift2_fn(df, epsg, directory, lon, lat, crs_name, orig):
    # create offset geoDataFrame and export a shapefile lon lat set to center points.
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs=epsg)
    shp_output = (directory + "\\" + "points_" + orig + "_" + crs_name + ".shp")
    gdf.to_file(shp_output, driver="ESRI Shapefile")
    print("-", shp_output)
    return gdf


def convert_to_line_fn(df_subset, directory, uif_number, Point, LineString, epsg_int):  # , i):
    # epsg = 4326
    print('uif_number - convert to line', uif_number)
    print('epsg: ', epsg_int)
    import pandas as pd

    # zip the coordinates into a point object and convert to a GeoData Frame
    geometry = [Point(xy) for xy in zip(df_subset.orig_easting, df_subset.orig_northing)]
    geo_df = gpd.GeoDataFrame(df_subset, geometry=geometry, crs=epsg_int)
    print('got to here')

    # , 'feature', 'cond_label', 'date_rec', 'datum'
    geo_df2 = geo_df.groupby(['uid_feature', 'group', 'cond_label', 'date_rec', 'datum'])['geometry'].apply(
        lambda x: LineString(x.tolist()))
    geo_df2 = gpd.GeoDataFrame(geo_df2, geometry='geometry', crs=epsg_int)

    geo_df2.to_file(directory + '\\shapefile\\line_' + str(uif_number) + '_' + str(epsg_int) + '_origin.shp',
                    driver="ESRI Shapefile")

    print(uif_number, 'crs = ', geo_df2.crs)

    return geo_df2


def main_routine(temp_dir, feature):
    # import modules
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point, LineString, shape

    print('step3_2_compile_points_infrastructure.py INITIATED')

    # create variable directory containing the path to previously exported csv files.
    directory = temp_dir + '\\' + 'infra_lines'
    # call the glob_dir function to extract and concatenate when required all csv files into one dataframe.
    df = glob_dir_fn(directory)

    # insert uid_feature column at the beginning of the data frame
    df.insert(0, 'uid_feature', '')
    df['uid_feature'] = df.index + 1

    df.to_csv(directory + '\\df_with_uid_feature.csv')

    df_list = []

    # for loop through unique identifier column variables to transform dataframe shape.
    for uif_number in df.uid_feature.unique():
        print("uif_number: ", uif_number)
        # filter the data frame based on uid_feature variable
        uid_subset_df = df[df['uid_feature'] == uif_number]
        print(uid_subset_df.group.iloc[0])
        # call the select_features_fn function to subset the dataframe.
        df_subset = select_features_fn(uid_subset_df, directory, uif_number)
        df_subset.to_csv(directory + "\\concat_df_orig" + str(uif_number) + ".csv")

        #df_list.append(df_subset)
    # concatenate all outputs to one data frame.
    """if len(df_list) <= 1:
        df_concat = df_list[0]

    else:
        df_concat = pd.concat(df_list)
        df_concat.dropna(axis=0, inplace=True)"""

    df_subset.to_csv(directory + "\\subset" + str(uif_number) + "_orig.csv")
    print("DF_concat created")

    # --------------------------------------- project dataframes --------------------------------------------

    # insert uid_feature column at the beginning of the data frame
    df_subset = df_subset.reset_index(drop=True)
    df_subset.insert(0, 'uid', '')
    df_subset['uid'] = df_subset.index + 1

    df_subset.to_csv(directory + "\\test1" + str(uif_number) + ".csv")

    epsg_list = [32752, 32753]

    df_list_project = []
    for epsg_int in epsg_list:

        dest_gdf_list = []
        # loop though all unique observations from the data frame (uid)
        for i in df_subset["uid"].unique():
            print(i)

            # filter the dataframe based on the unique value index_date_time variable (i.e. 1 observation).
            df = df_subset[df_subset["uid"] == i]

            # extract the datum from the observation and ensure it is all in upper case.
            datum = df.datum.iloc[0].upper()
            print('datum: -------------------------------', datum)

            # call the epsg_fn function to extract the epsg value as an integer storing it in the variable epsg
            epsg = epsg_fn(datum)
            print("epsg_fn completed")

            # call the projection_file_name to extract two variables crs_name string object for saving a file and the
            # crs_output epsg dictionary.
            crs_name, crs_output = projection_file_name_fn(epsg)
            print("projection_file_name_fn completed")
            print(crs_name, crs_output)

            # Create a point shapefile using the (n) lat and lon values.
            gdf = export_shapefile_fn(df, epsg, directory + "\\temp_shape", i, crs_name)
            print(gdf.crs)
            print("export_shapefile_fn completed")

            # extract for variables for shifting the point to a new location, based on

            distance_m, bearing, lat_orig, lon_orig = extract_variables_for_destination_fn(
                gdf, "lat", "lon", "dist", "bear")
            print("extract_variables_for_destination_fn completed")

            # call the project_and_extract_easting_northing_fn function to extract and store the original eastings/nothings
            projected_gdf, orig_easting, orig_northing = project_and_extract_easing_northing_fn(epsg_int, gdf)
            print("project_and_extract_easting_northing_fn completed")
            # call the easting_northing_fn function to extract destination eastings and northings
            l_sin, l_cos, dest_easting, dest_northing = easting_northing_fn(
                orig_easting, orig_northing, bearing, distance_m)
            print("easting_northing_fn completed")

            projected_gdf = add_destination_variables(projected_gdf, l_sin, l_cos, dest_easting, dest_northing,
                                                      epsg_int)

            # subset and reorder geo-dataframe
            projected_gdf_subset = projected_gdf[[
                "uid", "uid_feature", "group", "feature", "feature", "cond_label", "date_rec", "datum", "epsg",
                "orig_easting", "orig_northing", "l_sin", "l_cos", "dest_easting", "dest_northing"]]

            # convert to pandas dataframe (i.e. remove geometry)
            projected_subset = pd.DataFrame(projected_gdf_subset)

            # call the projection_file_name_fn function to extract the crs_name
            crs_name, crs_output = projection_file_name_fn(epsg_int)
            print("here?")
            dest_gdf = export_shapefile_shift_fn(projected_subset, epsg_int, directory + "\\shapefile\\",
                                                 "dest_easting", "dest_northing", crs_name, i)

            # append geo dataframe to list
            dest_gdf_list.append(dest_gdf)

        print("dest_gdf_list", dest_gdf_list)
        # concatenate all  outputs to one data frame.
        print('length of gdf_list: ', len(dest_gdf_list))
        if len(dest_gdf_list) <= 1:
            df_concat = dest_gdf_list[0]

        else:
            df_concat = pd.concat(dest_gdf_list)
            df_concat.dropna(axis=0, inplace=True)
        print(str(epsg_int))

        df = pd.DataFrame(df_concat)
        #df.to_csv(directory + "\\fuckthis" + str(i) + ".csv")
        #df.drop('geometry', axis='columns', inplace=True)
        df_list_project.append(df)


    #dest_gdf.to_csv(directory + "\\output_" + str(uif_number) + ".csv")

    # ----------------------------------------- convert to line ----------------------------------------------
    print('here...')
    print('hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
    print('df_list_project: ', len(df_list_project))
    if len(df_list_project) <= 1:
        df_concat = df_list_project[0]

    else:
        df_concat = pd.concat(df_list_project)
        df_concat.dropna(axis=0, inplace=True)
    print(str(epsg_int))
    df_concat.to_csv(directory + "\\fuckthis_concat.csv")

    for uif_number in df_concat.uid_feature.unique():
        subset_df = df_concat[df_concat['uid_feature'] == uif_number]
        df = pd.DataFrame(subset_df)
        df.to_csv(directory + "\\fuckthis" + str(uif_number) + ".csv")
        df.drop('geometry', axis='columns', inplace=True)

        convert_to_line_fn(df, directory, uif_number, Point, LineString, epsg_int)



if __name__ == '__main__':
    main_routine()
