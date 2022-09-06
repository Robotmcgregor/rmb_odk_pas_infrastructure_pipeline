# !/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
from glob import glob
import pandas as pd
import geopandas as gpd
from datetime import datetime
from datetime import date

warnings.filterwarnings("ignore")


def dir_folders_fn(primary_dir, directory_dict):
    """
    Create directory tree within the temporary directory based on shapefile type.

    :param primary_dir: string object containing the newly created temporary directory path.
    :param directory_dict: dictionary object containing the shapefile types.
    :return direc: string object containing the path to the top of the directory tree (i.e. property name).
    """

    if not os.path.exists(primary_dir):
        os.mkdir(primary_dir)

    for key, value in directory_dict.items():
        shapefile_dir = ('{0}\\{1}'.format(primary_dir, key))

        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)


def property_path_fn(pastoral_districts_path):
    """ Create a path to all property sub-directories and return them as a list.

    :param path: string object containing the pastoral_districts_path to the Pastoral Districts directory.
    :return prop_list: list object containing the path to all property sub-directories.
    """
    # create a list of pastoral districts.
    dir_list = next(os.walk(pastoral_districts_path))[1]
    print('dir_list: ', dir_list)
    prop_list = []

    # loop through districts to get property name
    for district in dir_list:
        dist_path = os.path.join(pastoral_districts_path, district)

        property_dir = next(os.walk(dist_path))[1]

        # loop through the property names list
        for prop_name in property_dir:
            # join the path, district and property name to create a path to each property directory.
            prop_path = os.path.join(pastoral_districts_path, district, prop_name)
            # append all property paths to a list
            prop_list.append(prop_path)

    print(prop_list)
    return prop_list


def upload_download_path_fn(prop_list):
    """ Create a path to to the Server Upload and Download sub-directories for each property and return them as a list.

    :param prop_list: list object containing the path to all property sub-directories.
    :return upload_list: list object containing the path to each properties Server_Upload sub-directory.
    :return download_list: list object containing the path to each properties Server_Upload sub-directory.
    """
    upload_list = []
    download_list = []
    for prop_path in prop_list:
        upload_path = os.path.join(prop_path, 'Infrastructure', 'Server_Upload')
        upload_list.append(upload_path)
        download_path = os.path.join(prop_path, 'Infrastructure', 'Server_Download')
        download_list.append(download_path)

    return upload_list, download_list


def assets_search_fn(search_criteria, folder):
    """ Searches through a specified directory "folder" for a specified search item "search_criteria".

    :param search_criteria: string object containing a search variable including glob wildcards.
    :param folder: string object containing the path to a directory.
    :return files: string object containing the path to any located files or "" if none were located.
    """
    path_parent = os.path.dirname(os.getcwd())
    assets_dir = (path_parent + '\\' + folder)

    files = ""
    file_path = (assets_dir + '\\' + search_criteria)
    for files in glob(file_path):
        # print(search_criteria, 'located.')
        pass

    return files


def extract_paths_fn(upload_list, transition_dir, year):
    # transition_dir = r'Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT'
    year = '2021'
    folder_list = ['points', 'lines', 'polygons']
    print('upload list: ', upload_list)
    points_list = []
    lines_list = []
    poly_list = []

    for i in upload_list:
        # print(i)
        for n in folder_list:

            direct = os.path.join(i, year, n)
            output_dir = os.path.join(transition_dir, n)
            check_folder = os.path.isdir(direct)
            if check_folder:
                for files in glob(direct + '\\*.shp'):

                    # if not files.endswith('.shp.xml'):
                    gdf = gpd.read_file(files)
                    # shutil.copy(files, output_dir)

                    if n == 'points':
                        points_list.append(gdf)
                        # points_df = df_concat
                    elif n == 'lines':
                        lines_list.append(gdf)
                        # lines_df = df_concat
                    elif n == 'polygons':
                        poly_list.append(gdf)
                        # poly_df = df_concat
                    else:
                        pass
                    """else:
                        print(files, 'not copied')"""

    return points_list, lines_list, poly_list


def check_column_names(df_list, asset_dir, file_end):
    """ Sort input dataframe based on correct column headings.

    :param df_list: list object containing open dataframes extracted from the pastoral districts directory.
    :param asset_dir: directory containing correct empty dataframe structures.
    :param file_end: string object containing the file name.
    :return checked_list: list object containing dataframes that matched.
    :return faulty_list: list objet containing dataframes that did not match.
    """
    accurate_df = gpd.read_file('{0}\\{1}'.format(asset_dir, file_end), index_col=0)

    checked_list = []
    faulty_list = []

    for test_df in df_list:
        # create two lists from the correctly formatted csv (accurate) and the data for upload (test)
        accurate_cols = accurate_df.columns.tolist()
        test_cols = test_df.columns.tolist()
        # check that the columns have the same number of headers as the accurate version.
        if len(accurate_cols) == len(test_cols):
            # check that the column headers match the accurate version.
            if accurate_cols == test_cols:
                checked_list.append(test_df)

        else:
            faulty_list.append(test_df)

    return checked_list, faulty_list


def check_dirs_fn(i, infrastructure_directory):
    file_path = os.path.join(infrastructure_directory, i)

    if os.path.isdir(file_path):
        pass
    else:
        os.mkdir(file_path)
    return file_path


def output_path_fn(prop, gdf, infrastructure_directory, pastoral_districts_path, key):
    test = gdf[gdf["PROPERTY"] == prop]
    if len(test.index) > 0:
        dist = gdf[gdf["PROPERTY"] == prop]["DISTRICT"].values[0]

        dist_ = dist.strip().title().replace(" ", "_")

        if dist_ == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist_ == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        else:
            district = dist_

        prop_tag = gdf.loc[gdf["PROPERTY"] == prop]["PROP_TAG"].values[0]
        prop_tag.strip()
        final_property = "{0}_{1}".format(prop_tag, prop.replace(" ", "_").replace("-", "_").title())

        year_path = os.path.join(pastoral_districts_path, district, final_property, "Infrastructure", "Server_Download",
                                 str(date.today().year))  # , "Raw", key)

        if not os.path.exists(year_path):
            os.mkdir(year_path)

        raw_path = "{0}\\Raw".format(year_path)
        if not os.path.exists(raw_path):
            os.mkdir(raw_path)

        key_path = "{0}\\{1}".format(raw_path, key)
        if not os.path.exists(key_path):
            os.mkdir(key_path)

    else:
        key_path = "No_data"

    return key_path


def feature_extraction_fn(gdf, server_download_path, property_name, i):
    if server_download_path != 'No_data':

        for feature_type in gdf.FEATURE.unique():
            feature_gdf = gdf.loc[gdf["FEATURE"] == feature_type]
            feature_gdf.to_file(
                "{0}\\{1}_{2}_{3}.shp".format(server_download_path, property_name.title().replace(" ", "_"),
                                              feature_type.title().replace(" ", "_"), i.title()),
                driver="ESRI Shapefile")
    else:
        pass


def all_data_export_fn(gdf, server_download_path, property_name, i):
    if server_download_path != 'No_data':
        #print('i: ', i)

        gdf.to_file(
            "{0}\\{1}_{2}.shp".format(server_download_path, property_name.title().replace(" ", "_"), i.title()),
            driver="ESRI Shapefile")
    else:
        pass


def date_reformat_fn(gdf):
    insp_date_list = []
    for date_ in gdf['DATE_INSP']:
        #print(date_)

        if '-' in date_:
            #print(date_)
            year, month, day = date_.split('-')
            new_date = '{0}-{1}-{2}'.format(year, month, day)

            if len(day) == 1:
                day_ = '0{0}'.format(day)
            else:
                day_ = day
            if len(month) == 1:
                month_ = '0{0}'.format(month)
            else:
                month_ = month

            new_date = "{0}-{1}-{2}".format(year, month_, day_)
            #print('new_date: ', new_date)

        #print('='*50)

        insp_date_list.append(new_date)
    #print(insp_date_list)
    gdf['DATE_INSP'] = insp_date_list

    return gdf


def buffer_fn(prop, pastoral_estate, export_dir):
    albers = pastoral_estate.to_crs(epsg=3577)
    tile_grid_temp_dir = '{0}\\tile_grid'.format(export_dir)
    if not os.path.exists(tile_grid_temp_dir):
        os.mkdir(tile_grid_temp_dir)

    property_ = albers.loc[albers['PROPERTY'] == prop]
    if len(property_.index) > 0:
        # apply a buffer from each property boundary.
        property_buffer = property_.buffer(500)

        gda94 = property_buffer.to_crs(epsg=4283)

        # export shapefile.
        gda94.to_file("{0}\\{1}_buffer.shp".format(tile_grid_temp_dir, prop.replace(" ", "_").title()),
                      driver='ESRI Shapefile')

        buffer = gpd.read_file("{0}\\{1}_buffer.shp".format(tile_grid_temp_dir, prop.replace(" ", "_").title()),
                               driver='ESRI Shapefile')

        # identify surrounding properties
        intersect_df = gpd.overlay(buffer, pastoral_estate, how='identity')
        # create a list of unique property names - removing the nan value for the end.
        adjacent_property_names_list = intersect_df.PROPERTY.unique().tolist()
        adjacent_property_names_list_ = adjacent_property_names_list[:-1]

    return adjacent_property_names_list_


def main_routine(pastoral_districts_path, start_date, primary_export_dir, property_enquire, infrastructure_directory,
                 pastoral_estate):
    """ Search for neighbouring properties and download infrastructure shapefiles to the server download folders.

    """
    print('step6_1')
    year = start_date[:4]

    directory_dict = {"points": "Points", "lines": "Lines", "polygons": "Other", "paddocks": "Paddocks"}

    # create subdirectories within the export directory
    dir_folders_fn(primary_export_dir, directory_dict)

    for key, value in directory_dict.items():

        for shape in glob("{0}\\*{1}.shp".format(infrastructure_directory, value)):
            shape_gdf = gpd.read_file(shape)
            # confirm that shapefiles contain data
            if len(shape_gdf.index) > 0:

                if property_enquire == 'All':
                    property_name_list = shape_gdf.PROPERTY.unique().tolist()
                    # Loop through all property names
                    for prop in property_name_list:

                        adjacent_properties_list = buffer_fn(prop, pastoral_estate, primary_export_dir)
                        # identity_df_fn(buffer_gda94, pastoral_estate_gdf)
                        for property_ in adjacent_properties_list:
                            gdf = shape_gdf.loc[shape_gdf["PROPERTY"] == property_]
                            server_download_path = output_path_fn(property_, gdf, infrastructure_directory,
                                                                  pastoral_districts_path, key)
                            gdf1 = date_reformat_fn(gdf)
                            # gdf1 = date_reformat2_fn(gdf1)

                            # feature_extraction_fn(gdf, server_download_path, property_, key)
                            all_data_export_fn(gdf1, server_download_path, property_, key)

                else:

                    adjacent_properties_list = buffer_fn(property_enquire, pastoral_estate, primary_export_dir)
                    # identity_df_fn(buffer_gda94, pastoral_estate_gdf)
                    for property_ in adjacent_properties_list:
                        gdf = shape_gdf.loc[shape_gdf["PROPERTY"] == property_]
                        server_download_path = output_path_fn(property_, gdf, infrastructure_directory,
                                                         pastoral_districts_path, key)
                        gdf1 = date_reformat_fn(gdf)
                        # gdf1 = date_reformat2_fn(gdf1)

                        print('server_download_path: ', server_download_path)

                        # feature_extraction_fn(gdf, server_download_path, property_, key)
                        all_data_export_fn(gdf1, server_download_path, property_, key)

    print('The following properties infrastructure have been downloaded: ')
    for i in adjacent_properties_list:
        print(' - ', i)


if __name__ == '__main__':
    main_routine()
