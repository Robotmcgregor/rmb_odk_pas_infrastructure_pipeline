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

from __future__ import print_function, division
import pandas as pd
from glob import glob
import os
import geopandas as gpd
from datetime import datetime
import shutil

from distutils.dir_util import copy_tree
import warnings
warnings.filterwarnings("ignore")


def add_values_in_dict_fn(site_dict, key, list_of_values):
    """Append multiple values to a key in the given dictionary.

    :param site_dict: dictionary object containing the key site (property_site) and multiple values in a list.
    :param key: string object containing the site name.
    :param list_of_values: list object containing three values: district, property and site.
    :return site_dict: updated dictionary"""

    if key not in site_dict:
        site_dict[key] = list()
    site_dict[key].extend(list_of_values)

    return site_dict


def prop_code_extraction_fn(prop, pastoral_estate):
    property_list = pastoral_estate.PROPERTY.tolist()

    prop_upper = prop.upper().replace('_', ' ')

    if prop_upper in property_list:

        prop_code = pastoral_estate.loc[pastoral_estate['PROPERTY'] == prop_upper, 'PROP_TAG'].iloc[0]
    else:
        prop_code = ''

    return prop_code


def search_for_files_fn(site_dict, search_criteria, directory, pastoral_estate):
    csv_list = []

    for dirpath, subdirs, files in os.walk(directory):
        csv_list.extend(os.path.join(dirpath, x) for x in files if x.endswith(search_criteria))

    for i in csv_list:
        df = pd.read_csv(i)
        site = df.site.iloc[0]
        dist = df.district.iloc[0].replace(' ', '_')
        if dist == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        else:
            district = dist

        prop = df.final_prop.iloc[0]
        prop_name = prop.replace(" ", "_")
        site_orig = df.site_orig.iloc[0]
        date = df.date.iloc[0]
        # convert str to datetime
        date_time_obj = datetime.strptime(date, '%d/%m/%y')
        # extract year
        year = date_time_obj.year

        prop_code = prop_code_extraction_fn(prop, pastoral_estate)

        list_values = [district, prop_code, prop_name, site_orig, str(year)]

        site_dict = add_values_in_dict_fn(site_dict, site, list_values)

    return site_dict, csv_list


def check_create_folders_fn(path):
    """Check if directory exists, if not, create it
    :param path:
    """

    # Check for directory.
    # print('Path: ', path)
    check_folder = os.path.isdir(path)

    # Create directory if it does not exist.
    if not check_folder:

        # create directory
        os.mkdir(path)
        # create sub folder
        raw = os.path.join(path, 'Raw')
        os.mkdir(raw)
        # create sub folder
        draft = os.path.join(path, 'Draft')
        os.mkdir(draft)
        # create sub folder
        final = os.path.join(path, 'Final')
        os.mkdir(final)


    else:
        # create sub folder
        raw = os.path.join(path, 'Raw')
        check_raw_folder = os.path.isdir(raw)
        if not check_raw_folder:
            # create directory
            os.mkdir(raw)

        # create sub folder
        draft = os.path.join(path, 'Draft')
        check_draft_folder = os.path.isdir(draft)
        if not check_draft_folder:
            # create directory
            os.mkdir(draft)
        # create sub folder
        final = os.path.join(path, 'Final')
        check_final_folder = os.path.isdir(final)
        if not check_final_folder:
            # create directory
            os.mkdir(final)

    return raw


def join_ras_site_photo_file_path_2_fn(pathway, output_drive, values_, search_criteria, site_type):
    files = glob.glob(pathway + search_criteria)

    for f in files:
        photo_file_path = os.path.join(output_drive, values_[0],
                                       str(values_[1]) + '_' + str(values_[2]),
                                       'Photos', str(site_type), str(values_[4]))

        # Check for directory.
        check_folder = os.path.isdir(photo_file_path)

        # Create directory if it does not exist.
        if not check_folder:
            # create directory
            os.mkdir(photo_file_path)

        shutil.copy(f, photo_file_path)


def join_transect_site_html_file_path_fn(pathway, output_drive, values_, search_criteria, site_type, file_type):
    files = glob.glob(pathway + search_criteria)

    if files:
        for f in files:
            processed_odk_path = os.path.join(output_drive, values_[0],
                                              str(values_[1]) + '_' + str(values_[2]),
                                              'Data', 'Processed_Odk')  # str(site_type), str(values_[4]))

            # Check for directory.
            check_folder = os.path.isdir(processed_odk_path)

            # Create directory if it does not exist.
            if not check_folder:
                # create directory
                os.mkdir(processed_odk_path)

            # create sub folder
            measured = os.path.join(processed_odk_path, str(site_type))
            check_measured_folder = os.path.isdir(measured)
            if not check_measured_folder:
                # create directory
                os.mkdir(measured)

            year = os.path.join(measured, str(values_[4]))
            check_year_folder = os.path.isdir(year)
            if not check_year_folder:
                # create directory
                os.mkdir(year)

            # create sub folder
            transect = os.path.join(year, str(file_type))
            check_transect_folder = os.path.isdir(transect)
            if not check_transect_folder:
                # create directory
                os.mkdir(transect)

            shutil.copy(f, transect)
    else:
        pass


def join_xlsx_site_file_path_fn(pathway, output_drive, values_, search_criteria, site_type):
    """

    :param pathway:
    :param output_drive:
    :param values_:
    :param search_criteria:
    :param site_type:
    """
    files = glob.glob(pathway + search_criteria)

    if files:
        for f in files:
            processed_odk_path = os.path.join(output_drive, values_[0],
                                              str(values_[1]) + '_' + str(values_[2]),
                                              'Data', str(site_type), str(values_[4]))

            # Check for directory.
            check_folder = os.path.isdir(processed_odk_path)

            # Create directory if it does not exist.
            if not check_folder:
                # create directory
                os.mkdir(processed_odk_path)

            # create sub folder
            raw = os.path.join(processed_odk_path, 'Raw')
            check_measured_folder = os.path.isdir(raw)
            if not check_measured_folder:
                # create directory
                os.mkdir(raw)
                # print('f: ', f)
                # print('raw: ', raw)
            shutil.copy(f, raw)
    else:
        pass


def join_csv_file_path_fn(search_criteria, output_drive, directory, geo_series, n, location):
    """

    :param search_criteria:
    :param output_drive:
    :param directory:
    :param geo_series:
    :param n:
    :param location:
    """
    site_dict = {}

    site_dict, csv_list = search_for_files_fn(site_dict, search_criteria, directory, geo_series)

    for i in csv_list:

        # split filename from path and split by "_"
        pathway, file = i.rsplit('\\', 1)
        prop, file_name = file.split('_' + search_criteria)

        values_ = site_dict[prop]

        ras_file_path = os.path.join(output_drive, values_[0],
                                     str(values_[1]) + '_' + str(values_[2]),
                                     str(location), str(n), str(values_[4]))

        raw = check_create_folders_fn(ras_file_path)

        if values_[3].endswith('A'):
            site_type_xlsx = 'Observation_Sheets'
            site_type_other = 'Measured'
            join_xlsx_site_file_path_fn(pathway, output_drive, values_, '\\*xlsx', site_type_xlsx)
            join_transect_site_html_file_path_fn(pathway, output_drive, values_, '\\*.csv', site_type_other, 'Csv')
            join_transect_site_html_file_path_fn(pathway, output_drive, values_, '\\*.html', site_type_other,
                                                 'Transect')
            join_ras_site_photo_file_path_2_fn(pathway, output_drive, values_, '\\*.jpg', site_type_other)

        elif values_[3].startswith('RAS'):
            site_type_xlsx = 'Ras'
            site_type_other = 'Ras'
            join_xlsx_site_file_path_fn(pathway, output_drive, values_, '\\*xlsx', site_type_xlsx)
            join_ras_site_photo_file_path_2_fn(pathway, output_drive, values_, '\\*.jpg', site_type_other)
        else:
            site_type_xlsx = 'Ras'
            site_type_other = 'Tier1'
            join_xlsx_site_file_path_fn(pathway, output_drive, values_, '\\*xlsx',
                                        site_type_xlsx)  # todo check that tier 1 should not be filed separately
            join_ras_site_photo_file_path_2_fn(pathway, output_drive, values_, '\\*.jpg', site_type_other)


def join_processed_csv_file_path_fn(pathway, output_drive, values_, search_criteria, site_type):
    """

    :param pathway:
    :param output_drive:
    :param values_:
    :param search_criteria:
    :param site_type:
    """
    files = glob.glob(pathway + search_criteria)

    for f in files:
        processed_odk_path = os.path.join(output_drive, values_[0],
                                          str(values_[1]) + '_' + str(values_[2]),
                                          'Data', 'Processed_Odk',
                                          'Measured')  # str(site_type), str(values_[4]))

        # Check for directory.
        check_folder = os.path.isdir(processed_odk_path)

        # Create directory if it does not exist.
        if not check_folder:

            # create directory
            os.mkdir(processed_odk_path)
            # create sub folder
            year = os.path.join(processed_odk_path, str(values_[4]), 'Csv')
            os.mkdir(year)

        else:

            # create sub folder
            year = os.path.join(processed_odk_path, str(values_[4]), 'Csv')
            check_raw_folder = os.path.isdir(year)
            if not check_raw_folder:
                # create directory
                os.mkdir(year)

        shutil.copy(f, year)


def csv_file_workflow_fn(csv_list):
    for directory in csv_list:

        for file in glob(directory + '\\*GDA94.csv'):

            df = pd.read_csv(file)
            for prop in df.property.unique():

                prop_df = df.loc[df['property'] == prop]


def check_folder_exists_fn(destination, infra_feature_list):
    """ Check for and create when necessary year and raw sub-directories within the Infrastructure directory.

    :param destination: string object containing the path to destination directory including year.
    :return raw path: string object containing the path to the final infrastructure destination folder.
    """
    # Check for directory.
    check_folder = os.path.isdir(destination)

    # Create directory if it does not exist.
    if not check_folder:
        # create directory
        os.mkdir(destination)
        # create sub folder

    raw_path = os.path.join(destination, 'Raw')
    check_folder = os.path.isdir(raw_path)
    # Create directory if it does not exist.
    if not check_folder:
        # create directory
        os.mkdir(raw_path)

    for i in infra_feature_list:

        infra_path = os.path.join(destination, 'Raw', i)
        check_folder = os.path.isdir(infra_path)
        # Create directory if it does not exist.
        if not check_folder:
            # create directory
            os.mkdir(infra_path)

    return raw_path


def check_folder_poi_exists_fn(destination, feature_list, year):
    """ Check for and create when necessary year and raw sub-directories within the Infrastructure directory.

    :param year: string object containing the year derived from the start_date search
    :param feature_list:
    :param destination: string object containing the path to destination directory including year.
    :return raw path: string object containing the path to the final infrastructure destination folder.
    """
    # Check for directory.
    check_folder = os.path.isdir(destination)
    if not check_folder:
        # create directory
        os.mkdir(destination)

    year_path = os.path.join(destination, year)
    check_folder = os.path.isdir(year_path)
    # Create directory if it does not exist.
    if not check_folder:
        # create directory
        os.mkdir(year_path)
        # create sub folder

    raw_path = os.path.join(year_path, 'Raw')
    check_folder = os.path.isdir(raw_path)
    # Create directory if it does not exist.
    if not check_folder:
        # create directory
        os.mkdir(raw_path)

    for i in feature_list:

        path = os.path.join(raw_path, i)
        check_folder = os.path.isdir(path)
        # Create directory if it does not exist.
        if not check_folder:
            # create directory
            os.mkdir(path)

    return year_path


def copy_subdirectories_fn(original_path, raw_path, feature_list):
    for i in feature_list:
        directory = os.path.join(original_path, i)
        #print('-'*50)
        #print('directory: ', directory)
        dest_dir = os.path.join(raw_path, i)
        #print('dest_dir: ', dest_dir)
        #print('-' * 50)
        copy_tree(directory, dest_dir)


def copy_infra_photos_to_photos_fn(original_path, feature_list, destination_infra_photos):

    #print('!'*50)
    #print('destination_infra_photos; ', destination_infra_photos)

    for i in feature_list:
        directory = os.path.join(original_path, i, 'photos')
        #print('-' * 50)
        #print('directory: ', directory)
        #print('destination_infr_photos: ', destination_infra_photos)
        dest_dir = os.path.join(destination_infra_photos, i)

        for photo in glob("{0}\\*.jpg".format(directory)):
            #print('photo: ', photo)
            path, file = photo.rsplit('\\', 1)

            dest_dir = os.path.join(destination_infra_photos, i)
            #print('dest_dir; ', dest_dir)

            check_raw_folder = os.path.isdir(destination_infra_photos)
            if not check_raw_folder:
                # create directory
                os.mkdir(destination_infra_photos)

            check_raw_folder = os.path.isdir(dest_dir)
            if not check_raw_folder:
                # create directory
                os.mkdir(dest_dir)

            dest_output = os.path.join(destination_infra_photos, i, file)
            #print('dest_output: ', dest_output)
            shutil.copy(photo, dest_output)


def copy_poi_photos_to_photos_fn(original_path, feature_list, destination_poi_photos):
    for i in feature_list:
        directory = os.path.join(original_path, i, 'photos')
        #print('-' * 50)
        #print('directory: ', directory)
        #print('destination_poi_photos: ', destination_poi_photos)
        dest_dir = os.path.join(destination_poi_photos, i)

        for photo in glob("{0}\\*.jpg".format(directory)):
            #print('photo: ', photo)
            path, file = photo.rsplit('\\', 1)


            photo_type = file[4:12]
            #print('photo_type: ', photo_type)
            if photo_type != 'location':

                dest_dir = os.path.join(destination_poi_photos, i)

                check_raw_folder = os.path.isdir(destination_poi_photos)
                if not check_raw_folder:
                    # create directory
                    os.mkdir(destination_poi_photos)

                check_raw_folder = os.path.isdir(dest_dir)
                if not check_raw_folder:
                    # create directory
                    os.mkdir(dest_dir)

                dest_output = os.path.join(destination_poi_photos, i, file)
                #print('dest_output: ', dest_output)
                shutil.copy(photo, dest_output)


def remove_empty_dir(path):
    try:
        os.rmdir(path)
    except OSError:
        pass


def remove_empty_dirs(path):
    for root, dirnames, filenames in os.walk(path, topdown=False):
        for dirname in dirnames:
            remove_empty_dir(os.path.realpath(os.path.join(root, dirname)))


def main_routine(output_dir, path, start_date):

    # create two lists of feature names
    infra_feature_list = ['infra_lines', 'infra_points', 'infra_water_points']
    poi_list = ['clearing', 'erosion', 'feral_animals', 'fire', 'paddock', 'sinkhole', 'unidentified', 'weeds',
                'woody_thickening']

    # extract the year from the start date filter
    year = start_date[0:4]

    # create a list of the first level sub-folders
    #subfolder_list = next(os.walk(output_dir))[1]
    #print(subfolder_list)

    subfolder_list = os.listdir(output_dir)
    #print(subfolder_list)

    # extract directory pathways for all subdirectories within the Pastoral Districts directory.
    property_paths_list = []
    list_subfolders_with_paths = [f.path for f in os.scandir(path) if f.is_dir()]

    for i in list_subfolders_with_paths:
        #print('i: ', i)
        property_paths = [f.path for f in os.scandir(i) if f.is_dir()]

        property_paths_list.extend(property_paths)

    for prop_dir_ in subfolder_list:
        print('prop_dir_: ', prop_dir_)
        if prop_dir_ != 'Unknown':
            if prop_dir_ == "Mckinlay_River":
                prop_dir = "McKinlay_River"

                '''elif prop_dir_ == "Labelle_Downs":
                prop_dir = "La_Belle_Downs"'''
            else:
                prop_dir = prop_dir_
            print('property_paths_list: ', property_paths_list)
            print('prop_dir final: ', prop_dir)
            destination_dir = next((s for s in property_paths_list if prop_dir in s), None)

            # ----------------------------------------- infrastructure -------------------------------------------------

            #print('destination_dir: ', destination_dir)
            destination_year = os.path.join(destination_dir, 'Infrastructure', 'Field_Data', year)
            #print('destination_year: ', destination_year)

            infra_destination = os.path.join(destination_dir, 'Infrastructure', 'Field_Data')
            # call the check_folder_exists_fn function to check and create the correct year and raw sub-folders.
            raw_path = check_folder_exists_fn(destination_year, infra_feature_list)
            # join output dir with prop_path
            original_path = os.path.join(output_dir, prop_dir.title())# due to upper fields
            print('copy the original_path: ', original_path)
            copy_subdirectories_fn(original_path, raw_path, infra_feature_list)
            remove_empty_dirs(destination_year)

            #print('INFRASTRUCTURE ' * 50)
            destination_infra_photos = os.path.join(destination_dir, 'Photos', 'Infrastructure')
            #print('destination_infra_photos: ', destination_infra_photos)

            copy_infra_photos_to_photos_fn(original_path, infra_feature_list, destination_infra_photos)

            # --------------------------------------------- poi --------------------------------------------------------

            destination_poi = os.path.join(destination_dir, 'General', 'Poi')
            # call the check_folder_exists_fn function to check and create the correct year and raw sub-folders.
            year_path = check_folder_poi_exists_fn(destination_poi, poi_list, year)
            # join output dir with prop_path
            original_path = os.path.join(output_dir, prop_dir.title())
            #print('original_path: ', original_path)
            copy_subdirectories_fn(original_path, year_path + '\\Raw', poi_list)
            remove_empty_dirs(year_path)
            #print('POI '*50)

            destination_poi_photos = os.path.join(destination_dir, 'Photos', 'Poi')
            copy_poi_photos_to_photos_fn(original_path, poi_list, destination_poi_photos)



if __name__ == "__main__":
    main_routine()
