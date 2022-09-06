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
from datetime import date
import argparse
import shutil
import sys
import warnings
from glob import glob

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Download points lines and  polygon data and file if to the Pastoral Districts directory.''')

    p.add_argument('-d', '--directory', type=str, help='The pastoral infrastructure directory (corporate drive).',
                   default=r'R:\LAND\Rangeland_Mgt\Pastoral_Infrastructure\Data\ESRI')

    p.add_argument('-x', '--export_dir', type=str, help='Directory path for outputs.',
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs")

    p.add_argument("-pd", "--pastoral_districts_directory",
                   help="Enter path to the Pastoral_Districts directory in the Spatial/Working drive)",
                   default=r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\Pastoral_Districts')

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote")
    p.add_argument('-t', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\assets')

    p.add_argument('-y', '--year', type=str, help='Enter the year (i.e. 2001).',
                   default=date.today().year)

    p.add_argument('-pe', '--property_enquire', type=str,
                   help='Enter name of an individual proeprty for infrastructure data download (i.e. XXXX).',
                   default='All')

    cmd_args = p.parse_args()

    if cmd_args.directory is None:
        p.print_help()

        sys.exit()

    return cmd_args


def user_id_fn(remote_desktop):
    """ Extract the users id stripping of the adm when on the remote desktop.

    :param remote_desktop: string object containing the computer system being used.
    :return final_user: string object containing the NTG user id. """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    if remote_desktop == 'remote':
        final_user = user[3:]

    else:
        final_user = user

    return final_user


def temporary_dir(final_user):
    """ Create an temporary directory 'user_YYYMMDD_HHMM' in your working directory - this directory will be deleted
    at the completion of the script.

    :param final_user: string object containing the NTG user id.
    :param export_dir: string object path to an existing directory where an output folder will be created.  """

    # extract the users working directory path.
    home_dir = os.path.expanduser("~")

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    primary_temp_dir = home_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' \
                       + str(date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(primary_temp_dir)

    except:
        pass

    # create folder a temporary folder titled (titled 'tempFolder')
    os.makedirs(primary_temp_dir)

    return primary_temp_dir, final_user


def dir_folders_fn(primary_dir, directory_dict):
    """
    Create directory tree within the temporary directory based on shapefile type.

    :param primary_dir: string object containing the newly created temporary directory path.
    :param directory_dict: dictionary object containing the shapefile types.
    :return direc: string object containing the path to the top of the directory tree (i.e. property name).
    """

    if not os.path.exists(primary_dir):
        os.mkdir(primary_dir)
        print(primary_dir, ' created.')

    for key, value in directory_dict.items():
        shapefile_dir = ('{0}\\{1}'.format(primary_dir, key))
        print('shapefile_dir: ', shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)


def export_file_path_fn(primary_export_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument primary_export_dir.

        :param primary_export_dir: string object containing the path to the export directory (command argument).
        :param final_user: string object containing the NTG user id
        :return export_dir_path: string object containing the newly created directory path for all retained exports. """

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = primary_export_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        pass

    # create folder titled 'tempFolder'
    os.makedirs(export_dir_path)

    return export_dir_path


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


def main_routine(pastoral_estate, primary_export_dir, transition_dir, infrastructure_directory, start_date,
                 prop_enquire, pastoral_districts_path, assets_dir):
    """
    """

    # read in the command arguments
    """cmd_args = cmd_args_fn()
    direc = cmd_args.directory
    export_dir = cmd_args.export_dir
    pastoral_districts_path = cmd_args.pastoral_districts_directory
    remote_desktop = cmd_args.remote_desktop
    transition_dir = cmd_args.transition_dir
    assets_dir = cmd_args.assets_dir
    year = cmd_args.year
    property_enquire = cmd_args.property_enquire"""

    print('You have chosen to download data for the following property: ', prop_enquire)

    directory_dict = {"points": "Points", "lines": "Lines", "polygons": "Other", "paddocks": "Paddocks"}

    # create subdirectories within the export directory
    dir_folders_fn(primary_export_dir, directory_dict)

    import step6_2_search_for_data
    step6_2_search_for_data.main_routine(pastoral_districts_path, start_date, primary_export_dir, directory_dict,
                                         prop_enquire, infrastructure_directory, pastoral_estate)



if __name__ == '__main__':
    main_routine()
