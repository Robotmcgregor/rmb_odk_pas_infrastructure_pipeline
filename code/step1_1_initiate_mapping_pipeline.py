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
import pandas as pd
import geopandas as gpd

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description="""Process raw RMB Mapping result csv -> csv, shapefiles.""")

    p.add_argument("-d", "--directory_odk", type=str, help="The directory containing ODK csv files.",
                   default=r"E:\DEPWS\code\rangeland_monitoring\rmb_mapping_pipeline\raw_odk")

    p.add_argument("-x", "--export_dir", type=str, help="Directory path for outputs.",
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs")

    p.add_argument("-pd", "--pastoral_districts_directory",
                   help="Enter path to the Pastoral_Districts directory in the Spatial\Working drive)",
                   default=r'U:\Pastoral_Districts')  # r"U:\Pastoral_Districts"

    p.add_argument("-ver", "--version", type=str, help="Enter odk form version (i.e. v1 or v2.",
                   default="v1")

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote_auto")

    p.add_argument("-t", "--time_sleep", type=int,
                   help="Time between odk aggregate actions -if lagging increase integer",
                   default=10)

    p.add_argument("-c", "--chrome_driver", help="File path for the chrome extension driver.",
                   default=r"E:\DEPWS\code\rangeland_monitoring\rmb_mapping_pipeline\assets\chrome_driver\chrome_driver_v89_0_4389_23\chromedriver.exe")

    p.add_argument("-s", "--start_date", type=str, help="Filter results by start date.",
                   default="{0}-01-01".format(date.today().year))

    p.add_argument("-e", "--end_date", type=str, help="Filter results by end date.",
                   default=datetime.today().strftime("%Y-%m-%d"))

    p.add_argument("-w", "--weeds_list", help="Path to the weeds_list.csv.",
                   default=r"E:\DEPWS\code\rangeland_monitoring\rmb_mapping_pipeline\assets\weeds_list.csv")

    p.add_argument("-pe", "--property_enquire", help="Single property name to process - otherwise all properties will be processed (i.e. 'ROCKHAMPTON DOWNS').",
                   default="All")

    p.add_argument('-i', '--infrastructure_directory', type=str, help='The pastoral infrastructure directory (corporate drive).',
                   #default=r'R:\LAND\Rangeland_Mgt\Pastoral_Infrastructure\Data\ESRI')
                   default=r'U:\Pastoral_Infrastructure\Lib_Corporate\Data\ESRI')

    p.add_argument('-td', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'E:\DEPWS\code\rangeland_monitoring\rmb_mapping_pipeline\assets\shapefiles\templates')


    cmd_args = p.parse_args()

    if cmd_args.directory_odk is None:
        p.print_help()

        sys.exit()

    return cmd_args


def user_id_fn(remote_desktop):
    """ Extract the users id stripping of the adm when on the remote desktop.

    :param remote_desktop: string object containing the computer system being used.
    :return final_user: string object containing the NTG user id. """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit("\\", 1)
    if remote_desktop == "remote" or "remote_auto":
        final_user = user[3:]

    else:
        final_user = user

    return final_user


def temporary_dir(final_user):
    """ Create an temporary directory (user_YYYMMDD_HHMM) in your working directory - this directory will be deleted
    at the completion of the script.

    :param final_user: string object containing the NTG user id.
    :param export_dir: string object path to an existing directory where an output folder will be created.
    :return primary_temp_dir: string object containing the location to the newly created temporary directory.
    :return final_user: string object containing the user name of the person running the script.
    """

    # extract the users working directory path.
    home_dir = os.path.expanduser("~")

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace("-", "")
    date_time_list = date_time_replace.split(" ")
    date_time_list_split = date_time_list[1].split(":")
    primary_temp_dir = "{0}\\{1}_{2}_{3}{4}".format(home_dir, str(final_user), str(date_time_list[0]),
                                                    str(date_time_list_split[0]), str(date_time_list_split[1]))
    """primary_temp_dir = home_dir + "\\" + str(final_user) + "_" + str(date_time_list[0]) + "_" \
                       + str(date_time_list_split[0]) + str(date_time_list_split[1])"""

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(primary_temp_dir)

    except:
        pass

    # create folder a temporary folder titled user_datetime
    os.makedirs(primary_temp_dir)

    return primary_temp_dir, final_user


def export_file_path_fn(primary_export_dir, final_user):
    """ Create an export directory (user_YYYMMDD_HHMM) at the location specified in command argument primary_export_dir.

    :param primary_export_dir: string object containing the path to the export directory (command argument).
    :param final_user: string object containing the NTG user id
    :return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace("-", "")
    date_time_list = date_time_replace.split(" ")
    date_time_list_split = date_time_list[1].split(":")
    export_dir_path = "{0}\\{1}_{2}_{3}{4}".format(primary_export_dir, str(final_user), str(date_time_list[0]),
                                                   str(date_time_list_split[0]), str(date_time_list_split[1]))

    """export_dir_path = primary_export_dir + "\\" + str(final_user) + "_" + str(date_time_list[0]) + "_" + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])"""

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        pass
        # print("The following directory did not exist: ", export_dir_path)

    # create folder titled "tempFolder"
    os.makedirs(export_dir_path)

    return export_dir_path


def odk_export_csv_checker_fn(dir_path, search_criteria, primary_temp_dir, pastoral_estate, feature_list,
                              primary_export_dir, start_date, end_date, pastoral_districts_path, weeds_bot_com,
                              property_enquire, user_df, transition_dir, infrastructure_directory, assets_dir,
                              remote_desktop):
    """ Search for the ODK Mapping Results csv, if located this function call the step2_1_mapping_processing_workflow
    script. If none is located (i.e. was not located or was purged (0 observations).

    :param property_enquire:
    :param weeds_bot_com: string object (command argument) containing the path to the weeds_list.csv file
    - file contains all known botanical and common weeds names.
    :param pastoral_districts_path: string object (command argument) containing the path to the Pastoral Districts
    directory.
    :param start_date: string object (command argument) containing the start date that the user wishes to filter the
    ODK Aggregate Results data to be filtered from.
    :param end_date: string object (command argument) containing the end date that the user wishes to filter the
    ODK Aggregate Results data to be filtered to.
    :param feature_list: list object containing the ODK Mapping feature classes (string objects).
    :param primary_export_dir: derived from export_dir: string object(command argument) containing the path to the
    desired export location for the transitional directory.
    :param dir_path: string object containing the raw odk output csv files.
    :param search_criteria: string object containing the raw odk file name and type.
    :param primary_temp_dir: string object path to the created output directory (date_time).
    :param pastoral_estate: string object containing the file path to the pastoral estate shapefile.
    """

    file_path = ("{0}\\{1}".format(dir_path, search_criteria))
    print("=" * 50)
    print("Searching for: ", search_criteria)

    if not os.path.exists(file_path):
        print("file_path: ", file_path)
        print(search_criteria, " not located.......")
        pass

    else:
        print(search_criteria, " located!!")
        # call the import_script_fn function.
        import step2_1_mapping_processing_workflow
        property_processed_list = step2_1_mapping_processing_workflow.main_routine(file_path, primary_temp_dir, pastoral_estate, feature_list,
                                                         primary_export_dir, start_date, end_date,
                                                         pastoral_districts_path, weeds_bot_com, property_enquire,
                                                         user_df, transition_dir, infrastructure_directory, assets_dir,
                                                         remote_desktop)

    return property_processed_list


def assets_search_fn(search_criteria, folder):
    """ Searches through a specified directory "folder" for a specified search item "search_criteria".

    :param search_criteria: string object containing a search variable including glob wildcards.
    :param folder: string object containing the path to a directory.
    :return files: string object containing the path to any located files or "" if none were located.
    """
    path_parent = os.path.dirname(os.getcwd())
    assets_dir = ("{0}\\{1}".format(path_parent, folder))

    files = ""
    file_path = ("{0}\\{1}".format(assets_dir, search_criteria))
    for files in glob(file_path):
        # print(search_criteria, "located.")
        pass

    return files


def main_routine():
    """ This pipeline either downloads the latest ODK Mapping Results csv or searches through a directory defined by
    command argument "remote_desktop". Following the discovery of the ODK Mapping Results csv, the script filters the
    data based on "start_date" and "end_date" command arguments and processes the data to an intermediary file
    defined by the "export_dir" command argument. Once all outputs have been created and filed into separate property
    sub-directories (shapefiles, csv files, downloaded photos, identification request forms), all contents are
    transferred to the relevant property folder in the Pastoral Districts directory in the working drive.

    IMPORTANT - if a point is in another properties Pastoral Estate boundary it will be filed there.

    :param directory_odk: string object (command argument) containing the path to the downloaded ODK Mapping Results
    csv.
    :param export_dir: string object(command argument) containing the path to the desired export location for
    the transitional directory.
    :param remote_desktop: string object (command argument) containing the variable:
    "remote_auto", "remote". Each has its own workflow, remote_auto will download the Results csv from ODK Aggregate
    and process the data, whereas remote or local requires the user to manually download the ODK Mapping Results csv
    and insert it into the "directory_odk" directory before processing.
    :param pastoral_districts_directory: string object (command argument) containing the path to the directory holding
    the Pastoral Estate shapefile.
    :param version: string object (command argument) containing the ODK form version (i.e "v1").
    :param chrome_driver: string object (command argument) containing the path to the chrome driver extension - used
    when "remote_desktop" is set to "remote_auto".
    :param time_sleep: integer object (command argument) defining the time chrome driver sleeps between clicking on
    active elements - used when "remote_desktop" is set to "remote_auto".
    :param start_date: string object (command argument) containing the start date that the user wishes to filter the
    ODK Aggregate Results data to be filtered from.
    :param end_date: string object (command argument) containing the end date that the user wishes to filter the
    ODK Aggregate Results data to be filtered to.
    """


    # read in the command arguments
    cmd_args = cmd_args_fn()
    directory_odk = cmd_args.directory_odk
    primary_export_dir = cmd_args.export_dir
    pastoral_districts_path = cmd_args.pastoral_districts_directory
    version = cmd_args.version
    remote_desktop = cmd_args.remote_desktop
    chrome_driver_path = cmd_args.chrome_driver
    time_sleep = cmd_args.time_sleep
    start_date = cmd_args.start_date
    end_date = cmd_args.end_date
    weeds_bot_com = cmd_args.weeds_list
    property_enquire = cmd_args.property_enquire
    infrastructure_directory = cmd_args.infrastructure_directory
    transition_dir = cmd_args.transition_dir
    assets_dir = cmd_args.assets_dir

    print('The following data filters have been applied:')
    print(' - Start date:', start_date)
    print(' - End date: ', end_date)
    print(' - Property name: ', property_enquire)


    pastoral_estate_ = assets_search_fn("NT_Pastoral_Estate.shp", "{0}\\{1}".format("assets", "shapefiles"))
    pastoral_estate = gpd.read_file(pastoral_estate_)
    user_df = assets_search_fn("contact_details.csv", "{0}".format("assets"))
    pd.read_csv(user_df)
    #pd.read_csv(r'E:\DENR\code\rangeland_monitoring\rmb_mapping_pipeline\assets\contact_details.csv')

    inspection_details = assets_search_fn("Inspection_Details.csv", "{0}\\{1}".format("assets", "csv"))

    # list of the current odk files to be processed
    odk_form_list = ["RMB_Mapping_{0}".format(version)]

    # list of the ODK Mapping feature classes.
    feature_list = ["infra_lines", "infra_points", "infra_water_points", "clearing", "paddock", "erosion", "weeds",
                    "woody_thickening",
                    "feral_animals", "fire", "sinkhole", "unidentified", "other_feature"]

    # defining the workflow based on the "remote_desktop" variable.
    if remote_desktop == "remote_auto":
        if remote_desktop == "remote_auto":
            # extract user home directory: thereby, extracting the users NTG id.
            home_dir = os.path.expanduser("~")
            _, user = home_dir.rsplit("\\", 1)

            # remove all old result files from the Downloads folder.
            download_folder_path = ("C:\\Users\\{0}\\Downloads".format(user))
            files = glob("{0}\\*results*.csv".format(download_folder_path))
            print("The following files were located in your download folder: ", files)
            # remove existing results files
            for f in files:
                os.remove(f)
                print("And have now been removed.")

        print("You have selected Remote Auto, python now has control of your computer, step back and enjoy the ride.")

        # call the step1_2_aggregate_collect_raw_data_remote_desktop script to download the latest data from
        # ODK Aggregate.
        import step1_2_aggregate_collect_raw_data_remote_desktop
        step1_2_aggregate_collect_raw_data_remote_desktop.main_routine(chrome_driver_path, odk_form_list, time_sleep)

        # purge result csv with 0 observations from the default directory_odk directory
        path_parent = os.path.dirname(os.getcwd())
        raw_odk_dir = ("{0}\\raw_odk".format(path_parent))

        files = glob("{0}\\RMB_Mapping*.csv".format(raw_odk_dir))
        for f in files:

            df = pd.read_csv(f)
            total_rows = len(df.index)

            if total_rows < 1:
                os.remove(f)
                print(files, "have been removed from ", raw_odk_dir)
                print("They had insufficient observations (0).")

        # copy result csv with 0 observations from the users download directory to the directory_odk directory
        files = glob("{0}\\*results.csv".format(download_folder_path))

        # remove existing results files
        odk_results_list = []
        for f_ in files:
            df = pd.read_csv(f_)
            total_rows = len(df.index)
            if total_rows > 1:
                _, file_ = f_.rsplit("\\", 1)
                file_output = raw_odk_dir + "\\" + file_
                shutil.move(f_, file_output)
                odk_results_list.append(file_output)
                print(file_, "have been moved to ", raw_odk_dir)

        directory_odk = raw_odk_dir

    elif remote_desktop == "remote":
        print("You have selected Remote, I hope you put the results csv in the correct folder.......")

    else:
        print("You are processing this outside of the remote server, you may need to adjust your computer settings for "
              "optimisation, you will also need to double check the shapefile outputs have the correct spatial "
              "information.")

    # call the user_id_fn function to extract the user id
    final_user = user_id_fn(remote_desktop)

    # call the temporary_dir function to create a temporary folder which will be deleted at the end of the script.
    primary_temp_dir, final_user = temporary_dir(final_user)

    # call the export_file_path_fn function to create an export directory.
    primary_export_dir = export_file_path_fn(primary_export_dir, final_user)

    # call the odk_export_csv_checker_fn function - search for star transect outputs
    property_processed_list = odk_export_csv_checker_fn(directory_odk, "RMB_Mapping_" + version + "_results.csv",
                              primary_temp_dir, pastoral_estate, feature_list, primary_export_dir, start_date, end_date,
                              pastoral_districts_path, weeds_bot_com, property_enquire, user_df,
                              transition_dir, infrastructure_directory, assets_dir, remote_desktop)

    print(primary_temp_dir, " has been deleted from your hard drive.")
    # delete the temp directory and its contents.
    shutil.rmtree(primary_temp_dir)

    inspection_details_df = pd.read_csv(inspection_details)
    #final_prop_list = []
    #prop_name_list = inspection_details_df.Property.tolist()
    '''for i in prop_name_list:
        prop_name = i.replace(" ", "_").upper()
        final_prop_list.append(prop_name)'''

    utm_dict = dict(zip(inspection_details_df.Property, inspection_details_df.UTM_Zone))

    district_dict = dict(zip(pastoral_estate.PROPERTY, pastoral_estate.DISTRICT))
    tag_dict = dict(zip(pastoral_estate.PROPERTY, pastoral_estate.PROP_TAG))

    date_ = datetime.now()
    year = date_.year


    for prop_name_ in property_processed_list:
        if prop_name_ != "UNKNOWN":

            # print(prop_name_)
            #prop_name__ = prop_name_.replace(" ", "_")
            utm_dict_code = utm_dict[prop_name_]
            # print('utm_dict_code: ', utm_dict_code)
            district = district_dict[prop_name_]
            tag = tag_dict[prop_name_]
            final_property = "{0}_{1}".format(tag.upper(), prop_name_.replace(" ", "_").title())
            property_dir_path_list = []
            output_list = ['csv', 'shapefile']
            for feature in feature_list:
                for file_type in output_list:

                    dir_path = os.path.join(pastoral_districts_path, district.title(), final_property, 'Infrastructure', 'Field_Data',
                                 str(year), 'Raw', feature, file_type)
                    property_dir_path_list.append(dir_path)


            for n in property_dir_path_list:
                #print('n : ', n)
                for delete_file in glob("{0}\\*".format(n, )):
                    #print('delete_file: ', delete_file)
                    path, file = delete_file.rsplit('\\', 1)
                    if str(utm_dict_code) not in file:
                        if 'photo' not in file:
                            print(' -- delete: ', file)
                            os.remove(delete_file)

    # search for faulty directory and delete it
    dir_list = next(os.walk(pastoral_districts_path))[1]
    # walk to the district directory
    for i in dir_list:
        dist_path = os.path.join(pastoral_districts_path, i)
        prop_list = next(os.walk(dist_path))[1]
        for n in prop_list:
            if "ALL_" in n:
                # print("n: ", n)
                path_ = os.path.join(dist_path, n)
                # print("A directory with all has been located")
                shutil.rmtree(path_)
            elif "All_" in n:
                # print("n: ", n)
                path_ = os.path.join(dist_path, n)
                # print("A directory with all has been located")
                shutil.rmtree(path_)
            else:
                pass



    print(primary_export_dir, " has been deleted from your hard drive - your final outputs have been filed.")
    # delete the export directory and its contents.
    shutil.rmtree(primary_export_dir)


if __name__ == "__main__":
    main_routine()
