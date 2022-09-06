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
import warnings
import geopandas as gpd
import pandas as pd
import os
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")


def string_clean_upper_fn(dirty_string):
    """
    Remove whitespaces and clean strings.

    :param dirty_string: string object.
    :return clean_string: processed string object.
    """

    str1 = dirty_string.replace("_", " ")
    str2 = str1.replace("-", " ")
    str3 = str2.upper()
    clean_string = str3.strip()
    return clean_string


def string_clean_capital_fn(dirty_string):
    """
    Remove whitespaces and clean strings.

    :param dirty_string: string object.
    :return clean_string: processed string object.
    """

    str1 = dirty_string.replace("_", " ")
    str2 = str1.replace("-", " ")
    str3 = str2.capitalize()
    clean_string = str3.strip()
    return clean_string


def string_clean_title_fn(dirty_string):
    """
    Remove whitespaces and clean strings.

    :param dirty_string: string object.
    :return clean_string: processed string object.
    """

    str1 = dirty_string.replace("_", " ")
    str2 = str1.replace("-", " ")
    str3 = str2.title()
    clean_string = str3.strip()
    return clean_string


def date_time_fn(row):
    """
    Extract and reformat date and time fields.

    :param row: pandas dataframe row value object
    :return date_time_list: list object containing two string variables: s_date2, obs_date_time.
    """

    s_date, s_time = row["START"].split("T")
    # date clean
    #start_date = s_date[-2:] + "/" + s_date[-5:-3] + "/" + s_date[0:4]
    start_date = s_date[0:4] + "-" + s_date[-5:-3] + "-" + s_date[-2:]


    return start_date


def meta_data_fn(row):
    """
    Extract and clean the form key information.

    :param row: pandas dataframe row value object.
    :return meta_key: string object containing the odk form identifier key.
    :return clean_meta_key: string object containing the cleaned odk form identifier key.
    :return form_name: string object containing the odk form identifier key.
    """

    meta_key = str(row["meta:instanceID"])
    clean_meta_key = meta_key[5:]
    form_name = str(row["meta:instanceName"])

    meta_data_list = [meta_key, clean_meta_key, form_name]
    return meta_data_list


def gps_points_fn(row):
    """
    Extract the coordinate information.

    :param row: pandas dataframe row value object.
    :return datum: string object containing datum.
    :return c_lat: float object containing the center point latitude information.
    :return c_lon: float object containing the center point longitude information.
    :return c_acc: float object containing the center point accuracy information (mobile device only).
    :return o_lat: float object containing the offset point latitude information.
    :return o_lon: float object containing the offset point longitude information.
    :return o_acc: float object containing the center point accuracy information.
    """

    inside = str(row["FEATURE_ATTRIB:OUTSIDE"])
    datum = "wgs84"
    lat = float(row["GROUP_COORDINATES:SITE_GPS1:Latitude"])
    lon = float(row["GROUP_COORDINATES:SITE_GPS1:Longitude"])
    acc = float(row["GROUP_COORDINATES:SITE_GPS1:Accuracy"])
    dist = float(row["GROUP_COORDINATES:OFF1_DIST"])
    bear = float(row["GROUP_COORDINATES:BEARING1"])

    lat_lon_list = [inside, datum, lat, lon, acc, dist, bear]
    return lat_lon_list


def photo_url_extraction_fn(row):
    """
    Extract the three photograph urls.

    :param row: pandas dataframe row value object.
    :return photo_url_list: list object containing the site names and urls of all site photographs stores in ODK
    Aggregate.
    """

    photo1 = str(row["GROUP_PHOTO:PHOTO1"])
    photo2 = str(row["GROUP_PHOTO:PHOTO2"])
    photo3 = str(row["GROUP_PHOTO:PHOTO3"])

    photo_url_list = [photo1, photo2, photo3]

    return photo_url_list


def remove_duplicate_substrings_fn(l):
    ulist = []
    [ulist.append(x) for x in l if x not in ulist]
    return ulist


def label_comment_fn(row, string_clean_capital_fn):
    """
    Extract the three variables feat_label, cond_label and comment.

    :param row: pandas row index
    :param string_clean_capital_fn: function that cleans string objects.
    :return label_comment_list: list object containing the three variables feat_label, cond_label and comment
    """

    feat_label = string_clean_capital_fn(str(row["FEATURE_ATTRIB:INF_LABEL"]))
    cond_label = string_clean_capital_fn(str(row["FEATURE_ATTRIB:COND_LABEL"]))
    comment = string_clean_capital_fn(str(row["FEATURE_ATTRIB:FREE_TEXT"]))

    water_dict = {"Water tank": "Water Tank", "Bore": "Bore", "Dam": "Dam", "Trough": "Trough",
                  "Pump out point": "Pump Out Pont",
                  "Turkey nest": " T/Nest", "Waterhole": "Waterhole"}
    infra_feature = (str(row["INFRA:INF_FEAT"]))

    final_label = feat_label
    if infra_feature == 'water_point':

        water_feature = string_clean_capital_fn(str(row["INFRA:WAT_OBJ"]))

        clean_water_feature = (water_dict[water_feature])

        if feat_label != "Not recorded":
            # print("{0} {1}".format(water_feature, feat_label))
            final_label_ = "{0} {1}".format(feat_label.title(), clean_water_feature.title())
            # remove repeated string variables and create a final string.
            final_label = ' '.join(remove_duplicate_substrings_fn(final_label_.split()))

            # create a status abbreviation for the label feature
            if cond_label != "Not recorded":
                if cond_label == "Abandoned":
                    cond_abrev = "Abd"
                elif cond_label == "Disused":
                    cond_abrev = "Dis"
                else:
                    pass

                final_label = "{0} {1} ({2}.)".format(feat_label, water_feature.title(), cond_abrev)
            else:
                final_label = "Not Recorded"
        else:
            final_label = "Not Recorded" #""


    elif infra_feature == 'point':

        points_dict = {"aerial": "Aerial", "Stock yard": "Yard", "Underground mine": "Mine", "Quarry": "Quarry",
                       "Landing ground": "Landing Ground", "General building": "Building", "Homestead": "Homestead",
                       "Gate": "Gate"}

        point_infra_feature = string_clean_capital_fn(str(row["INFRA:PNT_OBJ"]))
        # print('point_infra_feature: ', point_infra_feature)
        clean_infra_feature = (points_dict[point_infra_feature])

        if feat_label != "Not recorded":
            final_label_ = "{0} {1}".format(feat_label.title(), clean_infra_feature.title())

            # remove repeated string variables and create a final string.
            final_label = ' '.join(remove_duplicate_substrings_fn(final_label_.split()))
            # create a status abbreviation for the label feature
            if cond_label != "Not recorded":
                if cond_label == "Abandoned":
                    cond_abrev = "Abd"
                elif cond_label == "Disused":
                    cond_abrev = "Dis"
                else:
                    pass

                final_label = "{0} {1} ({2}.)".format(feat_label, clean_infra_feature.title(), cond_abrev)
            else:
                final_label = "Not Recoded"

        else:
            final_label = "Not Recorded" # clean_infra_feature


    else:
        pass

    prop_name = string_clean_capital_fn(str(row["PROPERTY"]))
    if prop_name:
        prop = prop_name.upper()

    else:
        prop = "UNKNOWN"

    prop_code = str(row["PROP_TAG"])
    if prop_name:
        prop_tag = prop_code
    else:
        prop_tag = "XXX"

    district = str(row["DISTRICT"])

    label_comment_list = [district, prop, prop_tag, final_label.title().strip(), cond_label, comment]

    return label_comment_list


'''
def label_comment_fn(row, string_clean_capital_fn):
    """
    Extract the three variables feat_label, cond_label and comment.

    :param row: pandas row index
    :param string_clean_capital_fn: function that cleans string objects.
    :return label_comment_list: list object containing the three variables feat_label, cond_label and comment
    """
    feat_label = string_clean_capital_fn(str(row["FEATURE_ATTRIB:INF_LABEL"]))
    cond_label = string_clean_capital_fn(str(row["FEATURE_ATTRIB:COND_LABEL"]))
    comment = string_clean_capital_fn(str(row["FEATURE_ATTRIB:FREE_TEXT"]))
    print('comment: ', comment)
    

    prop_name = string_clean_capital_fn(str(row["PROPERTY"]))
    if prop_name:
        prop = prop_name
    else:
        prop = "UNKNOWN"

    prop_code = str(row["PROP_TAG"])
    if prop_name:
        prop_tag = prop_code
    else:
        prop_tag = "XXX"

    label_comment_list = [prop, prop_tag, feat_label, cond_label, comment]

    return label_comment_list

'''


def property_name_extraction_fn(orig_odk_df, pastoral_estate, directory):
    """
    Identifies which pastoral property each point overlays and inserts the property name and tenure reference two
    columns - if a point is outside of the Pastoral Estate it is classified as:
    property name - UNKNOWN
    property reference:- XXX

    :param orig_odk_df: data frame object containing raw odk point data
    :param pastoral_estate: string object containing the path to the Pastoral Estate shapefile.
    :param directory: string object containing the path to the temporary directory.
    :return final_df: pandas dataframe object containing the raw odk point data with the extracted property name that
    it overlays.
    """
    identify_dir = "{0}\\identify".format(directory)
    os.makedirs(identify_dir)

    plots_dir = "{0}\\identify_plots".format(directory)
    os.makedirs(plots_dir)

    # Create a geo-dataframe in the datum WGS84
    gdf = gpd.GeoDataFrame(
        orig_odk_df, geometry=gpd.points_from_xy(orig_odk_df["GROUP_COORDINATES:SITE_GPS1:Longitude"],
                                                 orig_odk_df["GROUP_COORDINATES:SITE_GPS1:Latitude"]), crs=4326)

    # project the pastoral estate shapefile to WGS84
    pe_gdf_4326 = pastoral_estate.to_crs("EPSG:4326")

    odk_all_list = pe_gdf_4326.PROPERTY.unique().tolist()

    list_masked_df = []
    for prop_name in pe_gdf_4326.PROPERTY.unique():
        prop_gdf = pe_gdf_4326[pe_gdf_4326["PROPERTY"] == prop_name]
        prop_code = prop_gdf.loc[prop_gdf["PROPERTY"] == prop_name, "PROP_TAG"].iloc[0]

        property_mask = gdf.within(prop_gdf.geometry.iloc[0])
        property_data = gdf.loc[property_mask]
        point_overlay = pd.DataFrame(property_data)
        point_overlay["PROPERTY"] = prop_name
        point_overlay["PROP_TAG"] = prop_code

        if point_overlay.empty != True:
            print("=" * 50)
            print("prop name identified: ", prop_name)
            point_overlay.to_csv("{0}\\{1}_identify.csv".format(identify_dir, prop_name.title()))

            list_masked_df.append(point_overlay)

        else:
            orig_odk_df["PROPERTY"] = "UNKNOWN"
            orig_odk_df["PROP_TAG"] = "XXX"

    print("=" * 50)
    # concatenate the outputs stored in list_masked_df
    if len(list_masked_df) == 0:
        df_concat = pd.DataFrame()

    elif 0 > len(list_masked_df) <= 1:
        df_concat = list_masked_df[0]

    else:
        df_concat = pd.concat(list_masked_df)
    # concat original odk point dataframe with df_concat(includes property names)
    df_all = pd.concat([df_concat, orig_odk_df])
    # drop duplicates from the concatenated dataframe excluding the column PROPERTY
    final_df = df_all.drop_duplicates(subset=df_all.columns.difference(["PROPERTY", "PROP_TAG"]))
    # drop the geometry column
    final_df.drop("geometry", axis="columns", inplace=True)
    return final_df


def district_identify_fn(final_df, pastoral_estate):
    df_list = []
    pastoral_estate.to_file(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs\past_estate_check.shp")
    for prop in final_df.PROPERTY.unique():

        df = final_df.loc[final_df["PROPERTY"] == prop]
        if prop == "UNKNOWN":
            df["DISTRICT"] = "UNKNOWN"
            df_list.append(df)
        else:
            district = pastoral_estate.loc[pastoral_estate["PROPERTY"] == prop, 'DISTRICT'].item()

            df["DISTRICT"] = district
            df_list.append(df)
    district_df = pd.concat(df_list)

    return district_df


def processing_workflow_fn(temp_dir, string_clean_capital_fn, feature_df, feature, feature_group_dict, feature_dict,
                           weeds_bot_com):
    """
    Control the processing workflow based on the ODK Mapping feature class variable ( i.e. infrastructure, clearing).

    :param weeds_bot_com: string object containing the path to the weeds_list.csv file
    - file contains all known botanical and common weeds names.
    :param feature_group_dict: dictionary object used to reclassify lower level feature classes into upper level feature
    classes.
    :param feature: string object containing one of the ODK Mapping feature classes.
    :param feature_df: dataframe object that has been filtered by feature.
    :param string_clean_capital_fn: function that cleaned string objects.
    :param temp_dir: string object containing the path to temporary sub-directory titled by the property name within the
    primary temp dir.
    :return temp_dir: string object containing the path to temporary sub-directory titled by the property name within the
    primary temp dir.
    """

    csv_output_list = []

    if feature == "infrastructure":
        import step2_2_infrastructure_mapping
        step2_2_infrastructure_mapping.main_routine(
            temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
            photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict)

    elif feature == "clearing":
        import step2_3_clearing_mapping
        step2_3_clearing_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                              gps_points_fn,
                                              photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                              feature_group_dict, feature_dict)

    elif feature == "paddock":
        import step2_4_paddock_mapping
        step2_4_paddock_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                             gps_points_fn,
                                             photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                             feature_group_dict, feature_dict)

    elif feature == "erosion":
        import step2_5_erossion_mapping
        step2_5_erossion_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                              gps_points_fn,
                                              photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                              feature_group_dict, feature_dict)

    elif feature == "weed":
        import step2_6_weeds_mapping
        step2_6_weeds_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                           gps_points_fn,
                                           photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                           feature_group_dict, feature_dict, weeds_bot_com)

    elif feature == "woody_thickening":
        import step2_7_woody_thickening_mapping
        step2_7_woody_thickening_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature,
                                                      date_time_fn,
                                                      gps_points_fn,
                                                      photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                                      feature_group_dict, feature_dict)

    elif feature == "feral_animal":
        import step2_8_feral_animals_mapping
        step2_8_feral_animals_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                                   gps_points_fn,
                                                   photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                                   feature_group_dict, feature_dict)

    elif feature == "fire":
        import step2_9_fire_mapping
        step2_9_fire_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                          gps_points_fn,
                                          photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                          feature_group_dict, feature_dict)

    elif feature == "sinkhole":
        import step2_10_sinkhole_mapping
        step2_10_sinkhole_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                                               gps_points_fn,
                                               photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                               feature_group_dict, feature_dict)

    elif feature == "unidentified_species":
        import step2_11_unidentified_species_mapping
        step2_11_unidentified_species_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature,
                                                           date_time_fn,
                                                           gps_points_fn,
                                                           photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                                           feature_group_dict, feature_dict)

    elif feature == "other_feature":
        import step2_12_other_feature_mapping
        step2_12_other_feature_mapping.main_routine(temp_dir, string_clean_capital_fn, feature_df, feature,
                                                    date_time_fn,
                                                    gps_points_fn,
                                                    photo_url_extraction_fn, meta_data_fn, label_comment_fn,
                                                    feature_group_dict, feature_dict)

    else:
        print(feature, " script not active")
        pass

    return temp_dir


def temp_dir_folders_fn(primary_temp_dir, feature_list, prop_name):
    """
    Create directory tree within the temporary directory based on property name.

    :param prop_name: strong object containing the property name which the point is located.
    :param primary_temp_dir: string object containing the newly created temporary directory path.
    :param feature_list: list object containing the primary feature names.
    :return dir: string object containing the path to the top of the directory tree (i.e. property name).
    """

    property_name = prop_name.replace(" ", "_").title()
    dir = ("{0}\\{1}".format(primary_temp_dir, property_name))

    if not os.path.exists(dir):
        os.mkdir(dir)

    for i in feature_list:
        feature_dir = ("{0}\\{1}".format(dir, i))
        os.mkdir(feature_dir)

        temp_path = ("{0}\\temp_shape".format(feature_dir))
        os.mkdir(temp_path)

        shapefile_path = ("{0}\\shapefile".format(feature_dir))
        os.mkdir(shapefile_path)

    return dir


def export_dir_folders_fn(export_dir, feature_list, prop_name):
    """
    Create directory tree within the export directory based on property name.

    :param prop_name: strong object containing the property name which the point is located.
    :param export_dir: string object containing the newly created export directory path.
    :param feature_list: list object containing the primary feature names.
    :return property_directory: string object containing the path to the top of the directory tree (i.e. property name).
    """

    property_name = prop_name.replace(" ", "_").title()
    property_directory = ("{0}\\{1}".format(export_dir, property_name))

    if not os.path.exists(property_directory):
        os.mkdir(property_directory)

    for i in feature_list:
        dir = ("{0}\\{1}".format(property_directory, i))
        os.mkdir(dir)

        shapefile_path = ("{0}\\shapefile".format(dir))
        os.mkdir(shapefile_path)

        csv_path = ("{0}\\csv".format(dir))
        os.mkdir(csv_path)

        photos_path = ("{0}\\photos".format(dir))
        os.mkdir(photos_path)

    return property_directory


def main_routine(file_path, primary_temp_dir, pastoral_estate, feature_list, primary_export_dir, start_date, end_date,
                 pastoral_districts_path, weeds_bot_com, prop_enquire, user_df, transition_dir,
                 infrastructure_directory, assets_dir, remote_desktop):


    print('start 2.1')
    """
    Control the ODK Mapping data extraction workflow.

    :param pastoral_districts_path:
    :param prop_enquire:
    :param weeds_bot_com: string object containing the path to the weeds_list.csv file
    - file contains all known botanical and common weeds names.
   :param start_date: string object (command argument) containing the start date that the user wishes to filter the
    ODK Aggregate Results data to be filtered from.
    :param end_date: string object (command argument) containing the end date that the user wishes to filter the
    ODK Aggregate Results data to be filtered to.
    :param primary_export_dir: string object containing the path to the head of the export directory tree.
    :param feature_list: list object containing the primary feature names.
    :param pastoral_estate: string object containing the path to the Pastoral Estate shapefile.
    :param file_path: string object containing the dir_path concatenated with search_criteria.
    :param primary_temp_dir: string object path to the created output directory (date_time).
    """

    feature_group_dict = {"Bore": "Water Points", "Dam": "Water Points", "Pump out point": "Water Points",
                          "Trough": "Water Points", "Water tank": "Water Points", "Turkey nest": "Water Points",
                          "Waterhole": "Water Points",
                          "Aerial": 'Aviation Points', "Landing ground": 'Aviation Points',
                          "Stock yard": "Cultural Points", "Underground mine": "Cultural Points",
                          "Quarry": "Cultural Points",

                          "General building": 'Building Points', "Homestead": 'Building Points',
                          "Gate": 'Transport Points',
                          "Monitoring site": "rmb_site",
                          "Fence": 'Cultural Lines', "Cleared line": 'Cultural Lines',
                          "Water pipeline": 'Water Lines',
                          'Paddock Boundary': 'Cultural Areas', 'Paddock': 'Cultural Areas',
                          'Quarry': 'Cultural Areas', 'General Cultural Feature': 'Cultural Areas',
                          'Landing Strip': 'Aviation Areas'}

    feature_dict = {"Bore": "Bore", "Dam": "Dam", "Pump out point": "Pump Out Point", "Trough": 'Trough',
                    "Water tank": 'Water Tank', "Turkey nest": 'Turkey Nest', "Waterhole": 'Waterhole',
                    "Aerial": 'Aerial or Tower', "Landing ground": 'Landing Ground',
                    "Stock yard": 'Stock Yard', "Underground mine": 'Underground Mine', "Quarry": 'Quarry',
                    "General building": 'General Building', "Homestead": 'Homestead',
                    "Gate": 'Gate', "Monitoring site": "RMB Site",
                    "Fence": 'Fenceline', "Cleared line": 'Cleared Line',
                    "Water pipeline": 'Water Pipeline',
                    'Paddock Boundary': 'Paddock', 'Paddock': 'Paddock', "Track": 'Vehicle Track',
                    'Quarry': 'Quarry', 'General Cultural Feature': 'General Cultural Feature',
                    'Landing Strip': 'Landing Strip'}

    # Read in the star transect csv as a Pandas DataFrame.
    df = pd.read_csv(file_path)

    df["FEATURE_ATTRIB:FREE_TEXT"] = df["FEATURE_ATTRIB:FREE_TEXT"].fillna('Not recorded')
    df["FEATURE_ATTRIB:COND_LABEL"] = df["FEATURE_ATTRIB:COND_LABEL"].fillna('Not recorded')
    df["FEATURE_ATTRIB:INF_LABEL"] = df["FEATURE_ATTRIB:INF_LABEL"].fillna('Not recorded')

    # add a uid column to the dataframe
    df.insert(0, "orig_uid", "")
    df["orig_uid"] = df.index + 1

    # filter dataframe based on start and end dates.
    date_df = df[(df["START"] > start_date) & (df["START"] < end_date)]

    # call the property_name_extraction_fn to determine the property name the feature is located.
    property_df = property_name_extraction_fn(date_df, pastoral_estate, primary_temp_dir)
    district_df = district_identify_fn(property_df, pastoral_estate)

    # Determine the property name list
    if prop_enquire not in ["ALL", "ALL_ODK"]:
        print("prop enquire is not ALL or ALL_ODK")
        property_df_ = district_df.loc[
            (district_df["PROPERTY"] == prop_enquire) | (district_df["PROPERTY"] == "UNKNOWN")]
        property_df_.to_csv(r"Z:\Scratch\Rob\property_df_NOT_ALL.csv")
        odk_all_list = [prop_enquire]

    else:
        # print("prop enquire is EITHER ALL or ALL ODK")
        property_df_ = district_df

        odk_all_list = property_df_["PROPERTY"].unique().tolist()
        # print("odk_all_list:: ", odk_all_list)
        property_df_.to_csv(r"Z:\Scratch\Rob\property_df_.csv")



    # loop through the and filter the dataframe based on unique property names

    processed_property_list = []
    for prop_name in property_df_.PROPERTY.unique():
        print('=' * 50)
        processed_property_list.append(prop_name)
        print('property name to be processed: ', prop_name)
        prop_df = property_df_.loc[property_df_["PROPERTY"] == prop_name]

        # call the primary_temp_dir_folders_fn function to create sub-folders within the temp directory.
        temp_dir = temp_dir_folders_fn(primary_temp_dir, feature_list, prop_name)

        # call the export_dir_folders_fn function to create sub-folders within the export directory.
        export_prop_dir = export_dir_folders_fn(primary_export_dir, feature_list, prop_name)

        # loop through the and filter the dataframe based on unique feature names
        for feature in prop_df["GROUP_FEATURE:FEATURE"].unique():
            feature_df = prop_df[prop_df["GROUP_FEATURE:FEATURE"] == feature]

            temp_dir = processing_workflow_fn(temp_dir, string_clean_capital_fn, feature_df, feature,
                                              feature_group_dict, feature_dict, weeds_bot_com)

        for feature in feature_list:

            if feature == "infra_lines":
                import step3_1_compile_line_infrastructure
                step3_1_compile_line_infrastructure.main_routine(temp_dir, feature, export_prop_dir)

            elif feature == "infra_points":

                import step3_2_compile_points_infrastructure
                step3_2_compile_points_infrastructure.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                                   user_df)

            elif feature == "infra_water_points":
                import step3_3_compile_water_points_infrastructure
                step3_3_compile_water_points_infrastructure.main_routine(temp_dir, feature, export_prop_dir,
                                                                         pastoral_estate, user_df)

            elif feature == "paddock":
                import step3_4_compile_points_paddock
                step3_4_compile_points_paddock.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                            user_df)

            elif feature == "clearing":
                import step3_3_compile_points_clearing
                step3_3_compile_points_clearing.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                             user_df)

            elif feature == "erosion":
                import step3_5_compile_points_erosion
                step3_5_compile_points_erosion.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                            user_df)

            elif feature == "erosion":
                import step3_5_compile_points_erosion
                step3_5_compile_points_erosion.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                            user_df)

            elif feature == "weeds":
                import step3_6_compile_points_weeds_update
                step3_6_compile_points_weeds_update.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                                 user_df)

            elif feature == "woody_thickening":
                import step3_7_compile_points_woody_thickening
                step3_7_compile_points_woody_thickening.main_routine(temp_dir, feature, export_prop_dir,
                                                                     pastoral_estate, user_df)

            elif feature == "feral_animals":
                import step3_8_compile_points_feral_animals
                step3_8_compile_points_feral_animals.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                                  user_df)

            elif feature == "fire":
                import step3_9_compile_points_fire
                step3_9_compile_points_fire.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate, user_df)

            elif feature == "unidentified":
                import step3_11_compile_points_unidentified
                step3_11_compile_points_unidentified.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                                  user_df)

            elif feature == "other_feature":
                import step3_12_compile_points_other_feature
                step3_12_compile_points_other_feature.main_routine(temp_dir, feature, export_prop_dir, pastoral_estate,
                                                                   user_df)
            else:
                pass

    print('=' * 50)

    if remote_desktop != 'offline':

        import step5_1_file_outputs_to_working_drive
        step5_1_file_outputs_to_working_drive.main_routine(primary_export_dir, pastoral_districts_path, start_date)

        import step6_1_download_adjacent_infrastructure
        step6_1_download_adjacent_infrastructure.main_routine(pastoral_districts_path, start_date, primary_export_dir,
                                                              prop_enquire, infrastructure_directory, pastoral_estate,
                                                              odk_all_list)
    else:
        print('You are processing offline; as such, outputs will not be filed and no previous infrastructure data will '
              'be downloaded.')

    # print('['*50)
    # print('pastoral_districts_path: ', pastoral_districts_path)
    return processed_property_list


if __name__ == "__main__":
    main_routine()
