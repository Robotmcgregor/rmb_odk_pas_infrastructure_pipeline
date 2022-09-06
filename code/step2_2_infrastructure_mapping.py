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
import warnings
warnings.filterwarnings("ignore")


def raw_gps_infrastructure_lines_fn(row):
    """ extract the lat, lon distance and bearing information for infrastructure lines.

    :param row: pandas dataframe row value object.
    :return final_output: list object containing nine lat, lon, distance, and bearing variables (floats).
    """

    output_list = []
    final_list = []
    for i in range(10):
        value_ = str(1 + i)
        # first gps points have already been collected.
        if value_ != "1":
            lat = float(row["GROUP_LINE:GPS{0}_GROUP:SITE_GPS{1}:Latitude".format(value_, value_)])
            if lat:
                latitude = lat
            else:
                latitude = "nan"
            lon = float(row["GROUP_LINE:GPS{0}_GROUP:SITE_GPS{1}:Longitude".format(value_, value_)])
            acc = float(row["GROUP_LINE:GPS{0}_GROUP:SITE_GPS{1}:Accuracy".format(value_, value_)])
            dist = float(row["GROUP_LINE:GPS{0}_GROUP:OFF{1}_DIST".format(value_, value_)])
            bear = float(row["GROUP_LINE:GPS{0}_GROUP:BEARING{1}".format(value_, value_)])

            output_list.extend([latitude, lon, acc, dist, bear])
    # remove NoneType values within list
    final_output = [0 if x != x else x for x in output_list]

    return final_output


def infrastructure_line_fn(row):
    """ Control and extract relevant infrastructure line data (i.e. fence-lines, water pipes)

    :param row: pandas dataframe row value object.
    :return output_list: list object containing nine lat, lon, distance, and bearing variables (floats).
    """

    # call the raw_gps_infrastructure_lines_fn function to extract nine lat lon and bearing values.
    output_list = raw_gps_infrastructure_lines_fn(row)

    return output_list


def infrastructure_point_fn(row, string_clean_capital_fn, feature_group_dict, feature_dict, header):
    """ Control and extract relevant infrastructure point data.

    :param feature_group_dict: dictionary object used to reclassify lower level feature classes into upper level feature
    classes.
    :param header: string object dictionary object used to reclassify lower level feature classes into upper level
    feature classes.
    :param row: pandas dataframe row value object.
    :param string_clean_capital_fn: function removes dashes, whitespaces and returns string object with a capital
     character at the beginning of the string.
    :return feature_group: string object containing the higher level infrastructure feature class (i.e. water).
    ":return feature: string object containing the lower level infrastructure feature class (i.e. bore).
    """

    feat = string_clean_capital_fn(str(row[header]))
    feature_ = (feature_dict[feat])
    feature_group = (feature_group_dict[feat])

    return feature_group, feature_


def infrastructure_water_point_fn(row, string_clean_capital_fn):
    """ Control and extract relevant infrastructure water points data (i.e. bores and tanks)

    :param row: pandas dataframe row value object.
    :param string_clean_capital_fn: function removes dashes, whitespaces and returns string object with a capital
     character at the beginning of the string.
    :return:
    """
    feature_type = string_clean_capital_fn(str(row["INFRA:WAT_OBJ"]))

    return feature_type


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):

    """ Extract the infrastructure variables and export a csv and shapefile.

    :param feature_df: pandas dataframe object that has is currently being processed - filtered on property and feature. o
    :param date_time_fn: function created in step2_1 to extract date and time data.
    :param gps_points_fn: function created in step2_1 to extract location data.
    :param photo_url_extraction_fn: function created in step2_1 to extract photo url paths.
    :param meta_data_fn: function created in step2_1 to extract metadata.
    :param label_comment_fn: function created in step2_1 to extract comment data etc.
    :param feature_group_dict: dictionary object used to reclassify lower level feature classes into upper level feature
    classes.
    :param temp_dir: string object containing the directory path.
    :param string_clean_capital_fn: function used to clean strings.
    :param feature: string object containing the feature being mapped.
    """

    # create three empty lists
    final_infra_line_list = []
    final_infra_point_list = []
    final_infra_water_point_list = []

    for index, row in feature_df.iterrows():
        # create three empty list to be filled and emptied each iteration
        infra_line_list = []
        infra_point_list = []
        infra_water_point_list = []

        # extract the type of infrastructure (i.e. water point, point or line)
        infrastructure_type = str(row["INFRA:INF_FEAT"])
        if infrastructure_type == "line":
            # print(row)
            # call the date_time_fn function to extract date and time information.
            start_date = date_time_fn(row)

            # call the gps_points_fn function to extract the longitude and latitude information.
            lat_lon_list = gps_points_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            photo_url_list = photo_url_extraction_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            meta_data_list = meta_data_fn(row)

            # call the infrastructure_point_fn function to extract the feature and group feature records.
            feature_group, feature_type = infrastructure_point_fn(row, string_clean_capital_fn, feature_group_dict, feature_dict,
                                                                  "INFRA:LINE_OBJ")
            # call the infrastructure_line_fn function to extract the feature and group feature records.
            output_list = infrastructure_line_fn(row)

            # call the label_comment_fn function to extract the comment and label records.
            label_comment_list = label_comment_fn(row, string_clean_capital_fn)

            # map_list = infrastructure_mapping_features(row, string_clean_capital_fn)

            # append variables to a list
            infra_line_list.append(infrastructure_type)
            infra_line_list.append(feature_group)
            infra_line_list.append(feature_type)
            infra_line_list.extend(label_comment_list)
            infra_line_list.append(start_date)
            infra_line_list.extend(photo_url_list)
            infra_line_list.extend(lat_lon_list)
            infra_line_list.extend(output_list)
            infra_line_list.extend(meta_data_list)

            # append variables to a final list that will make up a complete feature related dataframe.
            final_infra_line_list.append(infra_line_list)

        elif infrastructure_type == "point":

            # call the date_time_fn function to extract date and time information.
            start_date = date_time_fn(row)

            # call the gps_points_fn function to extract the longitude and latitude information.
            lat_lon_list = gps_points_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            photo_url_list = photo_url_extraction_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            meta_data_list = meta_data_fn(row)

            # call the infrastructure_point_fn function to extract the feature and group feature records.
            feature_group, feature_type = infrastructure_point_fn(row, string_clean_capital_fn, feature_group_dict, feature_dict,
                                                                  "INFRA:PNT_OBJ")

            # call the label_comment_fn function to extract the comment and label records.
            label_comment_list = label_comment_fn(row, string_clean_capital_fn)

            # append variables to a list
            infra_point_list.append(infrastructure_type)
            infra_point_list.append(feature_group)
            infra_point_list.append(feature_type)
            infra_point_list.extend(label_comment_list)
            infra_point_list.append(start_date)
            infra_point_list.extend(photo_url_list)
            infra_point_list.extend(lat_lon_list)
            infra_point_list.extend(meta_data_list)

            # append variables to a final list that will make up a complete feature related dataframe.
            final_infra_point_list.append(infra_point_list)

        else:

            # call the date_time_fn function to extract date and time information.
            start_date = date_time_fn(row)

            # call the gps_points_fn function to extract the longitude and latitude information.
            lat_lon_list = gps_points_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            photo_url_list = photo_url_extraction_fn(row)

            # call the meta_date_fn function to extract the unique identifier information for each form record.
            meta_data_list = meta_data_fn(row)

            # call the infrastructure_point_fn function to extract the feature and group feature records.
            feature_group, feature_type = infrastructure_point_fn(row, string_clean_capital_fn, feature_group_dict, feature_dict,
                                                                  "INFRA:WAT_OBJ")

            # call the label_comment_fn function to extract the comment and label records.
            label_comment_list = label_comment_fn(row, string_clean_capital_fn)

            # append variables to a list
            infra_water_point_list.append(infrastructure_type)
            infra_water_point_list.append(feature_group)
            infra_water_point_list.append(feature_type)
            infra_water_point_list.extend(label_comment_list)
            infra_water_point_list.append(start_date)
            infra_water_point_list.extend(photo_url_list)
            infra_water_point_list.extend(lat_lon_list)
            infra_water_point_list.extend(meta_data_list)
            # append variables to a final list that will make up a complete feature related dataframe.
            final_infra_water_point_list.append(infra_water_point_list)

    # ---------------------------------------------- lines -------------------------------------------------------------

    line_column_list = ["shape", "feature_group", "feature", "district", "property", "prop_code", "label", "condition",
                        "comment", "date_rec", "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1",
                        "lon1", "acc1", "dist1", "bear1", "lat2", "lon2", "acc2", "dist2", "bear2",
                        "lat3", "lon3", "acc3", "dist3", "bear3", "lat4", "lon4", "acc4", "dist4", "bear4",
                        "lat5", "lon5", "acc5", "dist5", "bear5", "lat6", "lon6", "acc6", "dist6", "bear6",
                        "lat7", "lon7", "acc7", "dist7", "bear7", "lat8", "lon8", "acc8", "dist8", "bear8",
                        "lat9", "lon9", "acc9", "dist9", "bear9", "lat10", "lon10", "acc10", "dist10",
                        "bear10", "meta_key", "clean_meta_key", "form_name"]

    if len(final_infra_line_list) > 1:
        # create a dataframe from the final list
        infra_line_df = pd.DataFrame(final_infra_line_list)
        # rename column headers
        infra_line_df.columns = line_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_lines\\clean_infra_line.csv".format(temp_dir))
        infra_line_df.to_csv(csv_output)
        infra_line_df.to_csv("Z:\\Scratch\\Zonal_Stats_Pipeline\\rmb_mapping\\outputs\\clean_infra_line.csv")

    elif len(final_infra_line_list) == 1:
        # create a dataframe from the final list
        infra_line_df = pd.DataFrame(final_infra_line_list)
        # rename column headers
        infra_line_df.columns = line_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_lines\\clean_infra_line.csv".format(temp_dir))
        infra_line_df.to_csv(csv_output)
        infra_line_df.to_csv("Z:\\Scratch\\Zonal_Stats_Pipeline\\rmb_mapping\\outputs\\clean_infra_line.csv")

    else:
        pass

    # ------------------------------------------------- points ---------------------------------------------------------

    csv_output_list = []
    infra_point_column_list = ["shape", "feature_group", "feature", "district", "property", "prop_code", "label", "condition",
                               "comment", "date_rec", "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1",
                               "lon1", "acc1", "dist1", "bear1", "meta_key", "clean_meta_key", "form_name"]

    if len(final_infra_point_list) > 1:
        # create a dataframe from the final list
        infra_point_df = pd.DataFrame(final_infra_point_list)
        # rename column headers
        infra_point_df.columns = infra_point_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_points\\clean_infra_point.csv".format(temp_dir))
        infra_point_df.to_csv(csv_output)

    elif len(final_infra_point_list) == 1:
        # create a dataframe from the final list
        infra_point_df = pd.DataFrame(final_infra_point_list)
        # rename column headers
        infra_point_df.columns = infra_point_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_points\\clean_infra_point.csv".format(temp_dir))
        infra_point_df.to_csv(csv_output)

    else:
        pass

    # ---------------------------------------------- water points ------------------------------------------------------

    if len(final_infra_water_point_list) > 1:
        # create a dataframe from the final list
        infra_water_point_df = pd.DataFrame(final_infra_water_point_list)
        # rename column headers
        infra_water_point_df.columns = infra_point_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_water_points\\clean_infra_water_point.csv".format(temp_dir))
        infra_water_point_df.to_csv(csv_output)

    elif len(final_infra_water_point_list) == 1:
        # create a dataframe from the final list
        infra_water_point_df = pd.DataFrame(final_infra_water_point_list)
        # rename column headers
        infra_water_point_df.columns = infra_point_column_list

        # export csv to temporary directory
        csv_output = ("{0}\\infra_water_points\\clean_infra_water_point.csv".format(temp_dir))
        infra_water_point_df.to_csv(csv_output)

    else:
        pass


if __name__ == "__main__":
    main_routine()
