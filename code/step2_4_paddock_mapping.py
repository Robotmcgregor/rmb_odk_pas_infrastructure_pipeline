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
from datetime import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
import warnings
warnings.filterwarnings("ignore")


def paddock_fn(row, string_clean_capital_fn):
    """ Extract variables relevant to the paddock feature.

    :param row: index object - current row of the pandas data frame that is being worked on.
    :param string_clean_capital_fn: function that cleaned string objects.
    :return paddock_output: list object containing two paddock variables.
    """

    pdk_name = string_clean_capital_fn(str(row["FEAT_ATTRIB:PDK_NAME"]))
    land_use = string_clean_capital_fn(str(row["FEAT_ATTRIB:LAND_USE"]))

    if land_use == "Land use other":
        land_use.replace("other", str(row["FEAT_ATTRIB:LU_OTHER"]))

    paddock_output = [pdk_name, land_use]

    return paddock_output


def concat_list_to_df_fn(list_a):
    """ Define the method used to concatenate the data to a dataframe based on the length of the list.

    :param list_a: list object or a list of list object.
    :return df_concat: pandas dataframe object crated from the input list or list of lists (list_a):
    """
    if len(list_a) <= 1:
        df_concat = pd.DataFrame(list_a[0]).transpose()

    else:

        df_concat = pd.DataFrame.from_records(list_a)

    return df_concat


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):
    """ Extract the paddock variables and export a csv and shapefile.

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

    final_paddock_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        paddock_list = []

        # call the date_time_fn function to extract the date and time records.
        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        # call the label_comment_fn function to extract the comment and label records.
        label_comment_list = label_comment_fn(row, string_clean_capital_fn)

        # map_list = infrastructure_mapping_features(row, string_clean_capital_fn
        paddock_output = paddock_fn(row, string_clean_capital_fn)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        # append variables to a list
        paddock_list.append(feature)
        paddock_list.append(start_date)
        paddock_list.extend(label_comment_list)
        paddock_list.extend(paddock_output)
        paddock_list.extend(photo_url_list)
        paddock_list.extend(lat_lon_list)
        paddock_list.extend(meta_data_list)
        # append variables to a final list that will make up a complete feature related dataframe.
        final_paddock_list.append(paddock_list)

    # create a dataframe from the final list
    paddock_df = concat_list_to_df_fn(final_paddock_list)
    # rename column headers
    paddock_df.columns = ["feature", "date_rec", "district", "property", "prop_code", "label", "condition", "comment",
                          "pdk_name", "land_use",
                          "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                          "bear1", "meta_key", "clean_meta_key", "form_name"]

    # export csv to temporary directory
    csv_output = ("{0}\\paddock\\clean_paddock.csv".format(temp_dir))
    paddock_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
