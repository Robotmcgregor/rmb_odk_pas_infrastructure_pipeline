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


def fire_fn(row):
    """ Extract the fire information and amend string objects using relevant dictionaries.
            :param row: pandas dataframe row value object.
            :return: fire_list: list object containing four processed string variables:
            north_ff, north_fi, south_ff, south_fi."""

    north_ff_values = {"NFF_absent": "Absent", "since_last_growth_event": "Since last growth event",
                       "before_last_growth_event": "Before last growth event", "nan": "BLANK"}

    north_fi_values = {"NFI_absent": "Absent", "low_cool": "Low intensity/cool fire",
                       "low_moderate": "Low/moderate", "moderate": "Moderate",
                       "moderate_high": "Moderate/high", "high": "High", "nan": "BLANK"}

    south_ff_values = {"SFF_absent": "Absent", "<1": "<12 months", "1-2": "1-2 years", "2-10": "2-10 years",
                       ">10": ">10 years", "nan": "BLANK"}

    south_fi_values = {"SFI_absent": "Absent", "cool": "Cool fire", "hot": "Hot fire", "nan": "BLANK"}

    value = str(row["FIRE:NORTH_FF"])
    north_ff = (north_ff_values[value])

    value = str(row["FIRE:NORTH_FI"])
    north_fi = (north_fi_values[value])

    value = str(row["FIRE:SOUTH_FF"])
    print("value: ", value)
    south_ff = (south_ff_values[value])

    value = str(row["FIRE:SOUTH_FI"])
    south_fi = (south_fi_values[value])

    fire_output = [north_ff, north_fi, south_ff, south_fi]
    return fire_output


def concat_list_to_df_fn(list_a):
    """

    :param list_a:
    :return:
    """

    if len(list_a) <= 1:
        df_concat = pd.DataFrame(list_a[0]).transpose()

    else:
        # df_concat = pd.concat(list_a)
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

    final_fire_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        fire_list = []
        # call the date_time_fn function to extract date and time information.

        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        label_comment_list = label_comment_fn(row, string_clean_capital_fn)
        # map_list = infrastructure_mapping_features(row, string_clean_capital_fn
        print("ready for here 115")
        fire_output = fire_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        fire_list.append(feature)
        fire_list.append(start_date)
        fire_list.extend(label_comment_list)
        fire_list.extend(fire_output)
        fire_list.extend(photo_url_list)
        fire_list.extend(lat_lon_list)
        fire_list.extend(meta_data_list)

        final_fire_list.append(fire_list)

    fire_df = concat_list_to_df_fn(final_fire_list)

    fire_df.columns = ["feature", "date_rec", "district", "property", "prop_code", "label", "condition", "comment",
                       "north_ff", "north_fi", "south_ff", "south_fi",
                       "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                       "bear1", "meta_key", "clean_meta_key", "form_name", ]

    csv_output = ("{0}\\fire\\clean_fire.csv".format(temp_dir))
    fire_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
