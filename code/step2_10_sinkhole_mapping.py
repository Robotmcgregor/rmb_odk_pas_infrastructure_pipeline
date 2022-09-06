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


def sink_hole_fn(row, string_clean_capital_fn):
    """

    :param row:
    :param string_clean_capital_fn:
    :return:
    """
    sink_hole_diam = str(row["FEAT_ATTRIB:SINK_DIAM"])

    sink_hole_output = [sink_hole_diam]

    return sink_hole_output


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):
    """ Extract the sink_hole variables and export a csv and shapefile.

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

    final_sink_hole_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        sink_hole_list = []
        # call the date_time_fn function to extract date and time information.

        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        label_comment_list = label_comment_fn(row, string_clean_capital_fn)
        # map_list = infrastructure_mapping_features(row, string_clean_capital_fn
        sink_hole_output = sink_hole_fn(row, string_clean_capital_fn)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        sink_hole_list.append(feature)
        sink_hole_list.append(start_date)
        sink_hole_list.extend(label_comment_list)
        sink_hole_list.extend(sink_hole_output)
        sink_hole_list.extend(photo_url_list)
        sink_hole_list.extend(lat_lon_list)
        sink_hole_list.extend(meta_data_list)

        final_sink_hole_list.append(sink_hole_list)

    sink_hole_df = concat_list_to_df_fn(final_sink_hole_list)

    sink_hole_df.columns = ["feature", "date_rec", "district", "property", "prop_code", "label", "condition", "comment",
                            "sink_hole_diam",
                            "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                            "bear1", "meta_key", "clean_meta_key", "form_name", ]

    csv_output = ("{0}\\sinkhole\\clean_sink_hole.csv".format(temp_dir))
    sink_hole_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
