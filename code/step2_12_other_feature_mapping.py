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
    """ Define the method used to concatenate the data to a dataframe based on the length of the list.

    :param list_a: list object or a list of list object.
    :return df_concat: pandas dataframe object crated from the input list or list of lists (list_a):
    """

    if len(list_a) <= 1:
        df_concat = pd.DataFrame(list_a[0]).transpose()

    else:
        df_concat = pd.DataFrame.from_records(list_a)

    return df_concat


def other_feature_fn(row, string_clean_capital_fn):
    """ Extract variables relevant to the other feature type.

    :param row: index object - current row of the pandas data frame that is being worked on.
    :param string_clean_capital_fn: function that cleaned string objects.
    :return clearing_output: string object containing the type of unlisted feature.
    """
    other_feature = string_clean_capital_fn(str(row["INFRA:OTHER_FEAT"]))

    return other_feature


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):
    """ Extract the sink_hole variables and export a csv and shapefile.

    :param feature_df:
    :param date_time_fn:
    :param gps_points_fn:
    :param photo_url_extraction_fn:
    :param meta_data_fn:
    :param label_comment_fn:
    :param feature_group_dict:
    :param temp_dir: string object containing the directory path.
    :param string_clean_capital_fn: function used to clean strings.
    :param feature: string object containing the feature being mapped.
    :return:
    """

    final_other_feature_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        other_feature_list = []
        # call the date_time_fn function to extract date and time information.

        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        label_comment_list = label_comment_fn(row, string_clean_capital_fn)
        # map_list = infrastructure_mapping_features(row, string_clean_capital_fn
        other_feature = other_feature_fn(row, string_clean_capital_fn)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        other_feature_list.append(feature)
        other_feature_list.append(other_feature)
        other_feature_list.append(start_date)
        other_feature_list.extend(label_comment_list)
        other_feature_list.extend(photo_url_list)
        other_feature_list.extend(lat_lon_list)
        other_feature_list.extend(meta_data_list)

        final_other_feature_list.append(other_feature_list)

    other_feature_df = concat_list_to_df_fn(final_other_feature_list)

    other_feature_df.columns = ["feature", "oth_feat", "date_rec", "district", "property", "prop_code", "label", "condition",
                                "comment",
                                "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                                "bear1", "meta_key", "clean_meta_key", "form_name"]

    csv_output = ("{0}\\other_feature\\clean_other_feature.csv".format(temp_dir))
    other_feature_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
