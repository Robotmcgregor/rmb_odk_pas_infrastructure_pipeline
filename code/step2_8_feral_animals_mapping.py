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


def feral_fn(row, string_clean_capital_fn):
    """ Extract variables relevant to the clearing feature.

    :param row: index object - current row of the pandas data frame that is being worked on.
    :param string_clean_capital_fn: function that cleaned string objects.
    :return clearing_output: list object containing seven feral animal variables.
    """
    required_list = ["camel", "rabbit", "donkey", "horse", "pig", "buffalo", "nat_herb"]

    feral_list = ["blank", "blank", "blank", "blank", "blank", "blank", "blank", "blank"]
    n = 1
    for i in required_list:

        feral_type = str(row["FERAL_MAIN:FERAL" + str(n)])
        feral_evid = string_clean_capital_fn(str(row["FERAL_MAIN:FERAL" + str(n) + '_EVID']))
        if feral_type == "camel":
            feral_list[0] = feral_evid
        elif feral_type == "rabbit":
            feral_list[1] = feral_evid
        elif feral_type == "donkey":
            feral_list[2] = feral_evid
        elif feral_type == "horse":
            feral_list[3] = feral_evid
        elif feral_type == "pig":
            feral_list[4] = feral_evid
        elif feral_type == "buffalo":
            feral_list[5] = feral_evid
        elif feral_type == "nat_herb":
            feral_list[6] = feral_evid

        feral_other = string_clean_capital_fn(str(row["FERAL_MAIN:FERAL_OTHER_EVID"]))
        if feral_other != 'Nan':
            feral_list[7] = feral_other
        """print(feral_other)
        feral_list.append(feral_other)"""
        #print('feral_list: ', feral_list)

    return feral_list


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn, gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):
    """ Extract the clearing variables and export a csv and shapefile.

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

    final_feral_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        feral_list = []

        # call the date_time_fn function to extract the date and time records.
        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        # call the label_comment_fn function to extract the comment and label records.
        label_comment_list = label_comment_fn(row, string_clean_capital_fn)

        # call the feral_fn function to extract the feral animal variable records.
        feral_output = feral_fn(row, string_clean_capital_fn)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        # append variables to a list
        feral_list.append(feature)
        feral_list.append(start_date)
        feral_list.extend(label_comment_list)
        feral_list.extend(feral_output)
        feral_list.extend(photo_url_list)
        feral_list.extend(lat_lon_list)
        feral_list.extend(meta_data_list)

        # append variables to a final list that will make up a complete feature related dataframe.
        final_feral_list.append(feral_list)

    # create a dataframe from the final list
    feral_df = concat_list_to_df_fn(final_feral_list)

    #print(feral_df)
    # rename column headers
    feral_df.columns = ["feature", "date_rec", "district", "property", "prop_code", "label", "condition", "comment",
                        "camel", "rabbit", "donkey", "horse", "pig", "buffalo", "nat_herb", "other",
                        "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                        "bear1", "meta_key", "clean_meta_key", "form_name"]

    # export csv to temporary directory
    csv_output = ("{0}\\feral_animals\\clean_feral_animals.csv".format(temp_dir))

    feral_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
