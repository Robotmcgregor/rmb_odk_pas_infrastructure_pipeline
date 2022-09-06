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


def property_prop_tag_extraction_fn(row):
    """

    :param row: index object - current row of the pandas data frame that is being worked on.
    :return paddock_output: list object containing two property variables.
    """
    prop_tag = str(row["PROP_TAG"])
    prop = str(row["PROPERTY"])

    return prop, prop_tag


def weed_fn(row, string_clean_capital_fn, weeds_df):
    """ Extract the weed information.

     :param weeds_df: pandas dataframe object containing botanical and common weed names.
     :param row: pandas dataframe row value object.
     :param string_clean_capital_fn: function to remove whitespaces and clean strings.
     :return: output_list: list object containing three string variables:
     weed, common_name, weed_size, weed_den.
     """

    # extract the botanical weed name.
    weed = str(row["WEEDS:GROUP_WEED1:WEED1"])

    if weed == "other_weed":
        weed = weed.replace("other_weed", str(row["WEEDS:GROUP_WEED1:WEED1_OTHER"]))
    weed = string_clean_capital_fn(weed)

    # filter the weed_df dataframe based on the botanical name.
    located = weeds_df.loc[weeds_df["botanical"] == weed]

    # calculate the length of the filtered dataframe (i.e. 0 = botanical name not found) and extract common name.
    if len(located.index) > 0:
        common_name = located.common.iloc[0]
    else:
        common_name = None

    # extract weed size and density
    weed_size = str(row["WEEDS:GROUP_WEED1:SPECIES_SIZE1"])
    weed_den = str(row["WEEDS:GROUP_WEED1:SPECIES_DENSITY1"])

    output_list = [weed, common_name, weed_size, weed_den]

    return output_list


def device_id_fn(row):
    device_id = str(row['DEVICEID'])

    if device_id == 'collect:Gnm9lqtfIboIrxNE':
        user_id = "Chris Obst and Harrison Hughes"

    elif device_id == 'collect:zzQ3bCDHV1tzliXv':
        user_id = "Chris Obst and Harrison Hughes"

    elif device_id == 'collect:88vJCKkn2CCzga8o':
        user_id = "collect:88vJCKkn2CCzga8o"

    elif device_id == 'collect:9W3UalGBI3mI9cW1':
        user_id = "David Hooper and Rhonda Villiger"

    elif device_id == 'collect:rr2cQ91GYiI5JlU9':
        user_id = "Kate Stevens and Debbie Mitchell"

    elif device_id == 'collect:6MhPLIBuLwa5AXGd':
        user_id = "Eloise Kippers and Alvaro Gonzalez"

    elif device_id == 'collect:vi7AXBEUVNrexQLM':
        user_id = "Eloise Kippers and Alvaro Gonzalez"

    elif device_id == 'collect:J4uqGTakDrlzIXQJ':
        user_id = "Chris Obst and Harrison Hughes"


    else:
        user_id = device_id
    return user_id


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
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict,
                 weeds_bot_com):
    """ Extract the paddock variables and export a csv and shapefile.

    :param weeds_bot_com: string object containing the path to the weeds_list.csv file
    - file contains all known botanical and common weeds names.
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

    final_weeds_list = []

    # import weeds_list csv which contains all weed botanical and common names from the NT weeds database.
    weeds = pd.read_csv(weeds_bot_com, delimiter="\t", header=None)

    weeds.fillna("XXXX", inplace=True)
    weeds.columns = (["botanical", "common"])

    feature_df.to_csv("{0}\\weeds.csv".format(temp_dir))
    for index, row in feature_df.iterrows():
        weeds_list = []

        prop, prop_tag = property_prop_tag_extraction_fn(row)
        # call the date_time_fn function to extract date and time information.
        start_date = date_time_fn(row)

        # extract the year from the date variable
        year = start_date[-4:]

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        # call the clearing_fn function to extract the weeds variable records.
        weed_output = weed_fn(row, string_clean_capital_fn, weeds)

        # call the label_comment_fn function to extract the comment and label records.
        label_comment_list = label_comment_fn(row, string_clean_capital_fn)

        # call the device_id_fn function to extract the collectors name based on tablet id.
        user_id = device_id_fn(row)

        # append variables to a list
        weeds_list.append(feature)
        weeds_list.append(start_date)
        weeds_list.append(year)
        weeds_list.extend(label_comment_list)
        weeds_list.append(user_id)
        weeds_list.extend(weed_output)
        weeds_list.extend(photo_url_list)
        weeds_list.extend(lat_lon_list)
        weeds_list.extend(meta_data_list)


        # append variables to a final list that will make up a complete feature related dataframe.
        final_weeds_list.append(weeds_list)

    # create a dataframe from the final list
    weeds_df = concat_list_to_df_fn(final_weeds_list)
    weeds_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs\test_weeds.csv")
    # rename column headers
    weeds_df.columns = ["feature", "date_rec", "year", "district", "property", "prop_code", "label", "condition", "comment",
                        "recorder", "weed_bot", "weed_comm", "weed_size", "weed_den",
                        "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1", "acc1", "dist1",
                        "bear1",
                        "meta_key", "clean_meta_key", "form_name"]

    # export csv to temporary directory
    csv_output = ("{0}\\weeds\\clean_weeds.csv".format(temp_dir))
    weeds_df.to_csv(csv_output)

    csv_output = ("{0}\\clean_weeds.csv".format(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_mapping\outputs'))
    weeds_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
