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
from __future__ import print_function, division
import warnings
from glob import glob
import pandas as pd
import os
import urllib
warnings.filterwarnings("ignore")


def photo_url_extraction_fn(row):
    """ extract the seven photograph urls.

         :param row: pandas dataframe row value object.
         :return photo_url_list: list object containing the site names and urls of all site photographs stores in
         odk aggregate."""

    property_name = str(row["property"])
    date = str(row["date_rec"])
    weed_bot = (str(row["weed_bot"]))
    feature = str(row["feature"])
    uid = str(row["uid"])
    photo1 = str(row["photo1"])
    photo2 = str(row["photo2"])
    photo3 = str(row["photo3"])

    photo_url_list = [photo1, photo2, photo3]

    return photo_url_list, property_name, date, feature, uid, weed_bot


def date_label_fn(date):
    """

    :param date:
    :return:
    """
    year, month, day = date.split("-")
    if len(day) == 1:
        final_day = str(0) + str(day)
    else:
        final_day = str(day)

    if len(month) == 1:
        final_month = str(0) + str(month)
    else:
        final_month = str(month)

    date_label = str(year) + str(final_month + str(final_day))

    return date_label


def save_photo_fn(photo_url_list, property_name, date, feature, uid, export_dir_path, weed_botanical):
    """Download and save the seven transect photos_fn."""

    photo_dir_list = []

    date_label = date_label_fn(date)

    photo_label_list = []
    n = 1
    for i in photo_url_list:
        photo_num = n

        if i != "BLANK":
            output_str = "{0}\\{1}_{2}_{3}_uid{4}_photo{5}.jpg".format(export_dir_path,
                                                                       property_name.replace(" ", "_").title(),
                                                                       date_label, weed_botanical.replace(" ", "_"),
                                                                       str(uid), str(photo_num))

            photo_label_list.append(output_str)
            urllib.request.urlretrieve(i, output_str)
        else:
            photo_label_list.append(i)
    n += 1
    return photo_label_list


def main_routine(export_dir, feature_name):
    """ Extract the site photo urls.

        :param feature_name:
        :param export_dir:
        :param row: pandas dataframe row value object.
        :param site: pandas dataframe row value object.
        :return photo_url_list: list object containing the site names and urls of all site photographs stores in
             odk aggregate."""


    export_dir_path = "{0}\\photos".format(export_dir)
    for file in glob("{0}\\csv\\*photo.csv".format(export_dir)):

        if file:

            feature_df = pd.read_csv(file)

            if len(feature_df.index) != 0:

                feature_df.fillna("BLANK", inplace=True)
                total_photo_list = []
                for index, row in feature_df.iterrows():
                    # call photos function
                    photo_url_list, property_name, date, feature, uid, weed_botanical = photo_url_extraction_fn(row)

                    photo_label_list = save_photo_fn(photo_url_list, property_name, date, feature, uid, export_dir_path,
                                                     weed_botanical)

                    total_photo_list.append(photo_label_list)

    else:
        pass


if __name__ == "__main__":
    main_routine()
