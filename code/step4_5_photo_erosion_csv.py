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


def photo_url_extraction_fn(row, feature_name):
    """ extract the seven photograph urls.

         :param row: pandas dataframe row value object.
         :param site: string object containing the cleaned site name.
         :return photo_url_list: list object containing the site names and urls of all site photographs stores in
         odk aggregate."""

    if feature_name == "infra_lines":
        uids = "uid_feature"
    else:
        uids = "uid"

    property_name = str(row["property"])
    date = str(row["date_rec"])
    feature = str(row["feature"])
    erosion_type = str(row["ero_type"])
    uid = str(row[uids])
    photo1 = str(row["photo1"])
    photo2 = str(row["photo2"])
    photo3 = str(row["photo3"])

    photo_url_list = [photo1, photo2, photo3]

    return photo_url_list, property_name, date, feature, uid, erosion_type


def date_label_fn(date):
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


def save_photo_fn(photo_url_list, property_name, date, feature, uid, export_dir_path, erosion_type):
    """Download and save the seven transect photos_fn."""

    photo_dir_list = []

    date_label = date_label_fn(date)

    """day, month, year = date.split("/")
    if len(day) == 1:
        final_day = str(0) + str(day)
    else:
        final_day = str(day)

    if len(month) == 1:
        final_month = str(0) + str(month)
    else:
        final_month = str(month)

    date_label = str(year) + str(final_month + str(final_day))"""

    photo_label_list = []
    n = 1
    for i in photo_url_list:
        photo_num = (n)

        if i != "BLANK":
            """output_str = (export_dir_path + "\\" + property_name + "_" +
                          date_label + "_" + feature + "_uid" + str(uid) + "_photo" + str(photo_num) + ".jpg")"""

            output_str = ("{0}\\{1}_{2}_{3}_{4}_uid{5}_photo{6}.jpg".format(export_dir_path, property_name, date_label,
                                                                        feature, erosion_type, str(uid), str(photo_num)))

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
                    photo_url_list, property_name, date, feature, uid, erosion_type = photo_url_extraction_fn(row, feature_name)

                    photo_label_list = save_photo_fn(photo_url_list, property_name, date, feature, uid, export_dir_path, erosion_type)

                    total_photo_list.append(photo_label_list)

    else:
        pass


if __name__ == "__main__":
    main_routine()
