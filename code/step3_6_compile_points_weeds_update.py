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


# import modules
import warnings
warnings.filterwarnings("ignore")

def main_routine(temp_dir, feature, export_dir_path, pastoral_estate, user_df):
    """

    :param temp_dir:
    :param feature:
    :param export_dir_path:
    """

    # create a feature_name variable for directory access
    feature_name = "weeds"

    # create a list of features to subset the primary dataframe
    subset_list = ["feature", "date_rec", "district", "property", "prop_code", "in_cadast", "label", "condition",
                   "recorder", "weed_bot", "weed_comm", "weed_size", "weed_den",
                   "photo1", "photo2", "photo3", "comment", "datum", "lat1", "lon1", "acc1", "dist1", "bear1"]

    photo_subset_list = ["uid", "feature", "weed_bot", "weed_comm", "weed_size", "weed_den", "date_rec", "district", "property", "prop_code", "photo1", "photo2",
                         "photo3"]

    projected_gdf_dest_list = [
        "uid", "feature", "district", "property", "prop_code", "weed_bot", "weed_comm", "date_rec", "rec_method", "source",
        "recorder", "weed_size", "weed_den", "treatment", "seedlings", "juveniles", "adults", "seed_press",
        "past_treat", "herbicide", "year", "photo1", "photo2", "photo3", "comment", "datum", "epsg",
        "orig_easting", "orig_northing", "l_sin", "l_cos", "dest_easting", "dest_northing"]

    orig_drop_list = ["uid", "geometry", "l_sin", "l_cos", "dest_easting", "dest_northing", "datum", "epsg", "photo1",
                      "photo2", "photo3"]

    dest_drop_list = ["uid", "l_sin", "l_cos", "orig_easting", "orig_northing", "datum", "epsg", "geometry", "photo1",
                      "photo2", "photo3"]

    import step4_1_points_orig_to_destination
    export_dir = step4_1_points_orig_to_destination.main_routine(temp_dir, feature_name, export_dir_path, subset_list,
                                                                 projected_gdf_dest_list, orig_drop_list,
                                                                 dest_drop_list,
                                                                 photo_subset_list, pastoral_estate, user_df)

    import step4_2_photo_url_weeds_csv
    step4_2_photo_url_weeds_csv.main_routine(export_dir, feature_name)


if __name__ == "__main__":
    main_routine()
