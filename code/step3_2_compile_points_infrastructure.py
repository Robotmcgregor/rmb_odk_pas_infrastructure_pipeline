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
    feature_name = "infra_points"

    # create a list of features to subset the primary dataframe
    subset_list = []

    photo_subset_list = ["uid", "feature", "date_rec", "district", "property", "prop_code", "photo1", "photo2", "photo3"]

    projected_gdf_dest_list = ["uid", "feature_group", "feature", "label", "date_rec", "date_curr", "district", "property",
                               "prop_code",
                               "source",
                               "display", "condition",  "comment", "datum", "epsg",
                               "orig_easting", "orig_northing", "l_sin", "l_cos", "dest_easting", "dest_northing"] # "length_m", "area_km2",

    orig_drop_list = ["uid", "geometry", "l_sin", "l_cos", "epsg", "datum", "dest_easting",
                      "dest_northing"]

    dest_drop_list = ["uid", "geometry", "l_sin", "l_cos", "epsg", "datum", "orig_easting",
                      "orig_northing"]

    import step4_1_points_orig_to_destination
    export_dir = step4_1_points_orig_to_destination.main_routine(temp_dir, feature_name, export_dir_path, subset_list,
                                                                 projected_gdf_dest_list, orig_drop_list,
                                                                 dest_drop_list,
                                                                 photo_subset_list, pastoral_estate, user_df)

    import step4_2_photo_url_csv
    step4_2_photo_url_csv.main_routine(export_dir, feature_name)


if __name__ == "__main__":
    main_routine()
