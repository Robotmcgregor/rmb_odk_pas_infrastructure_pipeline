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


def unidentified_species_fn(row, string_clean_capital_fn):
    """ Extract variables relevant to the unidentified feature.

    :param row: index object - current row of the pandas data frame that is being worked on.
    :param string_clean_capital_fn: function that cleaned string objects.
    :return species_list: list object containg multiple unidentified species variables.
    """

    sample1_yn = string_clean_capital_fn(str(row["PLANT_COLLECTING:SAMPLE_YN_GROUP:YN_FLR_SP1"]))
    sample1_label = string_clean_capital_fn(str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE1:FLORA_SAMPLE_LABEL1"]))
    sample1_photo = str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE1:PHOTO_FLORA_SAMP1"])

    sample2_yn = string_clean_capital_fn(str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE2:YN_FLR_SP2"]))
    sample2_label = string_clean_capital_fn(str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE2:FLORA_SAMPLE_LABEL2"]))
    sample2_photo = str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE2:PHOTO_FLORA_SAMP2"])

    sample3_yn = string_clean_capital_fn(str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE3:YN_FLR_SP3"]))
    sample3_label = string_clean_capital_fn(str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE3:FLORA_SAMPLE_LABEL3"]))
    sample3_photo = str(row["PLANT_COLLECTING:GROUP_SAMPLE:SAMPLE3:PHOTO_FLORA_SAMP3"])

    floristics_yn = string_clean_capital_fn(str(row["PLANT_COLLECTING:SAMPLE_YN_GROUP:ID_DETAILS_YN"]))
    note1 = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_HABIT:FLORA_NOTE1"]))
    habit = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_HABIT:GROW_HABIT"]))
    height = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_HABIT:HEIGHT"])

    annual_perennial = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_ROOTS:PERENNIAL"]))
    roots = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_ROOTS:ROOT_SYS"]))
    roots_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_ROOTS:PHOTO_FLORA_ROOTS"])

    # cork
    cork = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_CORK:BARK_TYPE"]))
    cork_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_CORK:PHOTO_FLORA_BARK"])
    cork_colour = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_CORK:BARK_BLAZE_COLOUR"]))
    cork_colour_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_CORK:PHOTO_BLAZE_COLOUR"])

    # leaf
    leaf = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FOLIAGE:LEAF_DESC"]))
    leaf_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FOLIAGE:PHOTO_FLORA_LEAF"])

    # flower
    flower_col = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FLOWER:FLW_CLR"])
    flower_colour_other = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FLOWER:FLW_CLR_OTHER"])

    if flower_col == "flower_other":
        flower_col.replace("flower_other", flower_colour_other)
    flower_colour = string_clean_capital_fn(flower_col)

    flower_colour_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FLOWER:PHOTO_FLORA_FLOWER"])
    flower_cluster_photo = string_clean_capital_fn(
        str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FLOWER:PHOTO_FLORA_FLW_CLUSTER"]))

    # grass
    grass_head = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:GRASS_HEAD:WEED_SECTION"]))
    grass_head_photo = str(row["PLANT_COLLECTING:PLANT_ID:GRASS_HEAD:PHOTO_GRASS_SEED_HEAD"])
    grass_flower_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_FLOWER:PHOTO_FLORA_FLW_CLUSTER"])
    grass_spikelet_photo = str(row["PLANT_COLLECTING:PLANT_ID:GRASS_HEAD:PHOTO_FLORA_SPIKELET"])
    grass_awn_photo = str(row["PLANT_COLLECTING:PLANT_ID:GRASS_HEAD:PHOTO_FLORA_AWN"])

    # fruit
    fruit = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:FRUIT_SEEDS:FRUIT_DESC"]))
    fruit_photo = str(row["PLANT_COLLECTING:PLANT_ID:FRUIT_SEEDS:PHOTO_FLORA_FRUIT"])
    seed_photo = str(row["PLANT_COLLECTING:PLANT_ID:FRUIT_SEEDS:PHOTO_FLORA_SEED"])

    odour_yn = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:ODOUR"]))
    odour = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:ODOUR_DESC"]))

    prickles = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:HAIR_PRICK"]))
    prickles_location = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:DESC_H_P"]))
    prickles_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:PHOTO_H_P"])

    latex = string_clean_capital_fn(str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:SAP_LATEX"]))
    latex_photo = str(row["PLANT_COLLECTING:PLANT_ID:PLANT_OTHER:PHOTO_SAP"])

    abundance = str(row["PLANT_COLLECTING:PLANT_ID:SPECIES_ABUNDANCE:SPEC_SIZE"])
    density = str(row["PLANT_COLLECTING:PLANT_ID:SPECIES_ABUNDANCE:SPECIES_DENSITY"])

    species_list = [sample1_yn, sample1_label, sample1_photo, sample2_yn, sample2_label, sample2_photo,
                    sample3_yn, sample3_label, sample3_photo, floristics_yn, note1, habit, height,
                    annual_perennial, roots, roots_photo, cork, cork_photo, cork_colour, cork_colour_photo,
                    leaf, leaf_photo, flower_colour, flower_colour_photo, flower_cluster_photo, grass_head,
                    grass_head_photo,
                    grass_flower_photo, grass_spikelet_photo, grass_awn_photo, fruit, fruit_photo, seed_photo, odour_yn,
                    odour, prickles, prickles_location, prickles_photo, latex, latex_photo, abundance,
                    density]

    return species_list


def main_routine(temp_dir, string_clean_capital_fn, feature_df, feature, date_time_fn,
                 gps_points_fn,
                 photo_url_extraction_fn, meta_data_fn, label_comment_fn, feature_group_dict, feature_dict):
    final_species_list = []
    # for loop through the mapping transect dataframe (df)
    for index, row in feature_df.iterrows():
        species_list = []
        # call the date_time_fn function to extract date and time information.

        start_date = date_time_fn(row)

        # call the gps_points_fn function to extract the longitude and latitude information.
        lat_lon_list = gps_points_fn(row)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        photo_url_list = photo_url_extraction_fn(row)

        label_comment_list = label_comment_fn(row, string_clean_capital_fn)
        # map_list = infrastructure_mapping_features(row, string_clean_capital_fn
        unidentified_species_list = unidentified_species_fn(row, string_clean_capital_fn)

        # call the meta_date_fn function to extract the unique identifier information for each form record.
        meta_data_list = meta_data_fn(row)

        species_list.append(feature)
        species_list.append(start_date)
        species_list.extend(label_comment_list)
        species_list.extend(unidentified_species_list)
        species_list.extend(photo_url_list)
        species_list.extend(lat_lon_list)
        species_list.extend(meta_data_list)

        final_species_list.append(species_list)

    unidentified_species_df = concat_list_to_df_fn(final_species_list)

    unidentified_species_df.columns = ["feature", "date_rec", "district", "property", "prop_code", "label", "condition", "comment",
                                       "sample1_yn", "sample1_label", "sample1_photo", "sample2_yn", "sample2_label",
                                       "sample2_photo",
                                       "sample3_yn", "sample3_label", "sample3_photo", "floristics_yn", "note1",
                                       "habit", "height",
                                       "annual_perennial", "roots", "roots_photo", "cork", "cork_photo", "cork_colour",
                                       "cork_colour_photo",
                                       "leaf", "leaf_photo", "flower_colour", "flower_colour_photo",
                                       "flower_cluster_photo", "grass_head", "grass_head_photo",
                                       "grass_flower_photo", "grass_spikelet_photo", "grass_awn_photo", "fruit",
                                       "fruit_photo", "seed_photo", "odour_yn",
                                       "odour", "prickles", "prickles_location", "prickles_photo", "latex",
                                       "latex_photo", "abundance",
                                       "density", "photo1", "photo2", "photo3", "in_cadast", "datum", "lat1", "lon1",
                                       "acc1", "dist1",
                                       "bear1", "meta_key", "clean_meta_key", "form_name"]

    csv_output = ("{0}\\unidentified\\clean_unidentified.csv".format(temp_dir))
    unidentified_species_df.to_csv(csv_output)


if __name__ == "__main__":
    main_routine()
