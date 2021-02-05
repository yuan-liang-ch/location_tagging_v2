#coding:utf-8
import argparse
import logging
import sys
import pickle
import json
import time
import traceback
import os
import yaml
from collections import defaultdict, namedtuple
from logging import debug

import pandas as pd
from snpytools import *

from src.config import Meta_config
from src.geocoding_service import SummaryParser
from src.us_location_tagger import main

logging.basicConfig(level=logging.INFO)

# read config file
stream = open("./src/config.yaml", "r")
configs = yaml.load(stream)
Meta_config.setup_local_test_env(configs)


def detect_int(x):
    try:
        int(x)
        return True
    except:
        return False

def process_entity(entity_str):
    if entity_str == "" or detect_int(entity_str): return []
    segs = entity_str.split("]],")
    result = []
    for s in segs:
        try:
            s = s.replace("[", "")
            items = s.split(", ")
            dic = {}
            dic["name"] = str(items[0]).replace("'", "")
            dic["salience"] = float(items[1])
            dic["wikiURL"] = str(items[2]).replace("'", "") if items[2] != "None" else None
            dic["mid"] = str(items[3]).replace("'", "") if items[3] != "None" else None
            dic["type"] = str(items[4]).replace("'", "")
            result.append(dic)
        except:
            continue
    return result

def process_entity_list(entity_list):
    result = []
    if entity_list is None: return []
    for item in entity_list:
        dic = {}
        dic["name"] = str(item[0]).replace("'", "") 
        dic["salience"] = float(item[1]) if item[1] is not None else None
        dic["wikiURL"] = str(item[2]).replace("'", "") if item[2] is not None else None
        dic["mid"] = str(item[3]).replace("'", "") if item[3] is not None else None
        dic["type"] = str(item[4]).replace("'", "")
        result.append(dic)
    return result

def get_data():
    data = df_from_presto("select DISTINCT a.id as sequence, a.url, a.locations, a.features, a.googleEntities, b.slimTitle, b.body \
        from \
        (select DISTINCT id, analysis.locations as locations, analysis.tags as features, analysis.googleentities as googleEntities, resolvedurl as url \
        from hive.default.unified_article_metadata_updates \
        where analysis.language = 'EN' and discoverytimestamp >= 1606780800 and discoverytimestamp <= 1606867200) a \
        JOIN \
        (select DISTINCT structure.slimtitle as slimTitle, structure.textcontent as body, resolvedurl as url \
        from hive_ad.smartnews.raw_url_crawls_orc \
         where lang = 'EN' and dt >= '2020-12-01') b \
        ON a.url = b.url"
    )
    return data

def parse_result_to_martin_request(locations):
    cities = []
    counties = []
    states = []
    for loc in locations:
        if loc == []:
            cities.append("")
            counties.append("")
            states.append("")
        else:
            loc = loc[0]
            loc_name = loc["name"]
            loc_type = loc["locationType"]
            if loc_type == "LOCALITY":
                cities.append(loc_name)
                flag_1, flag_2 = 0, 0
                for item in loc["addressComponents"]:
                    if item["locationType"] == "ADMIN_AREA":
                        states.append(item["name"])
                        flag_1 = 1
                    if item["locationType"] == "SUB_ADMIN_AREA":
                        counties.append(item["name"])
                        flag_2 = 1
                if flag_1 == 0: states.append("")
                if flag_2 == 0: counties.append("")
            elif loc_type == "SUB_ADMIN_AREA":
                cities.append("")
                flag_1 = 0
                for item in loc["addressComponents"]:
                    if item["locationType"] == "ADMIN_AREA":
                        states.append(item["name"])
                        flag_1 = 1
                        break
                if flag_1 == 0: states.append("")
                counties.append(loc_name)
            elif loc_type == "ADMIN_AREA":
                cities.append("")
                counties.append("")
                states.append(loc_name)
    return cities, counties, states

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--example_path', default='./LT2.tsv'
    )
    parser.add_argument(
        '--state_code_listing', default='./LT/_DATA/state_code_listing.txt'
    )
    parser.add_argument(
        '--wiki_loc_names', default='./LT/_DATA/wiki_loc_names_1+2.txt'
    )
    parser.add_argument(
        '--text_mapper', default='./LT/_CORPUS/ID_FULLTEXT.tsv'
    )
    parser.add_argument(
        '--filename_gold', default='./LT/_CORPUS/GROUND_TRUTH.tsv'
    )
    parser.add_argument(
        '--task', default=1, type=int, help='0:get_candidates_only, 1:get_candidate_FVPs'
    )
    parser.add_argument(
        '--city_only', default=False,
    )
    parser.add_argument(
        '--henry_feature', default=True,
    )
    parser.add_argument(
        '--wendy_feature', default=False
    )
    args = parser.parse_args()
    # id_mapping = readFullText(args.text_mapper)
    # data_gen = iter(dataloader(args.example_path, id_mapping, debug=False))
    # line = next(data_gen)
    sequence_ids = []
    candidates = []
    google_has_result = 0
    text_has_result = 0
    final_has_result = 0

    count_v2 = 0
    count_v1 = 0
    count_local_v1 = 0
    count_local_v2 = 0
    count_local = 0
    #data = get_data()
    data = pd.read_csv("/home/yuan.liang/location_tagging/location_tagging_v1_no_result_article_samples.tsv", sep="\t", header=0)
    data["slimTitle"] = data["title"]
    
    data.drop_duplicates(subset=["sequence"], inplace=True)
    print("Total base is %d" % (len(data)))
    
    location_result = [] 
    for _, row in data.iterrows():
        #row["googleEntities"] = process_entity_list(row["googleEntities"])
        try:
            row["googleEntities"] = process_entity(row["google_entities"])
        except:
            row["googleEntities"] = []
            print(row["google_entities"])
        locations = main(row)["locations"]
        location_result.append(locations)
        #location_result.append(json.dumps(locations, default=str))
        #if locations != []: count_v2 += 1
        #if row["locations"] is not None: count_v1 += 1
        #if 'en_us_local_domains' in row["features"] or 'en_us_local_domains_vip' in row["features"]:
        #    count_local += 1
        #    if locations != []: count_local_v2 += 1
        #    if row["locations"] is not None: count_local_v1 += 1
    cities, counties, states = parse_result_to_martin_request(location_result)
    data["city"] = cities
    data["county"] = counties
    data["state"] = states
    #data["location_tagging_v2"] = location_result
    data.to_csv("/home/yuan.liang/location_tagging/location_tagging_v1_no_result_article_samples_v2_result.tsv", index=False)
    #print(count_v1)
    #print(count_v2)
    #print(count_local)
    #print(count_local_v1)
    #print(count_local_v2)

    #data = pd.read_csv("location_taggging_v2_391_no_candidates.tsv", sep="\t", header=0)
    #data["sequence"] = data["link_id"]
    #data.drop_duplicates(subset=["sequence"], inplace=True)
    #for _, row in data.iterrows():
    #    row["googleEntities"] = process_entity(row["google_entities"])
    #    locations = main(row)
        #if google_locations != []: google_has_result += 1
        #if text_locations != []: text_has_result += 1
        #if locations != []: final_has_result += 1
    #    sequence = row["sequence"]
    #    pickle.dump(locations, open(f"./outputs/{sequence}.pkl", "wb"))
        #sequence_ids.append(row["sequence"])
        #candidates.append(pickle.dumps(locations))
        #sequence_ids.extend([row["sequence"]] * len(locations))
        #for item in locations:
        #    candidates.append(json.dumps(item))
    #print(f"google has {google_has_result} results.")
    #print(f"text has {text_has_result} results.")
    #print(f"finally has {final_has_result} results.")
    #output = pd.DataFrame({"sequence": sequence_ids, "candidates": candidates})
    #output.to_csv("location_tagging_candidates_20201228.csv", index=False, sep="\t")

    # while line:
    #     cur_line_num += 1
    #     if cur_line_num % 100 == 0:
    #         print("cur_line_num:", cur_line_num)
    #     try:
    #         begin_time = time.time()
    #         line["slimTitle"]
    #         line["body"]
    #         line["url"]
    #         locations = main(line)
    #         end_time = time.time()
    #         cost += (end_time - begin_time)
    #         if len(locations) != 0: active_num += 1
    #         sequence_ids.extend([line["sequence"]] * len(locations))
    #         for item in locations:
    #             candidates.append(json.dumps(item))
    #     except Exception as err:
    #         try:
    #             print(line["sequence"])
    #         except:
    #             print(cur_line_num)
    #         err_num += 1
    #     try:
    #         line = next(data_gen)
    #     except:
    #         break
    # print(f"err num is {err_num}")
    # print(f"Cost {cost} seconds.")
    #output = pd.DataFrame({"sequence": sequence_ids, "candidates": candidates})
    #output.to_csv("location_tagging_candidates_20201207.csv", index=False, sep="\t")
