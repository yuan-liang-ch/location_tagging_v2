#coding:utf-8

__all__ = ["upload_to_s3", "download_s3_file", "get_connection_to_location_master_db",
           "query_location_master_db", "get_publisher_location_dataframe", "get_us_states_shorthand",
           "get_same_name_locations_dataframe"]

import os
import re
import json
import boto3
import logging

import pymysql
import pandas as pd
from botocore.client import Config
from config import Meta_config

from utils import *


PUBLISHER_LOCATION_FILE = "publisher_location_mapping.tsv"
SAME_NAME_LOCATIONS_FILE= "us_same_name_locations.tsv"


logger = logging.getLogger()
logger.setLevel("INFO")


@timeit
def upload_to_s3(bucket, folder, local_file_path):
    config = Config(
        connect_timeout=1,
        read_timeout=3,
        retries=dict(
            max_attempts=3
        )
    )

    s3 = boto3.client('s3', config=config)
    data = open(local_file_path, "rb")
    filename = local_file_path.split("/")[-1]
    key = folder + '/' + filename
    s3.upload_fileobj(data, bucket, key)

def download_s3_file(file_name, local_file_path):
    """Helper to download s3 file named `file_name` and save to `local_file_path`.

    Parameters
    ----------
    file_name : str
    local_file_path : str

    Returns
    -------
    None
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(os.environ['S3_BUCKET'])
    bucket.download_file(file_name, local_file_path)
    logger.info(f"Downloading {file_name} from S3...")

def get_connection_to_location_master_db():
    """Return a connection to location master database

    Returns
    -------
    cursor : pymysql.cursors.DictCursor
    """
    return pymysql.connect(
        host=os.environ['locationMasterDBHost'],
        user=os.environ['locationMasterDBUser'],
        password=os.environ['locationMasterDBPassword'],
        db=os.environ['locationMasterDB'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def query_location_master_db(sql):
    """Return rows given the sql query

    Parameters
    ----------
    sql : str
        a string contain a valid sql execute statement
    Returns
    -------
    rows : list of dicts
        where each dict represent a row in the table
    """
    connection = get_connection_to_location_master_db()
    cur = connection.cursor()
    rows = None
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception as err:
        logging.error(f"Current error {err} is found.")
    finally:
        cur.close()
        connection.close()
    return rows

def get_publisher_location_dataframe():
    """Return a Pandas DataFrame including corresponding admin_area for each publisher. The data source is generated
    by the follow jupyter notebook: `go/jupyter/article2geolocation/20200122_local_publisher_location_mapping_v2.ipynb`

    Returns
    -------
    DataFrame
    """
    local_file_path = os.path.join("./", PUBLISHER_LOCATION_FILE)

    if not os.path.exists(local_file_path):
        download_s3_file(PUBLISHER_LOCATION_FILE, local_file_path)

    df = pd.read_csv(local_file_path, sep="\t")
    return df

def get_us_states_shorthand(config=Meta_config):
    """
    """
    us_states = config.us_states
    df_states = pd.DataFrame(us_states, columns=['name', 'short_name'])
    df_states['name'] = df_states['name'].apply(lambda x: x.replace(" ", "-"))
    states_full = list(set(df_states.name.values.tolist()))
    states_short = list(set(df_states.short_name.values.tolist()))
    states = states_full + states_short
    return df_states, states_full, states_short, states

def get_same_name_locations_dataframe():
    local_file_path = os.path.join("./", SAME_NAME_LOCATIONS_FILE)

    if not os.path.exists(local_file_path):
        download_s3_file(SAME_NAME_LOCATIONS_FILE, local_file_path)

    df = pd.read_csv(local_file_path, sep="\t")
    return df

def load_wiki_entities():
    wiki_entity_file_path = os.environ["wiki_entity_file_path"]

    if not os.path.exists(wiki_entity_file_path):
        logging.error("wiki entity file not exists.")

    entities = []
    with open(wiki_entity_file_path, "r", encoding="utf-8") as in_file:
        for line in in_file:
            segs = line.split("\t")
            if re.findall("[0-9]+", segs[0]): continue
            entities.append(segs[0])
            entities.append(segs[0].upper())
    return entities

def load_poi_info():
    poi_info = []
    poi_entities = []
    poi_file_path = os.environ["poi_file_path"]
    with open(poi_file_path) as in_file:
        for line in in_file:
            obj = json.loads(line)
            poi_info.append(obj)
            poi_entities.append(obj["name"])
    return poi_info, list(set(poi_entities))
