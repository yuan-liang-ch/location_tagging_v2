
__all__ = ["logger", "Retrieval", "NlpRetrieval",
           "PublisherRetrieval", "FeatureRetrieval", "UrlRetrieval"]

import os
import re
import json
import logging
import itertools
import collections
from collections import defaultdict

import pandas as pd
from urllib.parse import urlparse
from flashtext.keyword import KeywordProcessor

from .loader import (
    query_location_master_db,
    get_publisher_location_dataframe,
    get_us_states_shorthand,
    get_same_name_locations_dataframe,
    load_wiki_entities,
    load_poi_info
)
from .utils import *
from .geocoding_service import GeoService, SummaryParser


logger = logging.getLogger()
logger.setLevel("INFO")


class Retrieval(object):

    def __init__(self):
        self.df_states, self.states_full, self.states_short, self.states = get_us_states_shorthand()
        self.full_2_short = {}
        self.short_2_full = {}
        for _, item in self.df_states.iterrows():
            if item["name"] not in self.full_2_short:
                self.full_2_short[item["name"]] = []
            if item["short_name"] not in self.short_2_full:
                self.short_2_full[item["short_name"]] = []
            self.full_2_short[item["name"]].append(item["short_name"])
            self.short_2_full[item["short_name"]].append(item["name"])
        self.geo_services = GeoService()
        self.summary_parser = SummaryParser()

    def get_state_full_name_for_maybe_short(self, surface_form):
        if surface_form in self.states_full:
            return [surface_form]
        if surface_form in self.states_short:
            state = self.short_2_full[surface_form]
            return [x.replace("-", " ") for x in state]
        # possibly a postal abbreviation plus period
        if len(surface_form) == 3 and surface_form.endswith("."):
            return self.get_state_full_name_for_maybe_short(surface_form[:2])
        return None

    def retrieval_filter_locations(self, feature_locations, publisher_locations, google_locations, text_locations):
        no_need_disambigious = []
        waiting_for_disambigious = []
        loc_name = {}
        loc_dic = defaultdict(set)
        for loc in publisher_locations:
            if loc['locationName'] not in loc_name:
                no_need_disambigious.append(loc)
                loc_name[loc["locationName"]] = 1
        for loc in feature_locations:
            if loc['locationName'] not in loc_name:
                no_need_disambigious.append(loc)
                loc_name[loc["locationName"]] = 1
        for loc in google_locations:
            if loc['locationName'] not in loc_name:
                no_need_disambigious.append(loc)
                loc_name[loc['locationName']] = 1
        for loc in text_locations:
            if loc["locationName"] not in loc_name:
                if loc["locationType"] in ["LOCALITY", "SUB_ADMIN_AREA"]:
                    waiting_for_disambigious.append(loc)
                    address_components = loc["addressComponents"]
                    for item in address_components:
                        if item["locationType"] == "ADMIN_AREA":
                            loc_dic[f"{loc['locationName']}:{loc['locationType']}"].add(item["locationName"])
                        else:
                            continue
                else:
                    no_need_disambigious.append(loc)
        need_disambigious = []
        for loc in waiting_for_disambigious:
            if len(loc_dic[f"{loc['locationName']}:{loc['locationType']}"]) >= 2:
                need_disambigious.append(loc)
            else:
                del loc_dic[f"{loc['locationName']}:{loc['locationType']}"]
                no_need_disambigious.append(loc)
        return no_need_disambigious, need_disambigious, loc_dic


class NlpRetrieval(Retrieval):

    def __init__(self):
        super().__init__()
        self.wiki_entities = load_wiki_entities()
        self.poi_entities_info, self.poi_entities = load_poi_info()
        self.keyword_processor = KeywordProcessor(case_sensitive=True)
        self.keyword_processor.add_keywords_from_list(self.wiki_entities)

    def get_text_entity_names(self, title, text):
        keyword_found = list(set(self.keyword_processor.extract_keywords(title + ". " + text)))
        return keyword_found

    def get_location_candidates(self, location_names):
        summaries = []
        for loc_name in location_names:
            cur_summary = self.geo_services.request_location_summaries_keyword(
                loc_name)
            if cur_summary == []: continue
            for item in cur_summary:
                if item["addressComponents"] == []: continue
                reconstruct_version = self.summary_parser.reconstruct_summary(item)
                reconstruct_version["salience"] = 0
                reconstruct_version["source"] = "WikiMatch"
                summaries.append(reconstruct_version)
        return summaries

    def get_location_candidates_bulk(self, location_names):
        final_summaries = []
        summaries = self.geo_services.request_location_summaries_keyword_bulk(location_names)
        for summary in summaries:
            cur_summary = summary["locations"]
            if cur_summary == []: continue
            for item in cur_summary:
                if item["addressComponents"] == []: continue
                reconstruct_version = self.summary_parser.reconstruct_summary(item)
                reconstruct_version["salience"] = 0
                reconstruct_version["source"] = "WikiMatch"
                final_summaries.append(reconstruct_version)
        return final_summaries
        
    def get_locations_via_google_entities(self, google_entities):
        summaries = []
        for entity in google_entities:
            wiki_url = entity.get("wikiURL", None)
            if entity.get("type", "") not in ["LOCATION", "PERSON", "ORGANIZATION"] or wiki_url is None or "en.wikipedia.org" not in wiki_url:
                continue
            try:
                cur_summary = self.geo_services.request_location_summaries_wikiURL(wiki_url)
            except:
                cur_summary = []
            if cur_summary == []:
                continue
            for item in cur_summary:
                if item["addressComponents"] == []: continue
                reconstruct_version = self.summary_parser.reconstruct_summary(item)
                reconstruct_version["salience"] = entity.get("salience", 0)
                reconstruct_version["source"] = "GoogleEntity"
                summaries.append(reconstruct_version)
        return summaries
    
    def get_locations(self, event):
        text_location_names = self.get_text_entity_names(event.get("slimTitle", ""), event.get("body", ""))
        google_locations = self.get_locations_via_google_entities(
            event.get("googleEntities", []))
        text_locations = self.get_location_candidates_bulk(
            text_location_names)
        return google_locations, text_locations

    def get_admin_area_by_text(self, text, location=None, max_len=50, lower=True):
        patterns = [
            ("loc1, loc2, pub", "([a-z ]*),? ([a-z ]+)\.? *\((.*)\)"),
        ]
        text = text.replace("—", "-")
        if lower:
            text = text.lower()
        words = re.split(r"\s", text)

        first_paragraph = " ".join(words[:max_len])
        matches_found = []

        # first paragraph
        for _, p in patterns:
            match = re.findall(p, first_paragraph)
            if match:
                for item in match:
                    tokens = item[:2]
                    _, loc2 = tokens
                    loc2 = loc2.replace(" ", "-")
                    full_name = self.get_state_full_name_for_maybe_short(loc2)
                    if full_name is not None:
                        for state in full_name:
                            matches_found.append(
                                (state, {"place": "Beginning"}))

        # name occurrences (except postal abbreviations)
        states = [s.replace("-", " ") for s in self.states]
        minimal_freq = 3
        for s in states:
            if s not in words: continue
            freq = words.count(s)
            min_index = words.index(s)
            if freq >= minimal_freq and min_index < 200:
                full_states = self.get_state_full_name_for_maybe_short(s)
                for state in full_states:
                    matches_found.append((state, {"place": "Body", "count": freq}))

        result = {}
        for admin, matches in itertools.groupby(matches_found, lambda m: m[0]):
            props = [m[1] for m in matches]
            result_props = {"place": [p["place"] for p in props],
                            "count": sum(p.get("count", 1) for p in props)}
            result[admin] = result_props

        return result


class PublisherRetrieval(Retrieval):

    def __init__(self):
        super().__init__()
        self.df_pub_loc = get_publisher_location_dataframe()

    def get_host(self, url, get_minimal_form=False):
        """Return the host given the url

        Parameters
        ----------
        url : str
        get_minimal_form: bool

        Returns
        -------
        host : str
        """
        o = urlparse(url)
        host = o.netloc
        if get_minimal_form:
            host = host.replace("www.", "").lower()
        return host

    def match_url_to_pub_domain(self, url):
        host = self.get_host(url, get_minimal_form=True)
        path_start = url.lower().index(host) + len(host)
        path_tokens = [t for t in url[path_start:].split("/") if len(t) != 0]
        result = None
        key_tokens = []
        for token in [host, *path_tokens]:
            key_tokens.append(token)
            key = "/".join(key_tokens)
            if key in self.df_pub_loc.domain.values:
                result = key
        return result

    def get_admin_area_by_pub(self, url):
        if url == "": return dict()
        publisher = self.match_url_to_pub_domain(url)
        publisher_states = self.df_pub_loc[self.df_pub_loc.domain ==
                                           publisher].admin_area.values
        return {s.lower(): dict() for s in publisher_states}

    def get_locations_via_publisher(self, url):
        if url == "": return []
        host = self.get_host(url)
        logger.info("Checking related locations for host:" + host)
        sql = f"""
            SELECT
                location_id,
                admin_area_id
            FROM local_publisher as t_l
            INNER JOIN publisher_location as t_p
            ON t_l.id = t_p.local_publisher_id
            WHERE t_l.host = '{host}'
                AND t_l.status = 'ACCEPTED'
        """

        records = query_location_master_db(sql)  # a list of dict
        locations = []

        for record in records:
            location_id = record["location_id"]
            summary = self.geo_services.request_location_summary_id(location_id=location_id)
            if summary == [] or summary["addressComponents"] == []: continue
            reconstruct_version = self.summary_parser.reconstruct_summary(summary)
            reconstruct_version["salience"] = 0
            reconstruct_version["source"] = "LocalPublisher"
            locations.append(reconstruct_version)

        #pprint(locations, "locations associated with publisher")
        return locations


class FeatureRetrieval(Retrieval):

    def __init__(self):
        super().__init__()
        self.predefined_features = self.get_location_features()
        self.include_feature_records = [
            f for f in self.predefined_features if f["condition_type"] == "INCLUDE"]
        self.exclude_feature_names = [
            f["feature"] for f in self.predefined_features if f["condition_type"] == "EXCLUDE"]

    def get_location_features(self):
        """Return a list of location-feature records

        Returns
        -------
        location_features : list of dicts
            Each dict represent a predefined features for tagging locations:
            {
                "id": int,
                "location_id": int,
                "feature": str,
                "condition_type": str
            }
        """
        sql = "SELECT * FROM location_feature"
        location_features = query_location_master_db(sql)
        return location_features

    def get_locations_via_features(self, features):
        locations = []
        include_location_ids = [f["location_id"]
                                for f in self.include_feature_records if f["feature"] in features]
        if include_location_ids:
            if not [f for f in features if f in self.exclude_feature_names]:
                for location_id in include_location_ids:
                    summary = self.geo_services.request_location_summary_id(
                        location_id=location_id)
                    if summary == [] or summary["addressComponents"] == []: continue
                    reconstruct_version = self.summary_parser.reconstruct_summary(summary)
                    reconstruct_version["salience"] = 0
                    reconstruct_version["source"] = "Features"
                    locations.append(reconstruct_version)
        pprint(locations, "locations associated with features")
        return locations


class UrlRetrieval(Retrieval):

    def __init__(self):
        super().__init__()

    def get_admin_area_by_url(self, url, lower=True):
        if url == "": return dict()
        state = None

        tokens = url.lower().replace("_", "-").replace("-", "/").split("/")
        for i, t in enumerate(tokens):
            if t in self.states_full:
                # check next token to make sure it's not refering some county or city
                if i != len(tokens) - 1:
                    next_t = tokens[i + 1]
                    if not next_t in ["city", "county"]:
                        state = t

                break  # use first occurence as prediction

        if state:
            state = state.replace("-", " ")
            if lower:
                state = state.lower()
            return {state: dict()}
        return dict()
