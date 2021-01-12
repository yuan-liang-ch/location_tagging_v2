
__all__ = ["logger", "GeoService", "SummaryParser"]

import os
import json
import requests
import logging
import functools
import collections

from .utils import pprint, timeit
from .loader import get_same_name_locations_dataframe

logger = logging.getLogger()
logger.setLevel("INFO")

class GeoService(object):

    def __init__(self):
        self.wiki_page_id_field = "wikipediaPageId"
        self.invalid_page_id = -1
        self.keep_google_entities_type = ["LOCATION", "PERSON", "ORGANIZATION"]

        # timeout
        self.connect_time_out = 1
        self.read_time_out = 5

        # feature
        self.feature_rule_salience = 0.01

        # service parameter
        self.locale = "en_US"
        self.countryCode = "US"
        
    def request_location_summaries_wikiURL(self, wiki_url):
        """
        Return the goecoding information.
        :param google_entities: List of List of String. 
        :return: 
        """
        if wiki_url == "" or wiki_url is None:
            raise Exception("Wikipedia URL should be provided.")
        request_url = os.path.join(
            os.environ["snLocationPlatformEndpoint"], os.environ["wikipedia_field"])

        proxy_kwargs = {"proxies": dict(http="socks5h://localhost:1080", https="socks5h://localhost:1080")} \
            if os.environ.get("use_proxy", "") == "true" else {}
        res = requests.get(request_url,
                            params={"url": wiki_url, "locale": self.locale},
                            headers={"Accept": "application/json"},
                            timeout=(self.connect_time_out,
                                    self.read_time_out),
                            **proxy_kwargs)

        if res.status_code == 200:
            return res.json()
        else:
            pprint(
                res.json(), "Something went wrong when requesting sn-location geocoding service.")
            return []

    def request_location_summary_id(self, location_id=""):
        if location_id == "":
            raise Exception("location_id should be provided.")
        base_url = os.path.join(
            os.environ["snLocationPlatformEndpoint"], os.environ["locationID_field"])
        proxy_kwargs = {"proxies": dict(http="socks5h://localhost:1080", https="socks5h://localhost:1080")} \
            if os.environ.get("use_proxy", "") == "true" else {}
        res = requests.get(base_url,
                            params={"id": location_id, "locale": self.locale},
                            headers={"Accept": "*/*"},
                            timeout=(self.connect_time_out, self.read_time_out), **proxy_kwargs)
        if res and res.status_code == 200:
            return res.json()
        else:
            return []

    def request_location_summaries_keyword(self, keyword):
        if keyword == "":
            raise Exception("Keyword should be provided.")
        base_url = os.environ["snLocationPlatformEndpoint"] + os.environ["keyword_field"]
        proxy_kwargs = {"proxies": dict(http="socks5h://localhost:1080", https="socks5h://localhost:1080")} \
            if os.environ.get("use_proxy", "") == "true" else {}
        res = requests.get(base_url,
                           params={"keyword": keyword, "countryCode": self.countryCode, "method": "EXACT_MATCH", "locale": self.locale},
                           headers={"Accept": "*/*"},
                           timeout=(self.connect_time_out, self.read_time_out), **proxy_kwargs)
        if res and res.status_code == 200:
            return res.json()
        else:
            pprint({"keyword": keyword},
                       "Location summary not found")
            return []

    def request_location_summaries_keyword_bulk(self, keywords):
        if keywords == []:
            return []
        base_url = os.environ["snLocationPlatformEndpoint"] + os.environ["keyword_bulk_field"]
        proxy_kwargs = {"proxies": dict(http="socks5h://localhost:1080", https="socks5h://localhost:1080")} \
            if os.environ.get("use_proxy", "") == "true" else {}
        params = {"requests": []}
        for keyword in keywords:
            params["requests"].append({"keyword": keyword, "countryCode": self.countryCode, "method": "EXACT_MATCH", "locale": self.locale, "limit": 10})
        res = requests.post(base_url,
                           json=params,
                           headers={"Accept": "*/*"},
                           timeout=(self.connect_time_out, self.read_time_out), **proxy_kwargs)
        if res and res.status_code == 200:
            return res.json()["responses"]
        else:
            return []
        

class SummaryParser(object):

    def __init__(self):
        self.same_name_df = get_same_name_locations_dataframe()
        self.same_name_df.admin_area = self.same_name_df.admin_area.fillna(
            "").apply(lambda x: x.lower())

    def get_specific_location_from_summary(self, loc, location_type, lower=True):
        specific_area = None
        loc_type = loc.get("locationType", None)
        if loc_type == location_type:
            specific_area = loc["locationName"]
        else:
            address_components = loc.get("addressComponents", [])
            specific_areas = [
                e for e in address_components if e["locationType"] == location_type]
            if specific_areas:
                specific_area = specific_areas[0]["locationName"]

        if specific_area and lower:
            specific_area = specific_area.lower()
        return specific_area

    def get_admin_area_stats(self, locations, waiting_for_disambugious, verbose=False, lower=True):
        state_stats = {}
        for loc in locations:
            salience = loc.get("salience", 0)
            state = self.get_specific_location_from_summary(
                    loc, "ADMIN_AREA", lower=lower)
            if state is None:
                continue
            if loc.get("locationType") != "ADMIN_AREA":
                if loc["source"] == "GoogleEntity":
                    weight = 2.5
                elif loc.get("locationName", "") + ":" + loc.get("locationType", "") not in waiting_for_disambugious:
                    weight = 2.5
                else:
                    weight = 1
            else:
                weight = 5
            if state not in state_stats:
                state_stats[state] = {"count": 0, "salience": 0}
            state_stats[state]["count"] += weight
            state_stats[state]["salience"] += salience
        return state_stats

    def get_admin_area_candidates(self,
                                  nlp_retrieval_obj,
                                  publisher_retrieval_obj,
                                  url_retrieval_obj,
                                  locations,
                                  waiting_for_disambugious,
                                  text,
                                  url,
                                  verbose=False):
        # article url
        url_admin_area = url_retrieval_obj.get_admin_area_by_url(url)
        # resolve publisher and get publisher's admin_area if available
        pub_admin_areas = publisher_retrieval_obj.get_admin_area_by_pub(url)
        # check text
        text_admin_areas = nlp_retrieval_obj.get_admin_area_by_text(text)
        # Tabulate google's guessed admin areas
        admin_area_stats = self.get_admin_area_stats( 
            locations, waiting_for_disambugious, verbose=verbose)

        sources = {"url": url_admin_area,
                   "publisher": pub_admin_areas,
                   "text": text_admin_areas,
                   "stat": admin_area_stats}

        merged = {}
        for source, areas in sources.items():
            for area, props in areas.items():
                if area not in merged: merged[area] = {}
                merged[area][source] = props

        # some blackboard space
        for k in merged:
            merged[k]["_features"] = dict()

        result = {**sources, "merged": merged}

        if verbose:
            pprint("Admin candidates: ", result)

        return result

    def get_admin_candidates_for_loc(self, loc, admin_candidates):

        # the admin area guessed by Google
        ge_admin_area = self.get_specific_location_from_summary(loc, "ADMIN_AREA")
        if ge_admin_area is not None:
            possible_admins = {ge_admin_area: {"google": dict()}}
        else:
            possible_admins = {}

        # Find other locations with the same name
        names = [loc["locationName"], loc["locationName"].replace(" County", "")]

        same_name = (self.same_name_df["name"].isin(names))
        different_location_ids = (self.same_name_df["id"] != loc["locationId"])
        df_same_name_locs = self.same_name_df[same_name & different_location_ids]

        # Get candidate admins w/ a location that has the same name
        for admin, sources in admin_candidates["merged"].items():
            if admin in df_same_name_locs.admin_area.values or admin in possible_admins:
                possible_admins[admin] = {
                    **possible_admins.get(admin, dict()), **sources}

        return possible_admins, df_same_name_locs.to_dict("records")

    def reconstruct_summary(self, summary):
        addressComponents = summary["addressComponents"]
        reconstruct_version = addressComponents[0]
        addressComponents.remove(reconstruct_version)
        reconstruct_version["addressComponents"] = addressComponents
        return reconstruct_version
