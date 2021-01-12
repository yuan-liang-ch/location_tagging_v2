
__ALL__ = ["logger", "LocationTagger", "main"]

import os
import json
import yaml
import logging

from .loader import *
from .utils import *
from .location_retrieval import *
from .location_disambiguation import Disambiguation
from .location_classifier import LocationClassifier
from .geocoding_service import SummaryParser
from .config import Meta_config
from .PlacelineTagger import PlacelineTagger
from .FeatureExtractor import FeatureExtractor


logger = logging.getLogger()
logger.setLevel("INFO")

# read config file
yaml_stream = open("./src/config.yaml", "r")
configs = yaml.load(yaml_stream)
Meta_config.setup_local_test_env(configs)


@singleton
class LocationTagger(object):

    def __init__(self):

        self.placelineTagger = PlacelineTagger(
            configs["state_code_file"], configs["wiki_loc_file"])

        # retrieval module
        self.nlp_retrieval_obj = NlpRetrieval()
        self.publisher_retrieval_obj = PublisherRetrieval()
        self.feature_retrieval_obj = FeatureRetrieval()
        self.url_retrieval_obj = UrlRetrieval()

        # Disambiguation module
        self.summary_parser = SummaryParser()
        self.disambiguate_obj = Disambiguation(
            self.nlp_retrieval_obj.full_2_short)

        # Filter model initial
        self.location_feature_extractor = FeatureExtractor(
            self.placelineTagger)
        self.lc_filter = LocationClassifier(configs["filter_model_path"])

    @timeit
    def tag(self, event, context=None, mode="predict"):
        """Entry point for handling location analysis request from SE-AA for a specific article

        Parameters
        ----------
        event : dict
            Meta-data about a article from SE-AA. Expected format:
            {
                "sequence": int,
                "url": str,
                "slimTitle": str,
                "body": str,
                "features": [str1, str2],
                "googleEntities": [
                    {
                        "name": str,
                        "salience": float,
                        "wikiURL": str | null,
                        "mid": str,
                        "type": str,
                        "googleEntityMentions": list of dicts
                    },
                    ...
                ]
            }
        context : Lambda function specific Content object

        Returns
        -------
        analysis_result : dict
            Location tagging result with normalized locations for the requested article. Expected format:
            {
                "locations": [
                    {
                        "locationId": str,
                        "name": str,
                        "locationType": str,
                        "algorithm": str,
                        "salience": float,
                        "addressComponents": list of dicts,
                        "relatedLocations": list of dicts
                    },
                    ...
                ]
            }
        """
        debug = event.get("debug", False)
        custom_features = ["us_location_tagging_ver_7.1"]

        #pprint(event, "event from se-aa")

        # retrieval locations
        feature_locations = self.feature_retrieval_obj.get_locations_via_features(
            event["features"]) if event.get("features", None) else []
        google_locations, text_locations, waiting_for_disambugious = self.nlp_retrieval_obj.get_locations(
            event)
        locations = feature_locations + google_locations + text_locations
        logger.info("[CandidateGeneration] sequence %s, Retrieval %d locations." % (event.get("sequence", ""), len(locations)))

        # Disambiguation
        admin_candidates = self.summary_parser.get_admin_area_candidates(nlp_retrieval_obj=self.nlp_retrieval_obj,
                                                                         publisher_retrieval_obj=self.publisher_retrieval_obj,
                                                                         url_retrieval_obj=self.url_retrieval_obj,
                                                                         locations=locations,
                                                                         waiting_for_disambugious=waiting_for_disambugious,
                                                                         text=event.get(
                                                                             "body", ""),
                                                                         url=event.get("url", ""))

        locations = self.disambiguate_obj.check_locations(
            event.get("url", ""), event.get(
                "body", ""), self.publisher_retrieval_obj.df_pub_loc, admin_candidates,
            locations, waiting_for_disambugious)
        logger.info("[Location Disambiguation] sequence %s, Retrieval %d locations." % (event.get("sequence", ""), len(locations)))
        #logger.info("[Location Disambiguation] sequence %s, locations are %s" % (event.get("sequence", ""), json.dumps(locations)))

        # Location Filter
        locations = self.location_feature_extractor.extractFeatures(event.get(
            "sequence", ""), event.get("slimtitle", ""), event.get("body", ""), locations, event.get("label", ""), mode)
        if mode == "train":
            return locations
        location_result = self.lc_filter.predict_doc(locations)

        output = {"locations": location_result, "features": custom_features}

        # Output data to kinesis
        write_to_stream(event.get("sequence", ""), event, output)
        return output


def main(event, context=None, mode="predict"):
    location_tagger = LocationTagger()
    return location_tagger.tag(event, context, mode)
