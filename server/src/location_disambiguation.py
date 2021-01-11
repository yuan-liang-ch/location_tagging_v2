
__all__ = ["Disambiguation", "Filter"]

import re
import logging

from utils import pprint, timeit
from location_retrieval import Retrieval
from geocoding_service import SummaryParser

from IPython import embed

logger = logging.getLogger()
logger.setLevel("INFO")


class Disambiguation(object):

    def __init__(self, full_2_short):
        self.full_2_short = full_2_short
        self.parser = SummaryParser()
        self.inclusion_testing_prefix = "InclusionTest:"

    def get_explicit_patterns(self, loc_name, admin):
        result = []
        escaped_name = loc_name.lower()
        for admin_area_alias in self.full_2_short.get(admin, []) + [admin]:
            comma_optional = '?'
            if admin_area_alias in ["or", "in"]:
                comma_optional = ''
            escaped_alias = admin_area_alias
            result.append(f'{escaped_name},{comma_optional} {escaped_alias}')
        return result

    def featurize_disambig_candidate(self, loc_name, possible_admins, admin_candidates, text, verbose=False):
        features = dict()
        lower_text = text.lower()
        loc_lower = loc_name.lower()
        # text co-occur suppport
        for admin in possible_admins:
            admin_lower = admin.lower()
            explicit_patterns = self.get_explicit_patterns(
                loc_lower, admin_lower)
            # todo: might have problem
            features[admin] = {}
            if any(re.search(p, lower_text) is not None for p in explicit_patterns):
                features[admin]["specific_mention"] = True
            merged = admin_candidates["merged"][admin_lower]
            for source, props in merged.items():
                if source == "_features": continue
                if source != "stat":
                    features[admin][f"from_{source}"] = True
                else:
                    features[admin]["stat:count"] = props["count"]

        if verbose:
            pprint(features)

        return features

    def check_locations(self, url, text, df_pub_loc, admin_candidates, locations, waiting_for_disambugious, verbose=False):
        """Disambiguate possibly ambiguous locations.

        Parameters
        ----------
        url : str
        locations : list of dicts
        text: str
        verbose : bool

        Returns
        -------
        updated_locations : list of dicts
        match_features: list of dicts
            Metadata about the matching process and each result location
        """

        updated_locations = []
        for loc in locations:
            if loc.get("flag", 0) == 1: continue
            location_name = loc["locationName"]
            location_type = loc["locationType"]
            # mark inclusion testing locations
            mark = False
            if loc.get("inclusion_test", ""):
                mark = True

            if location_type in ["ADMIN_AREA", "COUNTRY"]:
                updated_loc = loc
                updated_loc["algorithm"] = "SelfMatch"
                updated_loc["features"] = {}
            elif location_name.lower() in text:
                continue
            else:
                # get possible admins
                possible_locs, possible_admins = [], []
                for i, e in enumerate(locations):
                    if e["locationName"] == location_name and e["locationType"] == location_type:
                        address_components = e.get("addressComponents", [])
                        if address_components == []: continue
                        locations[i]["flag"] = 1
                        possible_locs.append(e)
                        possible_admins.extend([x["locationName"] for x in address_components if x["locationType"] == "ADMIN_AREA"])

                features = self.featurize_disambig_candidate(location_name, possible_admins, admin_candidates, text)
                # disambiguous
                picked_admin_area, algorithm, disambig_features = self.disambig_admin_area(possible_admins, features)

                if len(disambig_features) > 1:
                    logging.info(f"Current ambiguous stratedges can solve the problem for {location_name}.")
                    continue
            
                loc_idx = possible_admins.index(picked_admin_area)
                updated_loc = possible_locs[loc_idx]
                updated_loc["algorithm"] = algorithm
                updated_loc["features"] = disambig_features
            
            if mark:
                updated_loc["algorithm"] = self.inclusion_testing_prefix + \
                    loc['algorithm']
    
            updated_locations.append(updated_loc)

        # is_national = looks_like_national_news(locations)
        # if is_national:
        #     print("\nNational news, ignore location tagging.\n")
        #     updated_locations = []
        #     match_features['article_features'].append(
        #         "us_location_tagging_national")

        return updated_locations

    def disambig_admin_area(self, possible_admins, features):
        # Exact match
        def text_specific_mention(c, f):
            if 'specific_mention' in f:
                return 10
            return 0

        def pub_support(c, f):
            if 'from_publisher' in f:
                return 1
            return 0

        def url_support(c, f):
            if 'from_url' in f:
                return 1
            return 0

        def text_support(c, f):
            if 'from_text' in f:
                return 1
            return 0

        def has_multiple_locs(c, f):
            return f.get('stat:count', 0)

        hard_rules = [
            (text_specific_mention, 'TextSpecificMention'),
            (pub_support, 'PublisherSupport'),
            (url_support, 'URLSupport'),
            (text_support, 'TextSupport'),
            (has_multiple_locs, 'MultipleSameStateLocs'),
        ]

        hard_rule_match = {}
        candidate_score = {}
        for candidate, fs in features.items():
            hard_rule_match[candidate] = []
            candidate_score[candidate] = 0
            for rule, algo in hard_rules:
                rule_results = rule(candidate, fs)
                if rule_results == 0:
                    continue
                hard_rule_match[candidate].append(algo)
                candidate_score[candidate] += rule_results

        sorted_result = sorted(candidate_score.items(),
                               key=lambda x: x[1], reverse=True)
        hightest_score = sorted_result[0][1]
        filtered_candidate = [
            k for k, v in sorted_result if v == hightest_score]
        if len(filtered_candidate) == 1:
            selected_admin_area = sorted_result[0][0]
            return selected_admin_area, ",".join(hard_rule_match[selected_admin_area]), {selected_admin_area: features[selected_admin_area]}
        else:
            rules, results = {}, {}
            for candidate in filtered_candidate:
                rules[candidate] = ",".join(hard_rule_match[candidate])
                results[candidate] = features[candidate]
            return filtered_candidate, rules, results

