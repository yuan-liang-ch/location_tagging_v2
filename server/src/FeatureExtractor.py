#! /usr/bin/python3
# -*- coding: utf-8 -*-

import re
import logging
from sys import argv
from nltk import word_tokenize

from .PlacelineTagger import PlacelineTagger
from .PlacelineTagger import StateCodeManager
from .PlacelineTagger import WikiDataManager


logger = logging.getLogger()
logger.setLevel("INFO")

class LocationCandidateProcessor(object):
    def __init__(self):
        pass

    def parse_cg_info(self, kvps):
        this_loc_id = kvps.get("locationId", "")
        this_loc_name = kvps.get("locationName", "")
        this_loc_type = kvps.get("locationType", "")
        this_info_source = kvps.get("source")
        if not this_loc_name or not this_loc_type or this_loc_type == "COUNTRY":
            logger.warning("invalid loc name or type: {0}/{1}".format(this_loc_name, this_loc_type))
            return this_loc_id, None, None
        if this_loc_type == "ADMIN_AREA":
            return this_loc_id, [this_loc_name], this_info_source
        else:
            county, state = None, None
            address_components = kvps.get("addressComponents")
            for addr_comp in address_components:
                addr_type = addr_comp.get("locationType", "")
                addr_name = addr_comp.get("locationName", "")
                if addr_type == "SUB_ADMIN_AREA":
                    county = addr_name
                elif addr_type == "ADMIN_AREA":
                    state = addr_name
            if this_loc_type == "SUB_ADMIN_AREA" and state:
                return this_loc_id, [this_loc_name, state], this_info_source
            elif this_loc_type == "LOCALITY" and county and state:
                    return this_loc_id, [this_loc_name, county, state], this_info_source
            elif this_loc_type == "LOCALITY" and state:
                return this_loc_id, [this_loc_name, state], this_info_source
            else:
                logger.warning("insufficient information: {0}/{1}/{2}".format(this_loc_name, county, state))
        return this_loc_id, None, None

    def getName(self, fields):
        #return "/".join(self.all_fields)
        if len(fields) == 0:
            return "NO_NAME"
        else:
            return "/".join(fields)
        # elif len(fields) == 1:
        #     return fields[0]
        # else:
        #     return fields[0] + "/" + fields[-1]

    def getFVPs(self, features):
        return "|".join([f+":"+str(v) for (f,v) in features.items()])


class FeatureExtractor(object):
    def __init__(self, placelineTagger, full_2_short):
        self.placelineTagger = placelineTagger
        self.loc_cand_processor = LocationCandidateProcessor()
        self.full_2_short = full_2_short

    def get_explicit_patterns(self, loc_name, admin):
        result = []
        escaped_name = loc_name.lower()
        for admin_area_alias in self.full_2_short.get(admin, []) + [admin]:
            comma_optional = "?"
            if admin_area_alias in ["or", "in"]:
                comma_optional = ""
            escaped_alias = admin_area_alias
            result.append(f"{escaped_name},{comma_optional} {escaped_alias}")
        return result

    def extractFeatures(self, doc_id, title, bodytext, cg_cand_list, label, mode="predict"):
        if label == "" and mode == "train":
            logger.error("[ERROR] In train mode, no ground truth.")
        processed_title = word_tokenize(title)
        processed_body = word_tokenize(bodytext)
        doc_len = len(processed_body)
        placeline = self.placelineTagger.processDocument(bodytext)
        final_result = []

        for cg_cand in cg_cand_list:
            features = {}
            try:
                locId, all_fields, info_source = self.loc_cand_processor.parse_cg_info(cg_cand)
                if all_fields is None and info_source is None: continue
                full_name = self.loc_cand_processor.getName(all_fields)
                target_name = all_fields[0]
                state = all_fields[-1]
                # PLACELINE feature
                if placeline and placeline == full_name:
                    features["PLACELINE"] = 1
                else:
                    features["PLACELINE"] = 0
                # SOURCE feature
                if info_source == "WikiMatch":
                    features["FROM_TEXT"] = 1
                    features["FROM_GOOG"] = 0
                    features["FROM_PUB"] = 0
                elif info_source == "LocalPublishder":
                    features["FROM_TEXT"] = 0
                    features["FROM_GOOG"] = 0
                    features["FROM_PUB"] = 1
                else:
                    features["FROM_TEXT"] = 0
                    features["FROM_GOOG"] = 1
                    features["FROM_PUB"] = 0
                # TITLE feature
                features["TITLE"] = 1 if LocationFeatureExtractor.existsInText(target_name, processed_title) else 0
                # FREQuency feature
                features["FREQ"] = LocationFeatureExtractor.countOccurrences(target_name, processed_body) / doc_len
                # FIRST POSition feature
                features["FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(target_name, processed_body) / doc_len
                # ALL CAPital name"s FREQuency feature
                features["ALL_CAP_FREQ"] = LocationFeatureExtractor.countOccurrences(target_name.upper(), processed_body) / doc_len
                # ALL CAPItal name"s FIRST POSition feature
                features["ALL_CAP_FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(target_name.upper(), processed_body) / doc_len
                # CONTEXT feature (meanwhile context means just dashes)
                features["CONTEXT"] = 1 if LocationFeatureExtractor.hasContext(target_name, ["—", "-", "–", "--"], processed_body) else 0
                # FREQuency & FIRST POSition in FIRST 100 TOKENS features
                first100_toks = processed_body[:100] if len(processed_body) > 100 else processed_body
                features["FREQ_100"] = LocationFeatureExtractor.countOccurrences(target_name, first100_toks) / 100
                features["FIRST_POS_100"] = LocationFeatureExtractor.findFirstOccurrence(target_name, first100_toks) / 100
                 # Is lower case
                features["IS_LOWER_CASE"] = 1 if LocationFeatureExtractor.existsInText(target_name.lower(), processed_body) else 0
                # state features
                features["STATE_EXISTS"] = 1 if LocationFeatureExtractor.existsInText(state, processed_body) else 0
                features["STATE_FREQ"] = LocationFeatureExtractor.countOccurrences(state, processed_body) / doc_len
                features["STATE_FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(state, processed_body) / doc_len
                features["STATE_ALL_CAP_FREQ"] = LocationFeatureExtractor.countOccurrences(state.upper(), processed_body) / doc_len
                features["STATE_ALL_CAP_FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(state.upper(), processed_body) / doc_len
                # co-occurance
                explicit_patterns = self.get_explicit_patterns(target_name, state)
                features["CO_OCCURANCE"] = 1 if any(re.search(p, bodytext.lower()) is not None for p in explicit_patterns) else 0
                # putting full name and all features back to cg_cand
                cg_cand["henry_combo_name"] = full_name
                if "filter_features" not in cg_cand:
                    cg_cand["filter_features"] = {}
                sorted_features = sorted(features.items(), key=lambda x : x[0])
                cg_cand["filter_features"]["text_features"] = [x[1] for x in sorted_features]
                cg_cand["filter_features"]["text_features_names"] = [x[0] for x in sorted_features]
                if mode == "train":
                    cg_cand["filter_features"]["gt"] = 1 if full_name == label else 0
                final_result.append(cg_cand)
            except Exception as ex:
                logger.error(f"Feature extractor {doc_id}: {ex}")
        return final_result

    @staticmethod
    def hasContext(target, contexts, txt_toks):
        for context in contexts:
            if LocationFeatureExtractor.existsInText(context + " " + target, txt_toks):
                return True
            if LocationFeatureExtractor.existsInText(target + " " + context, txt_toks):
                return True
        return False

    @staticmethod
    def findFirstOccurrence(target, txt_toks):
        trg_toks = target.split(" ")
        start = 0
        while start < len(txt_toks):
            try:
                pos0 = txt_toks.index(trg_toks[0], start)
                if trg_toks == txt_toks[pos0:pos0+len(trg_toks)]:
                    return pos0
                else:
                    start = pos0 + len(trg_toks)
            except ValueError:
                return -1
        return -1

    @staticmethod
    def countOccurrences(target, txt_toks):
        result = 0
        trg_toks = target.split(" ")
        start = 0
        while start < len(txt_toks):
            try:
                pos0 = txt_toks.index(trg_toks[0], start)
                if trg_toks == txt_toks[pos0:pos0+len(trg_toks)]:
                    result += 1
                start = pos0 + len(trg_toks)
            except ValueError:
                break
        return result

    @staticmethod
    def existsInText(target, txt_toks):
        return " "+target+" " in " "+" ".join(txt_toks)+" "

    @staticmethod
    def normalizeToken(tok):
        if len(tok) > 2 and tok.isupper():
            return tok[0].upper() + tok[1:].lower()
        else:
            return tok

    @staticmethod
    def normalizeNgram(toks):
        return [LocationFeatureExtractor.normalizeToken(tok) for tok in toks]


class LocationCandidate:
    def __init__(self, kvps):
        """
        the constructor takes as input the key-value pairs from (Wendy"s) candidate generation module
        """
        self.locId, self.all_fields, self.info_source = self.parseCGinfo(kvps)
        self.features = {} 

    def parseCGinfo(self, kvps):
        this_loc_id = kvps.get("locationId", "")
        this_loc_name = kvps.get("locationName", "")
        this_loc_type = kvps.get("locationType", "")
        this_info_source = kvps.get("source")
        if not this_loc_name or not this_loc_type or this_loc_type == "COUNTRY":
            raise Exception("invalid loc name or type: {0}/{1}".format(this_loc_name, this_loc_type))
        if this_loc_type == "ADMIN_AREA":
            return this_loc_id, [this_loc_name], this_info_source
        else:
            county, state = None, None
            address_components = kvps.get("addressComponents")
            for addr_comp in address_components:
                addr_type = addr_comp.get("locationType", "")
                addr_name = addr_comp.get("locationName", "")
                if addr_type == "SUB_ADMIN_AREA":
                    county = addr_name
                elif addr_type == "ADMIN_AREA":
                    state = addr_name
                if this_loc_type == "SUB_ADMIN_AREA" and state:
                    return this_loc_id, [this_loc_name, state], this_info_source
                elif this_loc_type == "LOCALITY" and county and state:
                    return this_loc_id, [this_loc_name, county, state], this_info_source
            # exceptional case: full information cannot be retrieved
            if this_loc_type == "LOCALITY" and state:
                return this_loc_id, [this_loc_name, state], this_info_source
            else:
                raise Exception("insufficient information: {0}/{1}/{2}".format(this_loc_name, county, state))

    def getName(self):
        #return "/".join(self.all_fields)
        if len(self.all_fields) == 0:
            return "NO_NAME"
        else:
            return "/".join(self.all_fields)
        # elif len(self.all_fields) == 1:
        #     return self.all_fields[0]
        # else:
        #     return self.all_fields[0] + "/" + self.all_fields[-1]
    
    def getFVPs(self):
        return "|".join([f+":"+str(v) for (f,v) in self.features.items()])

    def getFeatureKeys(self):
        return [k for (k,v) in self.features.items()]
    
    def getFeatureValues(self):
        return [v for (k,v) in self.features.items()]


class LocationFeatureExtractor:
    def __init__(self, placelineTagger, doc_id, title, bodytext, cg_cand_list):
        #self.placelineTagger = placelineTagger
        self.doc_id = doc_id
        self.title = word_tokenize(title)
        self.src_txt = bodytext     # the real source or original, as "str"
        self.original_text = word_tokenize(bodytext) # word-tokenized version of the source/original
        self.norm_text = LocationFeatureExtractor.normalizeNgram(self.original_text) # the "normalized" version of the source/original
        self.doc_len = len(self.original_text)
        self.cg_cand_list = cg_cand_list
        self.placeline = placelineTagger.processDocument(self.src_txt)

    def extractFeatures(self):
        for cg_cand in self.cg_cand_list:
            try:
                loc_cand = LocationCandidate(cg_cand)
                full_name = loc_cand.getName()
                target_name = loc_cand.all_fields[0]
                # PLACELINE feature
                if self.placeline and self.placeline == full_name:
                    loc_cand.features["PLACELINE"] = 1
                else:
                    loc_cand.features["PLACELINE"] = 0
                # SOURCE feature
                if loc_cand.info_source == "WikiMatch":
                    loc_cand.features["FROM_TEXT"] = 1
                    loc_cand.features["FROM_GOOG"] = 0
                else:
                    loc_cand.features["FROM_GOOG"] = 1
                    loc_cand.features["FROM_TEXT"] = 0
                # TITLE feature
                loc_cand.features["TITLE"] = 1 if LocationFeatureExtractor.existsInText(target_name, self.title) else 0
                # FREQuency feature
                loc_cand.features["FREQ"] = LocationFeatureExtractor.countOccurrences(target_name, self.original_text) / self.doc_len
                # FIRST POSition feature
                loc_cand.features["FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(target_name, self.original_text) / self.doc_len
                # ALL CAPital name"s FREQuency feature
                loc_cand.features["ALL_CAP_FREQ"] = LocationFeatureExtractor.countOccurrences(target_name.upper(), self.original_text) / self.doc_len
                # ALL CAPItal name"s FIRST POSition feature
                loc_cand.features["ALL_CAP_FIRST_POS"] = LocationFeatureExtractor.findFirstOccurrence(target_name.upper(), self.original_text) / self.doc_len
                # CONTEXT feature (meanwhile context means just dashes)
                loc_cand.features["CONTEXT"] = 1 if LocationFeatureExtractor.hasContext(target_name, ["—", "-", "–", "--"], self.original_text) else 0
                # FREQuency & FIRST POSition in FIRST 100 TOKENS features
                first100_toks = self.original_text[:100] if len(self.original_text) > 100 else self.original_text
                loc_cand.features["FREQ_100"] = LocationFeatureExtractor.countOccurrences(target_name, first100_toks) / 100
                loc_cand.features["FIRST_POS_100"] = LocationFeatureExtractor.findFirstOccurrence(target_name, first100_toks) / 100
                # putting full name and all features back to cg_cand
                cg_cand["henry_combo_name"] = full_name
                if "filter_features" not in cg_cand:
                    cg_cand["filter_features"] = {}
                cg_cand["filter_features"]["text_features"] = loc_cand.getFeatureValues()
                cg_cand["filter_features"]["text_features_names"] = loc_cand.getFeatureKeys()
            except Exception as ex:
                print("Henry: " + str(self.doc_id) + " ==> " + str(ex))


    @staticmethod
    def hasContext(target, contexts, txt_toks):
        for context in contexts:
            if LocationFeatureExtractor.existsInText(context + " " + target, txt_toks):
                return True
            if LocationFeatureExtractor.existsInText(target + " " + context, txt_toks):
                return True
        return False

    @staticmethod
    def findFirstOccurrence(target, txt_toks):
        trg_toks = target.split(" ")
        start = 0
        while start < len(txt_toks):
            try:
                pos0 = txt_toks.index(trg_toks[0], start)
                if trg_toks == txt_toks[pos0:pos0+len(trg_toks)]:
                    return pos0
                else:
                    start = pos0 + len(trg_toks)
            except ValueError:
                return -1
        return -1

    @staticmethod
    def countOccurrences(target, txt_toks):
        result = 0
        trg_toks = target.split(" ")
        start = 0
        while start < len(txt_toks):
            try:
                pos0 = txt_toks.index(trg_toks[0], start)
                if trg_toks == txt_toks[pos0:pos0+len(trg_toks)]:
                    result += 1
                start = pos0 + len(trg_toks)
            except ValueError:
                break
        return result

    @staticmethod
    def existsInText(target, txt_toks):
        return " "+target+" " in " "+" ".join(txt_toks)+" "

    @staticmethod
    def normalizeToken(tok):
        if len(tok) > 2 and tok.isupper():
            return tok[0].upper() + tok[1:].lower()
        else:
            return tok

    @staticmethod
    def normalizeNgram(toks):
        return [LocationFeatureExtractor.normalizeToken(tok) for tok in toks]
