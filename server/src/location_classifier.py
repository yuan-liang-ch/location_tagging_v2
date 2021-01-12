import json
import pickle
import argparse
import logging

import numpy as np

logger = logging.getLogger()
logger.setLevel("INFO")


def load_model(model_path):
    with open(model_path, "rb") as f:
        return pickle.load(f)

class LocationClassifier:
    """
    """
    def __init__(self, model_path):
        self.model_file = load_model(model_path)
        self.model = self.model_file.get("model", None)
        self.preprocess = self.model_file.get("preprocesser", None)
        if self.model is None:
            logger.error("Model is None.")

    def input_parser(self, element):
        """
        from Candidate Generation Output
        return raw model input
        https://www.notion.so/Location-tagging-service-input-output-definition-d783cb50b9e14590a63d79a141b17567
        """
        x_test = np.array(element["filter_features"]["text_features"])[np.newaxis, :]
        return x_test

    def predict_one(self, sample):
        pred, prob = None, 0
        X_test = self.input_parser(sample)
        try:
            X_test = self.preprocess.transform(X_test)
            pred = self.model.predict(X_test)[0]
            prob = self.model.predict_proba(X_test)[0][1]
        except Exception as e:
            logger.error(type(self).__name__)
            logger.error(e)
            logger.error("ori example reading failed")
        finally:
            return pred, prob

    def predict_doc(self, sample_list):
        pred_true_loc = []
        best_loc = None
        max_prob = 0
        for ele in sample_list:
            pred, prob = self.predict_one(ele)
            ele["filter_features"]["pred"] = pred
            ele["filter_features"]["prob"] = prob
            if pred == 1:
                pred_true_loc.append(ele)
            if prob >= max_prob and prob >= 0.1:
                max_prob = prob
                best_loc = ele
        if pred_true_loc == []:
            return [] if best_loc is None else [best_loc]
        else:
            return pred_true_loc
