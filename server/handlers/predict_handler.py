#!/usr/bin/env python
"""
Location tagging v2
"""

import json
import logging
import os
import time
import traceback

import tornado.gen
import tornado.web


class PredictHandler(tornado.web.RequestHandler):
    """
    This class is a handler of corona virus binary prediction.
    """

    def __init__(self, application, request, *args, **kwargs):
        super(PredictHandler, self).__init__(application, request)

    @tornado.gen.coroutine
    @tornado.web.asynchronous
    async def post(self):
        """
        """
        logging.info("process-pid: %s", os.getpid())
        start_time = time.time()

        result = dict(locations=[], features=[])
        
        try:
            req = json.loads(self.request.body)
            sequence = req["sequence"]
            logging.info(f"sequence: {sequence}, receive: {self.request.body}")

            resp = self.application.predictor.tag(req)
            logging.info(resp)
            result.update(resp)
            result = json.dumps(result, default=str)
        except Exception as rough_err:
            detail_err = traceback.format_exc()
            logging.error("sequence: %s | error: %s", sequence, detail_err)
        finally:
            self.finish(result)
            logging.info("sequence: %s takes: %s | return: %s",
                         sequence, time.time() - start_time, result)
