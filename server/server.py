#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
main class for server
'''

import logging
import logging.config
import os

from cloghandler import ConcurrentRotatingFileHandler
import tornado.httpserver
import tornado.ioloop
from tornado.options import define, options
import tornado.web

from conf.logging_config import logging_conf
from handlers.predict_handler import PredictHandler
from src.us_location_tagger import *

define("port", default=8901, help="run on the given port", type=int)
define("dev", default=True, help="dev mode", type=bool)


logging.config.dictConfig(logging_conf)


class HealthCheckHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Serving alive...")


class Application(tornado.web.Application):
    '''main class'''

    def __init__(self):
        handlers = [
            (r'/predict', PredictHandler),
            (r'/ping', HealthCheckHandler),
        ]
        self.predictor = LocationTagger()
        tornado.web.Application.__init__(self, handlers, debug=True)

        logging.info('process starts: ' + str(os.getpid()))


def main():
    '''main func'''
    tornado.options.parse_command_line()
    port = int(options.port)
    if options.dev or options.num < 2:
        server = tornado.httpserver.HTTPServer(Application(), xheaders=True)
        server.listen(port)
    else:
        sockets = tornado.netutil.bind_sockets(port)
        tornado.process.fork_processes(options.num)
        server = tornado.httpserver.HTTPServer(Application(), xheaders=True, decompress_request=conf.use_decompress)
        server.add_sockets(sockets)

    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
