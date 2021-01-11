
__all__ = ["logger", "timeit", "pprint", "singleton", "send_analysis_log", "write_to_stream"]

import os
import time
import json
import logging
import boto3
from datetime import datetime
from botocore.client import Config

logger = logging.getLogger()
logger.setLevel("INFO")


def timeit(method):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        logging.info('[TIME] %r %2.2f sec' % (method.__name__, te-ts))
        return result

    return timed

def pprint(obj, obj_name='', print_f=logger.info):
    message = json.dumps(obj, indent=4, ensure_ascii=False)
    if obj_name:
        message = f"{obj_name} = " + message
    print_f(message)

def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]
    
    return inner

@timeit
def send_analysis_log(seaa_event, locations):
    dtime = datetime.utcnow()
    dt = dtime.strftime("%Y-%m-%d")
    hh = dtime.strftime("%H")
    sequence = seaa_event['sequence']

    # only save when sequence if available
    if sequence:
        message_template = '[ANALYSIS]|{dt}|{hh}|{seq}|{loc_id}|{loc_type}|{name}|{algor}|{salience}'

        # record only valid locations
        valid_locs = [
            loc for loc in locations if loc['locationId'] != INVALID_LOCATION_ID]
        for loc in valid_locs:
            message = message_template.format(dt=dt,
                                              hh=hh,
                                              seq=sequence,
                                              loc_id=loc['locationId'],
                                              loc_type=loc['locationType'],
                                              name=loc['name'],
                                              algor=loc['algorithm'],
                                              salience=loc['salience'])
            logger.info(message)

@timeit
def write_to_stream(event_id, event, output, region_name=None, stream_name=None):
    """Write streaming event to specified Kinesis Stream within specified region.

    Parameters
    ----------
    event_id: str
        The unique identifier for the event which will be needed in partitioning.
    event: dict
        SE-AA request
    output: dict
        output from lambda function
    region_name: str
        AWS region identifier, e.g., "ap-northeast-1".
    stream_name: str
        Kinesis Stream name to write.
    Returns
    -------
    res: Response returned by `put_record` func defined in boto3.client('kinesis')
    """
    region_name = os.environ.get("REGION_NAME", "") if not region_name else region_name
    stream_name = os.environ.get("KINESIS_STREAM", "") if not stream_name else region_name

    log = {}
    fields = ['sequence', 'slimTitle', 'features', 'url']
    for field in fields:
        log[field] = event[field]

    log["locations"] = json.dumps(output["locations"])

    config = Config(
        connect_timeout=1,
        read_timeout=3,
        retries=dict(
            max_attempts=3
        )
    )
    print(log)

    client = boto3.client('kinesis', region_name=region_name, config=config)
    res = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(log) + '\n',
        PartitionKey=str(event_id)
    )
    print(res)
    return res

