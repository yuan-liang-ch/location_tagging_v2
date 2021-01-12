#!/usr/bin/env python
# -*- coding:utf-8 -*-
import requests
import json

url = 'http://localhost:8902/predict'
#url = 'http://10.0.101.137:32792/predict'
#url = 'http://10.0.101.181:32912/predict'

req = dict(
        sequence=123,
        url="http://hao123.com",
        slimTitle="Lake Houston Area Chamber of Commerce CEO named to San Jacinto Regional Flood Planning Group",
        body="Lake Houston Area Chamber of Commerce CEO Jenna Armstrong was named to the Texas Water Development Board’s San Jacinto Regional Flood Planning Group. (Courtesy Lake Houston Area Chamber of Commerce) Now is the chance to help your local community succeed. Become a Patron by contributing to Community Impact Newspaper and gain daily insight into what's happening in your own backyard. Thank you for reading and supporting community journalism.become a ci patroncontribute today By Andy Li | 11:13 AM Oct 7, 2020 CDT | Updated 11:13 AM Oct 7, 2020 CDT Jenna Armstrong, Lake Houston Area Chamber of Commerce CEO, was named to the Texas Water Development Board’s San Jacinto Regional Flood Planning Group, according to an Oct. 1 press release. The group serves to prevent loss of life and property damage due to flooding in the region and was created as a part of Texas’ regional flood planning process by the Texas Legislature through Senate Bill 8 in 2019. Armstrong is one of 12 members of the group. These members were selected through a nomination process. “I’m proud to represent the Lake Houston Area and collaborate with leaders across the region to create a plan to reduce flooding in the San Jacinto watershed,” Armstrong said. “I look forward to being a part of this unprecedented and ambitious effort to reduce and manage flood risks across the entire state of Texas. ” The group will be responsible for adopting a flood plan by Jan. 10, 2023.",
        features = [],
        googleEntities = [{"name":"Jenna Armstrong", "salience":0.18, "wiki_url": None, "mid": "", "type": "PERSON"}, {"name": "San Jacinto Regional Flood Planning Group Lake Houston Area", "salience":0.18, "wiki_url": None, "mid": "", "type": "LOCATION"}, {"name": "Lake Houston Area", "salience":0.18, "wiki_url": None, "mid": "", "type": "LOCATION"}, {"name": "Texas", "salience":0.18, "wiki_url": "https://en.wikipedia.org/wiki/Texas", "mid": "", "type": "LOCATION"}]
)

resp = requests.post(url, json.dumps(req))
print(resp.json())
