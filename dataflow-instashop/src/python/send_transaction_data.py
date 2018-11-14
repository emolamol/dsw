#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import logging
import datetime
import glob
from google.cloud import pubsub

TOPIC = 'instashop'

def publish(topic, events):
   numobs = len(events)
   if numobs > 0:
      for event_data in events:
          publisher.publish(topic, event_data)


def simulate(topic):
   
   while True:
        topublish = list()
        files = glob.glob("./orders/*.json")

        for file_name in files:
            file = open(file_name, "r")
            topublish.append(file.read())
        print("published transations count - " + str(len(topublish)) + " , Current Timestamp - " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        publish(topic, topublish)
        time.sleep(1)   

if __name__ == '__main__':
   
   # create Pub/Sub notification topic
   logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
   publisher = pubsub.PublisherClient()
   event_type = publisher.topic_path('namita-186919',TOPIC)
   try:
      publisher.get_topic(event_type) 
      logging.info('Reusing pub/sub topic {}'.format(TOPIC))
   except:
      publisher.create_topic(event_type)
      logging.info('Reusing pub/sub topic {}'.format(TOPIC))
   
   simulate(event_type)
