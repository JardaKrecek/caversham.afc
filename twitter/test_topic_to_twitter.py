#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  5 11:32:06 2020

@author: JK-MBPro

Test driver for the Twitter sender
"""

import os

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                                'topic_text_to_twitter_dm')))
import main as topic_to_twitter

from dataclasses import dataclass
from datetime import datetime

from google.cloud import pubsub_v1 as pubsub

import base64

os.environ['consumer_key']='epPWNaRaKQoIA17b2ATtyb8Fn'
os.environ['consumer_secret']="0GWhi5aUhUWm0TL6X9NyHc3hGBSEWuRjYpXlJQ5r4gh8pCqB0f"
os.environ['access_token']="25494503-sOW31UdIZyMfWODVDEJiK9VqNgGppaEpTQ8f8AnO1"
os.environ['access_secret']="cxgiVrUE6d9xeg8U0YFOxzD6Vn4Y6Hg6RzZx0TQHCuSlQ"
os.environ['twitter_user_id']='25494503'


@dataclass
class pubsubContext:
    event_id: str = '-1'
    timestamp: str = datetime.now().isoformat()
    event_type: str = 'google.pubsub.topic.publish'
    resource: str = ''

pbsbMsg = { 
    'data': base64.b64encode('Test Unit: Test message data'.encode('utf-8'))
    }
    
    
topic_to_twitter.topic_to_twitter(pbsbMsg, pubsubContext())