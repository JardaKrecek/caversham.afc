#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 21:10:50 2020
@author: JKrecek

Test of publishing to the GCP Pub/Sub topic
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                                'ebfa_fixtures_change_detect')))
import main as ebfa_fixtures_change_detect_func

from dataclasses import dataclass
from datetime import datetime

import base64

            
# Set GOOGLE_APPLICATION_CREDENTIALS to the private key for GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/JK-MBPro/Documents/github.com/caversham.afc/key/CavershamAFC-1e2c4f66b68b.json"


@dataclass
class pubsubContext:
    event_id: str = '-1'
    timestamp: str = datetime.now().isoformat()
    event_type: str = 'google.pubsub.topic.publish'
    resource: str = ''


pbsbMsg = { 
    'data': base64.b64encode('Test'.encode('utf-8'))
    }


ebfa_fixtures_change_detect_func.fixtures_check(pbsbMsg, pubsubContext())