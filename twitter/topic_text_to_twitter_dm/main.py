#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  5 11:32:06 2020

@author: JK-MBPro
"""

def topic_to_twitter(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    
    # Importing the libraries we'll use
    import tweepy
    import os
    import random
    from os import getenv
    
    import base64
    
    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        topicTxt = base64.b64decode(event['data']).decode('utf-8')
    else:
        topicTxt = 'Topic message received. No text data.'
    print('Topic text: "{}"'.format(topicTxt))    
    
    # Getting the key and secret codes from my environment variables
    consumer_key = getenv("consumer_key")
    consumer_secret = getenv("consumer_secret")
    access_token = getenv("access_token")
    access_secret = getenv("access_secret")
    
    if len(consumer_key) == 0:
        print('Missing consumer key for Twitter authentication.')
        return()
    
    if len(consumer_secret) == 0:
        print('Missing consumer secret for Twitter authentication.')
        return()
        
        
    if len(access_token) == 0:
        print('Missing access token for Twitter authentication.')
        return()
        
    if len(access_secret) == 0:
        print('Missing access secret for Twitter authentication.')
        return()                    


    # Tweepy's process for setting up authorisation
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    
    try:
        api.verify_credentials()
        print("Authentication OK")
    except Exception as e:
        print("Error during authentication: [", e, ']')
        return()
    
    """    
    try:
        api.update_status("Test tweet from JK_CAFC_Bot")
        print("Tweeted OK")
    except Exception as e:
        print("Error during Updating Status: [", e, ']')
    """

    twitter_user_id = getenv('twitter_user_id')        
    try:
        api.send_direct_message(twitter_user_id, topicTxt)
        print("Sent DM OK")
    except Exception as e:
        print("Error during sending direct message:", e)
        return()