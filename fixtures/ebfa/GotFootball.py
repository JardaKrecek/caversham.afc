#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 300)


# Get list of EBFA event and group IDs for League and Trophy event
import requests
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

listEventIDs = {'4389', '4390'}
dfEventGrpID = pd.DataFrame(columns=['EventID', 'GroupID'])


# loop through root web pages for each event
for sEventID in listEventIDs:
    url = 'http://ebfa.gotfootball.co.uk/events/default.aspx?EventID='+sEventID

    #Create a request session
    r = requests.Session()
    response = r.get(url)
    html = response.text

    soup = BeautifulSoup(html, 'lxml')

    # loop through all hyperlinks that go to schedule.aspx
    for link in soup.find_all('a'):
        # links are formated as
        # schedule.aspx?EventID=4389&GroupID=7629&Gender=Coed&Age=10
        sLink = link.get('href')
        
        parsedLink = urlparse(sLink)
        
        if parsedLink.path == 'schedule.aspx':
            parsedQuery = parse_qs(parsedLink.query)
            
            if (('Age' in parsedQuery.keys()) 
                & ('EventID' in parsedQuery.keys())
                & ('GroupID' in parsedQuery.keys())):
                if int(parsedQuery['Age'][0]) >= 13:
                    dfEventGrpID = dfEventGrpID.append({'EventID' : parsedQuery['EventID'][0],
                                                        'GroupID' : parsedQuery['GroupID'][0]},
                                                       ignore_index=True)
                
# print('\nPopulated dataframe of EBFA links IDs:\n', dfEventGrpID)


# Loop through event - group ID and get home games
dfCafcHomeGames = pd.DataFrame()

for i in dfEventGrpID.index:
    urlGF = 'http://ebfa.gotfootball.co.uk/events/bracketschedule.aspx?EventID='+ \
        dfEventGrpID.at[i, 'EventID'] + '&GroupID=' + \
        dfEventGrpID.at[i, 'GroupID'] + '&print=true'
    print('Inspecting', urlGF)
        
    # load the html
    df_list = pd.read_html(urlGF, header=0)

    # get the last table from the website
    dfGF = df_list[-1]
    dfGF.rename(columns={'Unnamed: 3': 'GH', 'Unnamed: 5': 'GA', 'Unnamed: 7': 'Note'}, inplace=True)
    # print('Table from Gotfootball:')
    # print(dfGF.head())
    
    # get division info
    divStrList = dfGF.iloc[0]['Game'].split(' - ', 1)
    ageStrList = divStrList[0].split(' ', 2)
    ageStr = ageStrList[1]
    divStr = ageStrList[2]
    # print(ageStr)
    # print(divStr)
    
    # extract list of dates
    matchDatesStr = dfGF['Game'].tolist()
    
    # replace each match id with previous value
    prevCell = 'Date'
    for ix in range (0,len(matchDatesStr)):
        if matchDatesStr[ix][:1]=='#':
            matchDatesStr[ix]=prevCell
        else:
            dateString = matchDatesStr[ix][-10:]
            # print(dateString)
            if len(dateString)==10:
                prevCell = dateString
            matchDatesStr[ix]=prevCell
    
    # Convert match date strings to datetime
    from datetime import datetime
    matchDates = []
    for dateStr in matchDatesStr:
        matchDates.append(datetime.strptime(dateStr,'%d/%m/%Y'))
    
    # print('\nList of match dates:')
    # print(matchDates)
    
    # insert the match dates column in front
    dfGF.insert(0,'Date',matchDates)
    dfGF.insert(2,'Division',ageStr+' '+divStr)
    # print('\nGotfootball with match dates:')
    # print(dfGF.head())
    
    
    # filter home games only (ignore BYE games) 
    # filteredGF = dfGF[(dfGF['Game'].str[0]=='#') \
    #                   & (dfGF['Home Team'].str.startswith('CAVERSHAM AFC')) \
    #                   & ~(dfGF['Away Team'].str.contains('DIV BYE'))]

    filteredGF = dfGF[(dfGF['Game'].str[0]=='#') \
                      & (dfGF['Home Team'].str.startswith('CAVERSHAM AFC')) \
                      & ~(dfGF['Away Team'].str.contains('DIV BYE'))]
        
        
    # Get full name of the home team
    for i in filteredGF.index: #range (0,len(filteredGF)):
        teamStrFull = filteredGF.at[i, 'Home Team'].split(' (',1)[0]
        teamStr=teamStrFull.split(' ',2)[2]
        filteredGF.at[i,'Home Team']    = ageStr+' '+teamStr
        filteredGF.at[i, 'Game']        = filteredGF.at[i, 'Game'][:5]
        filteredGF.at[i, 'Away Team']   = filteredGF.at[i, 'Away Team'].split(' (',1)[0]
    
    # if (len(dfGF)>0):
    #     print('\nGotfootball home games without obsolete rows:', filteredGF)
    
    dfCafcHomeGames=dfCafcHomeGames.append(filteredGF, ignore_index=True)
    
print('\nFinal set of home games:\n', dfCafcHomeGames)

dfCafcHomeGames.to_csv("GotFootball.csv")