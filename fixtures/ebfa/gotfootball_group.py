#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 21:10:50 2020

@author: JK-MBPro
"""

import gotfootball_event
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

def gameTimeCellToNotes(dataCell: BeautifulSoup) -> str:
    """Gets data from div tags.
    Ignores not set time. (value 00:00:00)
    Concatenates data parts with ' | '"""
   
    dataCellParts=dataCell.find_all('div')
    sMatchNotes = ''
    for dataCellPart in dataCellParts:
        sPart = dataCellPart.get_text()
        # Ignore time that's not set
        if (sPart.strip() != '00:00:00'):
            sMatchNotes += sPart.strip()+' | '
            
    return sMatchNotes[:-3]
            


# https://www.gotfootball.co.uk/events/schedule.aspx?EventID=4389&GroupID=7590&print=true
urlSite='https://www.gotfootball.co.uk'
sEventId = '4389'
sGroupId = '7590'

columnNames = ['MatchId', 'Date', 'Home', 'HG', 'AG', 'Away', 'Notes']
columnDTypes = {'MatchId':str, 'Date':str, 'Home':str, 'HG':str, 'AG':str, 
                'Away':str, 'Notes':str}

sOutFolder = './data/'

def getGroupFixtures(eventId: str, groupId: str) -> pd.DataFrame:
    urlEvent=urlSite+'/events/schedule.aspx?EventID='+eventId
    urlGroup=urlEvent+'&GroupID='+groupId+'&print=true'
    
    bsGroupFixtures=gotfootball_event.getWebpage(urlGroup)

    fixtures = []
    
    bsFixturesDays = bsGroupFixtures.find_all('table', {'class' : 'standings'})
    for bsDayFixtures in bsFixturesDays:
        # Get date. It's a single merged row as a single header
        sDate = bsDayFixtures.find('th', {'class' : 'GroupBoxHeading'}).get_text()
        dDate = datetime.strptime(sDate, '%d %B %Y')
        sSqlDate = dDate.strftime('%Y-%m-%d')
    
        # Get data cell of class gameTime, which isn't header
        # The page has a table per day of fixture.
        # Hence searching for rows in all tables
        matchIds=bsDayFixtures.find_all('td', {'class' : 'gameTime'})
        for matchIdCell in matchIds:
            # Get match ID
            sMatchId = matchIdCell.find('div').get_text().strip()[7:]
            
            # Get match notes
            sMatchNotes = gameTimeCellToNotes(matchIdCell)
            
            # navigate to parent <tr> tag to get rest of the fields
            bsFixtureRow = matchIdCell.find_parent('tr')
            
            # Get home team name
            sHomeTeam = bsFixtureRow.find('td', {'class' : 'homeTeam'}).get_text().strip()
            
            # Get scores
            sScores = []
            bsScores = bsFixtureRow.find_all('span', {'class' : 'score'})
            for bsScore in bsScores:
                sScore = bsScore.get_text().strip()
                if sScore == '':
                    sScores.append(None)
                else:
                    sScores.append(sScore)
    
            # Get home team name
            sAwayTeam = bsFixtureRow.find('td', {'class' : 'awayTeam'}).get_text().strip()
    
            fixtures.append([sMatchId, sSqlDate, 
                             sHomeTeam, sScores[0], 
                             sScores[1], sAwayTeam, 
                             sMatchNotes])

    df = pd.DataFrame(fixtures, columns=columnNames)
    df.set_index('MatchId', inplace=True)
    return(df)
    

dfLast = pd.read_csv(sOutFolder+sEventId+'-'+sGroupId+'-LatestFixtures.csv', 
                       dtype=columnDTypes)
dfLast.set_index('MatchId', inplace=True)
dfLast.sort_index(inplace=True)

dfCurrent = getGroupFixtures(sEventId, sGroupId)
dfCurrent.sort_index(inplace=True)


dfChanges = dfLast.compare(dfCurrent, keep_shape=False, keep_equal=False)
if dfChanges.empty:
    print('No changes')
else:
    print(dfChanges.columns)
    dfChanges.columns = [f'{f}_old' if s == 'self' else (f'{f}_new' if s == 'other' else f'{f}') for f, s in dfChanges.columns]
    
    sTimestamp = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    dfChanges.insert(0, 'Updated', sTimestamp)
    dfChangeLog = dfChanges.join(dfLast)
    dfChangeLog2 = dfChangeLog.append(dfChanges.join(dfCurrent))
    dfChangeLog2.sort_index(inplace=True)
    print(dfChangeLog2)
    dfChangeLog2.to_csv(sOutFolder+sEventId+'-'+sGroupId+'-'+sTimestamp+'-Updates.csv')



dfCurrent.to_csv(sOutFolder+sEventId+'-'+sGroupId+'-LatestFixtures.csv')

