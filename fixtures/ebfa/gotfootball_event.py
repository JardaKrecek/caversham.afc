#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 18:17:31 2020
@author: JardaKrecek

Gets data for an event at GotFootball.co.uk
"""

from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

import pandas as pd


def getWebpage(url: str) -> BeautifulSoup:
    """Opens url and returns web page in BeautifulSoup.
    Returns None on any exception"""
    
    # open url
    try:
        pageHtml = urlopen(url)
    except HTTPError as err:
        print('get_webpage HTTPError exception:', err)
        return None
    
    # create BeautifulSoup object from the returned pageHtml
    try:
        bs = BeautifulSoup(pageHtml.read(), 'lxml')
    except AttributeError:
        print('get_webpage AttributeError')
        return None
    
    return bs


def getTeamLogo(teamCell: BeautifulSoup) -> str:
    logoTag = teamCell.img
    if logoTag is None:
        return None
    
    return(urlSite+logoTag['src'])


def getTeamId(teamCell: BeautifulSoup) -> str:
    """ Retrieves Team ID from <a href ... > with applicationID attribute """
    
    if teamCell is None:
        return None
    
    teamTag = teamCell.a
    if teamTag is None:
        return None
    
    teamLink = teamTag['href']
    teamLinkAttribs = teamLink.split('&')
    for linkPart in teamLinkAttribs:
        if linkPart.startswith('applicationID='):
            return(linkPart.split('=')[1])
            

def getScore(dataCell: BeautifulSoup):
    """ Retrieves Home score followed by Away score.
        Empty if none found
    """
    if dataCell is None:
        return None
    
    retScore = []
    
    scoreData = dataCell.find_all('', {'class':'score'})
    if scoreData is None:
        return None

    for score in scoreData:
        retScore.append(score.get_text())
            
    return retScore



def getMatchData(resultsRow: BeautifulSoup) -> pd.DataFrame:
    """ 
        Finds on the EBFA match result webpage following details: 
        - Match ID
        - Home Team name
        - Home Team logo URL
        - Home Team ID
        - Home goals
        - Away goals
        - Away Team name
        - Away Team logo URL
        - Away Team ID
    """

    if resultsRow is None:
        return None
        
    sMatchNo = ''
    
    # home team logo
    sHTLogo = getTeamLogo(resultsRow[0])
    
    # home team ID and Name
    sHTId = getTeamId(resultsRow[1])
    sHTName = resultsRow[1].get_text()
    
    # match results
    goals = getScore(resultsRow[2])
    
    # away team logo
    sATLogo = getTeamLogo(resultsRow[4])
    
    # away team ID and Name
    sATId = getTeamId(resultsRow[3])
    sATName = resultsRow[3].get_text()

    return (pd.DataFrame([[sMatchNo, 
                           sHTName, sHTLogo, sHTId, 
                            goals[0], goals[1], 
                            sATName, sATLogo, sATId]]))


"""    

# url='https://www.gotfootball.co.uk/events/schedule.aspx?EventID=4389&MatchNumber=4381'
urlSite='https://www.gotfootball.co.uk'
urlEvent=urlSite+'/events/schedule.aspx?EventID=4389'
urlMatch=urlEvent+'&MatchNumber=4381'


bs=getWebpage(urlMatch)

if bs is None:
    exit

print(bs.find('', { 'class' : 'EventTitle'}).get_text())

for ph in bs.find_all('', { 'class' : 'PageHeading'}):
    print(ph.get_text())

for bk in bs.find_all('', { 'class' : 'bracket'}):
    print(bk.get_text())
    
    

resultsRow = bs.find('table', { 'class' : 'standings'}).find('tr').nextSibling.find_all('td')

dfResults = getMatchData(resultsRow)
print(dfResults)

"""