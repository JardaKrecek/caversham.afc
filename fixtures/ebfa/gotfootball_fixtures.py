import ebfa_core

from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

# Imports the Google Cloud client library
from google.cloud import storage

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

def getGroupFixtures(eventId: str, groupId: str) -> pd.DataFrame:
    urlSite='https://www.gotfootball.co.uk'
    urlEvent=urlSite+'/events/schedule.aspx?EventID='+eventId
    urlGroup=urlEvent+'&GroupID='+groupId+'&print=true'
    
    bsGroupFixtures = ebfa_core.getWebpage(urlGroup)

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

    columnNames = ['MatchId', 'Date', 'Home', 'HG', 'AG', 'Away', 'Notes']

    df = pd.DataFrame(fixtures, columns=columnNames)
    df.set_index('MatchId', inplace=True)
    return(df)


def fixtures_check(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    print("This Function was triggered by messageId {} published at {}".format(context.event_id, context.timestamp))

    columnDTypes = {'MatchId':str, 'Date':str, 'Home':str, 'HG':str, 'AG':str, 
                    'Away':str, 'Notes':str}

    sEventId = '4389'
    sGroupId = '7590'

    sBucketUrl='ebfa_fixtures'
    sBlobName='4389-7590-LatestFixtures.csv'
    sLastBlob_uri = f'gs://{sBucketUrl}/{sBlobName}'

    # it is mandatory initialize the storage client
    storage_client = storage.Client()

    dfLast = pd.read_csv(sLastBlob_uri, dtype=columnDTypes)

    dfLast.set_index('MatchId', inplace=True)
    dfLast.sort_index(inplace=True)

    dfCurrent = getGroupFixtures(sEventId, sGroupId)
    dfCurrent.sort_index(inplace=True)


    dfChanges = dfLast.compare(dfCurrent, keep_shape=False, keep_equal=False)
    if dfChanges.empty:
        print('No changes')
    else:
        dfChanges.columns = [f'{f}_old' if s == 'self' else (f'{f}_new' if s == 'other' else f'{f}') for f, s in dfChanges.columns]
        
        sTimestamp = datetime.now().strftime('%Y-%m-%dT%H%M%S')
        dfChanges.insert(0, 'Updated', sTimestamp)
        dfChangeLog = dfChanges.join(dfLast)
        dfChangeLog2 = dfChangeLog.append(dfChanges.join(dfCurrent))
        dfChangeLog2.sort_index(inplace=True)

        # dfChangeLog2.to_csv(sOutFolder+sEventId+'-'+sGroupId+'-'+sTimestamp+'-Updates.csv')
        sUpdatedOutBlob = 'gs://'+sBucketUrl+'/'+sEventId+'-'+sGroupId+'-'+sTimestamp+'-Updates.csv'
        
        print(f'Detected changes. Saving to {sUpdatedOutBlob}')
        
        dfChangeLog2.to_csv(sUpdatedOutBlob)

        # dfCurrent.to_csv(sOutFolder+sEventId+'-'+sGroupId+'-LatestFixtures.csv')
        dfCurrent.to_csv(sLastBlob_uri)
        print(f'{sLastBlob_uri} updated. Changes saved to {sUpdatedOutBlob}')


"""    
    if 'data' in event:
        filename = base64.b64decode(event['data']).decode('utf-8')
        temp = pd.read_csv(filename, encoding='utf-8')
        print (temp.head())
    else:
        print('No data passed in.')
"""