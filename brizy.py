"""
brizy
Get station details for all measuring stations as reported by the website *Aqualim*.

This python 3 module:
- E -  scrapes info from http://aqualim.environnement.wallonie.be ;
- T -  forms a nice table around it;
- L -  stores the table(s) in local scv files;
- L -  stores the table(s) in a SQL RDBMS;

Online software repository: [GitHub](https://github.com/riklmr/brizy).
"""
import sys
sys.path.append('../chaudfontaine')

import json
import re
import time
import urllib.error 
import urllib.request

import bs4 as bs
#from bs4 import BeautifulSoup
from selenium import webdriver

import pandas as pd
import psycopg2

import chaudfontaine


CONNECTION_DETAILS = "dbname='meuse' user='postgres' password='password' host='localhost' port='5555'"
GECKO_PATH = '../geckodriver/geckodriver'

options = webdriver.firefox.options.Options()
options.headless = False

loginpage = f"http://aqualim.environnement.wallonie.be"
# print(loginpage)

listpage = "http://aqualim.environnement.wallonie.be/GeneralPages.do?method=displayStationsList"
# print(listpage)


def get_stations_www():
    """
    Creates a dataframe with all available Aqualim stations.
    """
    driver = webdriver.Firefox(firefox_options=options, executable_path = GECKO_PATH)

    driver.get(loginpage)
    driver.get(listpage)

    select_datatable_length = webdriver.support.ui.Select(driver.find_element_by_name("dataTable_length"))
    select_datatable_length.select_by_value("-1")

    datatable = driver.find_element_by_id("dataTable")

    df = pd.read_html(datatable.get_attribute('outerHTML'), index_col=3)
    driver.quit()

    return df

def parseMesure(soup):
    """
    Returns: station details in JSON or None when appropriate
    Parameter: parsed html (soup in BeautifulSoup parlance)
    """
    stationDetails = None

    # The station details are in a html <dl></dl> structure or "description list",
    # but we do not know which dl because it is not identified by the website.
    # So we find all the dl's and search for the one that has 'Station' in
    # one of the <dt></dt> terms.

    # Do NOT assume there exists a 1:1 relation from dt to dd.
    # This website sometimes has a 1:n relation for dt:dd (Hauteur in particular) where sometimes n=0.
    # case in point:
    """
    <dl>
        <dt>Station</dt>
        <dd> brizy            </dd>
        <dt>Rivière</dt>
        <dd>HOEGNE              </dd>
        <dt>Coordonnées Lambert (x, y)</dt>
        <dd>262860, 134258</dd>
        <dt>Référence du zéro</dt>

        <dd>357.32 m (DNG) (21/10/1998 - ..)</dd>                      <<< 0, 1 or more entries in this list :-(
        <dd>357.32 m (DNG) (22/06/1993 - 20/10/1998)</dd>

        <dt>Date d'installation</dt>
        <dd>22/06/1993</dd>
    </dl>
    """


    allDescriptionLists = soup.find_all('dl')

    if len(allDescriptionLists) > 0:
        for dl in allDescriptionLists:
            first_tag = dl.find_next(['dt', 'dd'])
            if first_tag.text == 'Station':
                stationDetails = {}
                description = None
                for tag in dl.find_all(['dt', 'dd']):
                    if tag.name == 'dt':
                        description = tag.text.strip()
                        # fill the key/value in the dict with a None value
                        # this is our fall back if we do not encounter a valid description for it
                        stationDetails[description] = None
                    elif tag.name == 'dd':
                        if description != None:
                            # remove leading and trailing whitespace from all descriptions
                            stationDetails[description] = tag.text.strip()
                            # only accept the first dd tag in a list of potentially many dd's
                            # reset description to None, so we visit this branch max 1x per <dt>
                            description = None
                        #
                    #
                #
            # turn dict into json string
            stationDetails = json.dumps(stationDetails)
            # print(stationDetails)
            break # allDescriptionLists
        #
    else:
        print('no stationDetails found in this soup')

    return stationDetails

def retrieveMesure(combi_code):
    """
    Contacts webserver for station details.
    Returns station details as created by parseMesure()
    Parameter: station code string (4 digit station identifier + 4 digit quantity code)
    """

    # we ask the website for printable output (xt=prt) because it is cleaner and easier to parse
    this_url = f'http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Actuelle/crue/mesure.jsp?code={combi_code}&xt=prt'
    stationDetails = None

    # define a request object
    req = Request(this_url)
    try:
        # open the http connection with the server
        _ = urlopen(req)
    # catch a few observed exceptions
    except HTTPError as e:
        print(f'skipping {combi_code} cuz http Error code: ', e.code)
    except URLError as e:
        print(f'skipping {combi_code} cuz url Reason: ', e.reason)
    else:
        # read the raw html from the connection (sauce in BeautifulSoup parlance)
        sauce = urlopen(this_url).read()
        # parse the sauce into soup
        soup = bs.BeautifulSoup(sauce, 'lxml')
        # parse the soup into a JSON object (or None)
        stationDetails = parseMesure(soup)
        if stationDetails == None:
            print(this_url)

    return stationDetails

def retrieveStations(stationCodes, wantedQuantity):
    """
    Returns a new df with all stations in the given list 
    with all their yummy details.
    parameters:
     stations_Codes: list of strings representing 4-digit station codes
     wantedQuantity: string (one of the keys in dict QUANTITY_CODES)
    """
    print(f'retrieving details for {len(stationCodes)} stations with {wantedQuantity} sensor...')
    # translate french word into string (4 digit code)
    wantedQuantityCode = QUANTITY_CODES[wantedQuantity]
    # start an empty dataframe
    df = pd.DataFrame(index=stationCodes)
    df.index.name = 'code'

    # iterate over all requested stationCodes
    for stationCode in stationCodes:
        # combine the two codes into one 8 digit code
        combi_code = f'{stationCode}{wantedQuantityCode}'
        print(combi_code)

        # do the actual scraping
        stationDetailsString = retrieveMesure(combi_code)
        # store in the df the station details in a string as they came to us
        df.loc[stationCode, 'stationDetails'] = stationDetailsString

        # go over the individual details and translate them, clean them up, refactor them, etc.
        # fieldnames in df are english and camelCase
        if stationDetailsString != None:
            stationDetailsJSON = json.loads(stationDetailsString)
            # station name
            df.loc[stationCode, 'name'] = stationDetailsJSON['Station']

            # river name
            df.loc[stationCode, 'river'] = stationDetailsJSON['Rivière']

            # coordinates x,y of the station (Lambert Belgium EPSG 31370)
            lambert = stationDetailsJSON['Coordonnées Lambert (x, y)']
            [x, y] = lambert.split(', ')
            df.loc[stationCode, 'x'] = x
            df.loc[stationCode, 'y'] = y

            # date of installation of the station Date d'installation
            if "Date d\'installation" in stationDetailsJSON:
                # print(stationDetailsJSON["Date d\'installation"])
                df.loc[stationCode, 'installationDate'] = stationDetailsJSON["Date d\'installation"]
                # print(df.loc[stationCode, 'installationDate'])

            # altitude of the station
            if "Altitude" in stationDetailsJSON:
                altitudeString = stationDetailsJSON["Altitude"]
                altitude = float(re.sub(r'\s+m', '', altitudeString))
                df.loc[stationCode, 'altitude'] = altitude

            # surface area of relevant drainage basin (square km)
            if "Superficie du bassin versant" in stationDetailsJSON:
                areaString = stationDetailsJSON["Superficie du bassin versant"]
                watershedArea = int(re.sub(r'\s+km2', '', areaString))
                df.loc[stationCode, 'watershedArea'] = watershedArea

            # Référence du zéro, reference level/altitude
            # DNG = https://fr.wikipedia.org/wiki/Deuxième_nivellement_général
            # 2.33m below NAP = https://nl.wikipedia.org/wiki/Normaal_Amsterdams_Peil
            if "Référence du zéro" in stationDetailsJSON:
                referenceLevelString = stationDetailsJSON["Référence du zéro"]
                if referenceLevelString == None:
                    df.loc[stationCode, 'referenceLevel'] = None
                else:
                    reMatch = re.match(r"(\d+\.?\d*)\s*m", referenceLevelString)
                    df.loc[stationCode, 'referenceLevel'] = float(reMatch.groups()[0])
            #
        else:
            # we got no stationDetails at all, let's just enter the station without any details (issue #8)
            # df.drop(stationCode, axis=0, inplace =True) ## OLD CODE before #8
            # no new code required?! SMILE
            pass
        # wait a moment before moving on, as not to overload the webserver
        time.sleep(.5)
    #
    return df

def insert_records_station(df, station_type):
    """
    Stores stations from a dataframe to postgres RDBMS (postgis).
    This assumes the table is already created in the DB.
    This also assumes the column names in the df and the DB table are the same (camelCase notWithStanding).
    """
    etl = chaudfontaine.Chaudfontaine()
    quantity_ids = etl.get_quantity_ids_db()

    # table_name = f"wallonie.station_{station_type}"

    conn = psycopg2.connect(CONNECTION_DETAILS)
    cursor = conn.cursor()
    print("connected to database meuse")

    print("start inserting/updating stations")


    # SELECT ST_SetSRID(ST_MakePoint(-71.1043443253471, 42.3150676015829),4326)
    # SELECT ST_SetSRID(ST_MakePoint(x, y),31370)

    q = """
    INSERT INTO wallonie.station (
        code, 
        name, 
        quantity_id, 
        x, 
        y, 
        river, 
        altitude, 
        installationDate, 
        referenceLevel, 
        watershedArea, 
        the_geom, 
        stationDetails)
    VALUES (
        %(code)s, 
        %(name)s, 
        %(quantity_id)s, 
        %(x)s, 
        %(y)s, 
        %(river)s, 
        %(altitude)s, 
        %(installationDate)s, 
        %(referenceLevel)s, 
        %(watershedArea)s, 
        public.ST_SetSRID( public.ST_MakePoint(%(x)s, %(y)s) , 31370),
        %(stationDetails)s)
    ON CONFLICT (code, quantity_id) DO
        UPDATE SET
            code = EXCLUDED.code, 
            name = EXCLUDED.name, 
            quantity_id = EXCLUDED.quantity_id, 
            x = EXCLUDED.x, 
            y = EXCLUDED.y, 
            river = EXCLUDED.river, 
            altitude = EXCLUDED.altitude, 
            installationDate = EXCLUDED.installationDate, 
            referenceLevel = EXCLUDED.referenceLevel, 
            watershedArea = EXCLUDED.watershedArea, 
            the_geom = public.ST_SetSRID(public.ST_MakePoint(EXCLUDED.x, EXCLUDED.y), 31370), 
            stationDetails = EXCLUDED.stationDetails
    ;
    """
    # print(q)

    dtypes_map = {
        'code': 'int',
        'x': 'int',
        'y': 'int',
        'altitude': 'float',
        'installationDate': 'date',
        'referenceLevel': 'float',
        'watershedArea': 'int',
    }

    # iterating over the dataframe index
    for i in df.index:
        # pandas series object to dict
        v = {
            'code': i,
            'name': None,
            'quantity_id': quantity_ids[station_type],
            'x': None,
            'y': None,
            'river': None,
            'altitude': None,
            'installationDate': None,
            'referenceLevel': None,
            'watershedArea': None,
            'stationDetails': None,
        }
        # psycopg2.ProgrammingError: can't adapt type 'numpy.int64' 
        # turns out: numpy types are not natively supported by psycopg2
        # possible solutions:
        # 1) automatically call an adapter
        #   http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax
        # 2) cast the numpy stuff to ordinary python types before calling psycopg2
        # let's try the 2) casting outside of pandas, because inside pandas everything is forced into numpy types :-(
        # at the same time, let's correct some NaN situations
        # print(df.loc[i])
        for c in df.columns:
            v[c] = df.loc[i, c]
            # correct the dtype
            if c in dtypes_map.keys():
                try:
                    if dtypes_map[c] == 'int':
                        v[c] = int(v[c])
                    elif dtypes_map[c] == 'float':
                        v[c] = float(v[c])
                    elif dtypes_map[c] == 'date':
                        v[c] = time.strftime('%Y-%m-%d', time.strptime(v[c], '%d/%m/%Y'))
                except ValueError:
                    v[c] = None
                except TypeError:
                    v[c] = None
        #
        # print(v)
        cursor.execute(q, v)  


    conn.commit()
    print("row(s) inserted")

    cursor.close()
    conn.close()
    print("connection closed")

def scrape_stations(stations_www, station_type):
    """
    Scrapes website for stationdetails.
    Parameters:
        stations_www: dataframe holding a list of stations of all types
        station_type: requested type of station
    """
    print("scrape")
    codes = stations_www.index[stations_www[station_type] == station_type]
    df = retrieveStations(codes, station_type)
    print(f"retrieved {len(df)} stations of type {station_type}")
    return df

