import requests
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import logging
import pymysql
from sqlalchemy import create_engine
import mysql.connector
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pandas_schema import Column, Schema
from pandas_schema.validation import *


# docker-compose exec starwarsservercontainer ./launch-starwarsserver.sh
import ptvsd

ptvsd.enable_attach(address = ('0.0.0.0', 3000))
print("Waiting for debug attach")
ptvsd.wait_for_attach()

#create object logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('bitacora.log')
handler.setLevel(logging.INFO)


# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the file handler to the logger
logger.addHandler(handler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)   


#SESSION
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504, 503),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

#STING SEPARATION
def extract(st):
    
    partes = st.split("/")
    res=partes[5]
    return res


#CONNECT FUNCTION:
def getData(url):
    finalList=[]
    nxt=""
    count=0
    try:
        response = requests_retry_session().get(url)
        logger.info('Success Connection %s', url) 
        if response.status_code != 200:
            raise Exception("Error in response: Code error-",response.status_code)
        #GET DATA LIST AND NEXT URL
        data=response.json()
        nxt=data['next']
        count=data['count']
        #PUSH INTO LIST
        finalList.extend(data['results'])
        while True:  
            if nxt is None:
                break
            response = requests_retry_session().get(nxt)
            data=response.json()
            nxt=data['next']
            
            if nxt is None:

                finalList.extend(data['results'])
                break
                
            finalList.extend(data['results'])
        df=json_normalize(finalList)
        #print('Initial count:',count)
        #print('Initial len:',len(df))
        if(count!=len(df)):
            raise Exception("The counts values don't match")
        else:
            return df
    except Exception as e:
        logger.error('Connection error%s', e)
        

#MODIFY DATAFRAMES
def organiseData(df):
    #PLANETS
    if 'diameter' in df: 
        #CLEAN UP

        df.drop(['terrain', 'surface_water','created','edited','residents','films'], axis=1, inplace=True)
          
    #CHARACTERS    
    elif 'gender' in df: 
        df.drop(['mass', 'hair_color','skin_color','eye_color','birth_year','species','vehicles','starships','created','edited','url'], axis=1, inplace=True)  
          
    #FILMS
    else:         
        df.drop(['opening_crawl', 'characters','planets','starships','vehicles','species','created','edited','url'], axis=1, inplace=True)  


    return df

def conection(tableName):

        readed = pd.read_sql_table(tableName, con=engine)
        if(len(readed)>0):          
            query = ('TRUNCATE %s' % tableName)
            cursor.execute(query)

            
        if tableName=='planets':
            planets.to_sql(tableName, con = engine, if_exists = 'append', chunksize = len(planets),index=False)
            readed = pd.read_sql_table('planets', con=engine)
            if len(readed) == len(planets):
                logger.info('Planets insertion equal to the original data: %s', len(planets))
            else:
                raise Exception('Error at the sql planets query')
            
        elif tableName=='characters':
            characters.to_sql(tableName, con = engine, if_exists = 'append', chunksize = len(characters),index=False)
            readed = pd.read_sql_table('characters', con=engine)
            if len(readed) == len(characters):
                logger.info('Characters insertion equal to the original data: %s', len(characters))
            else:
                raise Exception('Error at the sql characters query')
        elif tableName=='films':
            films.to_sql(tableName, con = engine, if_exists = 'append', chunksize = len(films),index=False)
            readed = pd.read_sql_table('films', con=engine)
            if len(readed) == len(films):
                logger.info('Films insertion equal to the original data: %s', len(films))
            else:
                raise Exception('Error at the sql films query')   
        elif tableName=='relationFC':
            relationFC.to_sql(tableName, con = engine, if_exists = 'append', chunksize = len(relationFC),index=True) 
            readed = pd.read_sql_table('relationFC', con=engine)
            if len(readed) == len(relationFC):
                logger.info('relationFC insertion equal to the original data: %s', len(relationFC))
            else:
                raise Exception('Error at the sql relationFC query')
        else:
            raise Exception('Not such table in the BD')

        return readed

try:
    #PLANETS
    url ="https://swapi.co/api/planets"   
    df1 = getData(url)
    planets = organiseData(df1)
    planets['url']=planets['url'].map(extract)
    planets.rename(columns={'url': 'id_planet'}, inplace=True)
    logger.info('Planets extraction count: %s', len(planets)) 
    
    #PEOPLE
    url2 ="https://swapi.co/api/people"
    df2 = getData(url2)
    characters = organiseData(df2)
    logger.info('Characters extraction count: %s', len(characters)) 
    
    #FILMS
    url3 ="https://swapi.co/api/films"
    df3 = getData(url3)
    films = organiseData(df3)
    logger.info('Films extraction count: %s', len(films)) 

except Exception as e:
    logger.error('Error in extraction')


#CREATE STRUCTURE OF RELATION TABLE
conv = { 'https://swapi.co/api/films/2/': 5, 'https://swapi.co/api/films/1/': 4, 'https://swapi.co/api/films/3/': 6 , 'https://swapi.co/api/films/4/':1, 'https://swapi.co/api/films/5/':2,'https://swapi.co/api/films/6/':3,'https://swapi.co/api/films/7/': 7}
characters['films'] = characters.films.apply(lambda row: [conv[v] for v in row if conv.get(v)])
relationFC = characters['films'].apply(lambda x: pd.Series(x[0])).stack().reset_index(level=1, drop=True).to_frame('films').join(characters[['name']], how='left')
relationFC.columns = ['id_film', 'id_character']

#ADAPT CHARACTERS TABLE
characters['homeworld'] = characters['homeworld'].map(extract)
del characters['films']



#CHECK SYNTAX
try:
    schema = Schema([
        Column('name', [IsDtypeValidation(np.dtype('O')),MatchesPatternValidation(r'^[a-zA-Z, ]*$')]),
        Column('rotation_period', [IsDtypeValidation(np.dtype('O'))]),
        Column('orbital_period', [IsDtypeValidation(np.dtype('O'))]),
        Column('diameter', [IsDtypeValidation(np.dtype('O'))]),
        Column('climate', [IsDtypeValidation(np.dtype('O')),MatchesPatternValidation(r'^[a-zA-Z0-9, ]*$')]),
        Column('gravity', [IsDtypeValidation(np.dtype('O')),MatchesPatternValidation(r'^[a-zA-Z0-9,.()/ ]*$'),LeadingWhitespaceValidation()]),
        Column('population', [IsDtypeValidation(np.dtype('O'))]),
        Column('id_planet', [InRangeValidation(0, 500)]),
    ])
    schema1 = Schema([
        Column('name', [IsDtypeValidation(np.dtype('O'))]),
        Column('gender', [IsDtypeValidation(np.dtype('O')),MatchesPatternValidation(r'^[a-zA-Z,/()]*$'),LeadingWhitespaceValidation()]),
        Column('homeworld', [IsDtypeValidation(np.dtype('O')),LeadingWhitespaceValidation()]),
        Column('height', [IsDtypeValidation(np.dtype('O')),LeadingWhitespaceValidation()]),
    ])
    schema2 = Schema([
        Column('title', [IsDtypeValidation(np.dtype('O')),LeadingWhitespaceValidation()]),
        Column('episode_id', [IsDtypeValidation(np.dtype('int64'))]),
        Column('director', [IsDtypeValidation(np.dtype('O'))]),
        Column('producer', [IsDtypeValidation(np.dtype('O'))]),
        Column('release_date', [IsDtypeValidation(np.dtype('O'))]),
    ])
    schema3 = Schema([
        Column('id_character', [IsDtypeValidation(np.dtype('O')),MatchesPatternValidation(r'^[a-zA-Z,/()]')]),
        Column('id_film', [IsDtypeValidation(np.dtype('int64')),LeadingWhitespaceValidation(),MatchesPatternValidation(r'^[0-9,/]')]),
    ])

    errors = schema.validate(planets)
    for error in errors:
        print(error)
        raise ValueError("There are " + str(len(errors)) )
    errors = schema1.validate(characters)
    for error in errors:
        print(error)
        raise ValueError("There are " + str(len(errors)) )
    errors = schema2.validate(films)
    for error in errors:
        print(error)    
        raise ValueError("There are " + str(len(errors)) )
    errors = schema3.validate(relationFC)
    for error in errors:
        print(error)
        raise ValueError("There are " + str(len(errors)) )
        

except Exception as e:
    logger.error("Sytax error at the table%s",e)



#CREATE ENGINE TO INSERT
try:
    cnx = mysql.connector.connect(user='root', password='secretD',
                                  host='mysql-database',port='3306', database='starWars')


    engine = create_engine("mysql+pymysql://{user}:{pw}@mysql-database:3306/starWars"
                           .format(user="root",
                                   pw="secretD"))
    engine.execute("USE starWars")

except Exception as e:
    print(e)



#INSERT CHARACTERS TABLE
try:
   tables = {0: "planets", 1: "films", 2: "characters",3:"relationFC"}
   i=0
   cursor = cnx.cursor()
   while(i<4):
       res = conection(tables[i])
       i=i+1
   cursor.close()
   cnx.close()
except Exception as e:
    logger.error("Fatal error in inserting tables%s",e)
finally:
    logger.info("Insert completed successfully")