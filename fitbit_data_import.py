
# coding: utf-8


'''
Fitbit Data Analysis
Author: Sirajus Salayhin
Version: 1.0
Email: salayhin@gmail.com
'''


# Import Libraries
import fitbit 
import requests
import json
import pandas as pd
from time import sleep
import pdb
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, date, timedelta

#Initializing
#file_path = '/projects/data-engineering/fitbit/fitbit-data-collection/data/'
file_path = '/export/sc-ehealth01/fitbit/fitbit-data-analysys/data/'

#db_full_path = '/projects/data-engineering/fitbit/fitbit-data-collection/'
db_full_path = '/export/sc-ehealth01/fitbit/fitbit-data-collection/'


# Connection
def get_fit_bit_tokens():
    engine = create_engine('sqlite:///'+ db_full_path+ 'data-dev.sqlite')
    connection = engine.connect()
    my_query = 'SELECT * FROM fitbit_tokens'
    return connection.execute(my_query).fetchall()


def refresh_token():
    import base64
    client_id = '22DF9R'
    client_secret = 'e8879d2af117c1e2b41dd6a4a759992f'
    refresh_token = 'a5439b19bcc2a8f7bf8910bf7934549837b2f0d227a63dc4471d1cf004c4c0a6'
    t = client_id + ':' + client_secret
    auth_header = base64.urlsafe_b64encode(t.encode('UTF-8')).decode('ascii')

    headers = {
        'Authorization': 'Basic %s' % auth_header,
        'Content-Type' : 'application/x-www-form-urlencoded'
    }

    # Parameters for requesting tokens (auth + refresh)
    params = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'expires_in' : 31556926
    }

    resp = requests.post('https://api.fitbit.com/oauth2/token', data=params, headers=headers)

    return resp.json()
    

def _get_user_info(token):
    #token = 'eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyMkRGOVIiLCJzdWIiOiI3MjY4WUwiLCJpc3MiOiJGaXRiaXQiLCJ0eXAiOiJhY2Nlc3NfdG9rZW4iLCJzY29wZXMiOiJyc29jIHJhY3QgcnNldCBybG9jIHJ3ZWkgcmhyIHJudXQgcnBybyByc2xlIiwiZXhwIjoxNTQ3NjA3NjQyLCJpYXQiOjE1NDc1Nzg4NDJ9.Adkd6vxnqR_TXw6slsxcCvhmgHLrh-guWMqe6P7PUYo'
    #url = 'https://api.fitbit.com/1/user/-/activities/heart/date/' + date + '/1d/1sec/time/00:00/23:59.json'
    url = 'https://api.fitbit.com/1/user/-/profile.json'

    response = requests.get(url=url, headers={'Authorization':'Bearer ' + token})
    return response.json()

def _get_user_id(token):
    data = _get_user_info(token)
     
    if 'user' in data:
        return  data['user']['encodedId']
    elif data['success'] == False:
        return ''
       
def _get_daily_activity(token, user_id, date=None):

    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    else:
        date = date

    url = 'https://api.fitbit.com/1/user/'+ user_id +'/activities/date/' + date +'.json'
    print(url)

    response = requests.get(url=url, headers={'Authorization':'Bearer ' + token})
    return response.json()    

def _get_timeseries_data(token, user_id, resource, date=None):
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    else:
        date = date
    
    url = 'https://api.fitbit.com/1/user/-/'+ resource + '/date/' + str(date) + '/1m.json'
    print(url)
    
    response = requests.get(url=url, headers={'Authorization':'Bearer ' + token})
    return response.json() 

def _get_sleep_data(token, user_id, date=None):
    if date is None:
        yesterday = datetime.now() - timedelta(1)
        date = yesterday.strftime('%Y-%m-%d')
    else:
        date = date

    url = 'https://api.fitbit.com/1.2/user/-/sleep/date/'+ date +'.json'
    print(url)
    
    response = requests.get(url=url, headers={'Authorization':'Bearer ' + token})
    return response.json() 
         

def _get_heart_rate_data(token, user_id, date=None):
    if date is None:
        yesterday = datetime.now() - timedelta(1)
        date = yesterday.strftime('%Y-%m-%d')
    else:
        date = date

    #url = 'https://api.fitbit.com/1.2/user/-/sleep/date/'+ date +'.json'
    url = 'https://api.fitbit.com/1/user/-/activities/heart/date/'+ date +'/1m.json'
    print(url)
    
    response = requests.get(url=url, headers={'Authorization':'Bearer ' + token})
    return response.json() 


## Daily activity summary
def _get_daily_activity_summury(date=None):
    tokens = get_fit_bit_tokens()
    
    for token in tokens:
        
        user = _get_user_info(token[3])

        if 'user' in user:
            user_id =  user['user']['encodedId']
            user_fullname = user['user']['fullName']
            
            print("Import Daily Activity Data For: ")
            print({'user': user_id, 'fullname': user_fullname})
            
            data = _get_daily_activity(token[3], user_id, date)

            df = pd.DataFrame()
            df = df.append({'fitbit_user_id': user_id, 'fitbit_fullname': user_fullname,
                            'json_value': data}, ignore_index=True)

            df.to_csv(file_path + 'daily_activity_summary.csv', mode='a', header=False, sep=',', index=False,
                      encoding='utf-8') 
            
            
def _get_timeseries_steps_data(date=None):
    resource = 'activities/steps'
    tokens = get_fit_bit_tokens()
    
    for token in tokens:
        user = _get_user_info(token[3])

        if 'user' in user:
            user_id =  user['user']['encodedId']
            user_fullname = user['user']['fullName']
            print("Import Timeseries data for: ")
            print({'user': user_id, 'fullname': user_fullname})
            data = _get_timeseries_data(token[3], user_id, resource, date)

            df = pd.DataFrame()
            df = df.append({'fitbit_user_id': user_id, 'fitbit_fullname': user_fullname,
                            'json_value': data}, ignore_index=True)

            df.to_csv(file_path + 'timeseries_steps.csv', mode='a', header=False, sep=',', index=False,
                      encoding='utf-8') 
            

def _sleep_data(date=None):
    tokens = get_fit_bit_tokens()
    
    for token in tokens:
        user = _get_user_info(token[3])

        if 'user' in user:
            user_id =  user['user']['encodedId']
            user_fullname = user['user']['fullName']
            print("Import Data for: ")
            print({'user': user_id, 'fullname': user_fullname})
            data = _get_sleep_data(token[3], user_id, date)

            df = pd.DataFrame()
            df = df.append({'fitbit_user_id': user_id, 'fitbit_fullname': user_fullname,
                            'json_value': data}, ignore_index=True)

            df.to_csv(file_path + 'sleep_data.csv', mode='a', header=False, sep=',', index=False,
                      encoding='utf-8') 
            
            
def _heart_rate_data(date=None):
    tokens = get_fit_bit_tokens()
    
    for token in tokens:
        user = _get_user_info(token[3])

        if 'user' in user:
            user_id =  user['user']['encodedId']
            user_fullname = user['user']['fullName']
            print("Import Data for: ")
            print({'user': user_id, 'fullname': user_fullname})
            data = _get_heart_rate_data(token[3], user_id, date)

            df = pd.DataFrame()
            df = df.append({'fitbit_user_id': user_id, 'fitbit_fullname': user_fullname,
                            'json_value': data}, ignore_index=True)

            df.to_csv(file_path + 'heart_rate_data.csv', mode='a', header=False, sep=',', index=False,
                      encoding='utf-8') 
            
            


if __name__== "__main__":
    ## If you use any date it will retrive specific date data, otherwise it will retrive current days data  
    print("Getting daily activity data import in progress")          
    _get_daily_activity_summury()


    # Load Daily Activity CSV File
    #pd.read_csv(file_path+'daily_activity_summary.csv',  names=['Name', 'Fitbit ID', 'Data'])

                
    ## It will give us last one montth data  
    print("Time series steps data import In progress") 
    _get_timeseries_steps_data()


    # Load Timeseries Steps CSV File
    #pd.read_csv(file_path+'timeseries_steps.csv',  names=['Name', 'Fitbit ID', 'Data'])


    ## It will give us yesterday's sleep data if no date provided
    print("Sleep Data Import In progress") 
    _sleep_data()


    # Load Sleep CSV File
    #pd.read_csv(file_path+'sleep_data.csv',  names=['Name', 'Fitbit ID', 'Data'])


    ## It will give us yesterday's sleep data if no date provided
    print("Heart rate Import In progress") 
    _heart_rate_data()


    # Load Heart Rate CSV File
    #pd.read_csv(file_path+'heart_rate_data.csv',  names=['Name', 'Fitbit ID', 'Data'])

