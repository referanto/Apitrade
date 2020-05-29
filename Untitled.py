#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3


# In[1]:


import requests
import psycopg2
import time
import sched
import json
from datetime import datetime, timedelta
from pandas.tseries.offsets import Day, BDay


# ### Define business day or weekend

# In[2]:


def is_bday(x):
    return x == x + Day(1) - BDay(1)


# ## Define Class token
# propety - access_token, refresh_token <br>
# method - refresh (refresh tokens)

# In[3]:


class token():
    #client_id = 'C60QCIA1CLLBSVT2KOO26A0YO955JP8A'
    client_id = 'YXNZWUC2EURVYEEGICX0XROGDANGKKOY'

    def read_config(self):
        with open('config.json', 'r') as f:
            # return pa_token
            return json.loads(f.read())
        
    def write_config(self, pa_token):
        with open ("config.json", 'w') as f:
             f.write(json.dumps(pa_token))
                
    def __init__(self):
        pa_token = self.read_config()
        self.access_token = pa_token['access_token']
        self.refresh_token = pa_token['refresh_token']
    
    

    
    def refresh(self):
        # get refresh token and new access token, point https://api.tdameritrade.com/v1/oauth2/token

        # define endpoint
        url = r'https://api.tdameritrade.com/v1/oauth2/token'

        # define headers
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        # define payload
        payload = {'grant_type': 'refresh_token',
           'refresh_token': self.refresh_token,
           'access_type': 'offline',
           'client_id': self.client_id, 
           'redirect_uri':  'http://localhost'}

        # post the  data to get the token
        authreplay = requests.post(url, headers=headers, data=payload)

        # show the status code, we want 200
        if authreplay.status_code == 200:
            print('Token refreshed. Status code ', '200')

            # convert json to dictionary
            pa_token = authreplay.json()

            # get access tokem (expires in 30 min) and refresh token (expires in 90 days)
            self.access_token = pa_token['access_token']
            self.refresh_token = pa_token['refresh_token']
            
            # write pa_token to file
            self.write_config(pa_token)

        else:
            print('Bad request! (Token did not refresh)')
            print(' Error', authreplay.status_code)
            print(authreplay.json())
            with open('refresh_token.log', 'a') as f:
                f.write(str(datetime.now()) + ' - Status code: ' + str(authreplay.status_code) + '\n')


# ### Refresh token every 30 minutes

# In[4]:


def repeat_refresh_token(token_class, scheduler):
    token_class.refresh()
    
    scheduler.enter(1700, 1, repeat_refresh_token, (token_class, scheduler,))


#   ### Get last price from TD Ameritrade

# In[5]:


def get_last_price():
    headers = {'Authorization': 'Bearer {}'.format(my_token.access_token)}
    
    #define our endpoint
    endpoint = r'https://api.tdameritrade.com/v1/marketdata/{}/quotes'.format('AAPL')

    #make a request
    try:
        content = requests.get(url = endpoint, headers=headers)
    except requests.exceptions.RequestException as e:  
        print(content.status_code, e)
        raise SystemExit(e)    
    
    
    # show the status code, we want 200
    if content.status_code == 200:
        #convert json to a dictionary
        return content.json()
    else:
        with open('get_last_price.log', 'a') as f:
            f.write(str(datetime.now()) + ' - ' + str(content.status_code) + str(content.json()) + '\n')


# ## Get several last prices from TD Ameritrade

# In[6]:


def get_last_prices(tickers):       # tickers in format 'AAPL,TSLA,GOLD'
    headers = {'Authorization': 'Bearer {}'.format(my_token.access_token)}
    
    #define our endpoint
    endpoint = r'https://api.tdameritrade.com/v1/marketdata/quotes'
    
    payload = {'symbol': tickers}

    #make a request
    try:
        content = requests.get(url = endpoint, headers=headers, params=payload)
    except requests.exceptions.RequestException as e:  
        print(content.status_code, e)
        raise SystemExit(e)    
    
    
    # show the status code, we want 200
    if content.status_code == 200:
        #convert json to a dictionary
        return content.json()
    else:
        with open('get_last_prices.log', 'a') as f:
            f.write(str(datetime.now()) + ' - ' + str(content.status_code) + str(content.json()) + '\n')


# ### INSERT DATA TO DB

# In[7]:


#Create a connection to the PostgreSQL database
def insert_data():
    
    try:
        connection = psycopg2.connect(database="mydb", user='ser', password='ser', host='localhost', port='')

        #Create a cursor connection object 
        cursor = connection.cursor()
    
        #Get the column name of a table inside the database and put some values
        pg_insert = """ INSERT INTO aapl (date, bidprice, askprice, lastprice, lastsize)
                    VALUES (%s,%s,%s,%s,%s)"""
    
        # Add some data        
        data = get_last_price()
       
        pg_values = (data['AAPL']['tradeTimeInLong'], 
                         data['AAPL']['bidPrice'], 
                         data['AAPL']['askPrice'], 
                         data['AAPL']['lastPrice'], 
                         data['AAPL']['lastSize'])
        
        #Execute the pg_insert SQL 
        cursor.execute(pg_insert, pg_values)
    
        # Save a data
        connection.commit()

    
    #Handle the error throws by the command that is useful when using python while working with PostgreSQL
    except(Exception, psycopg2.Error) as error:
        print("Error connecting to PostgreSQL database:", error)
        print('data = ', data)
        connection = None
        
        with open('insert_data.log', 'a') as f:
            f.write(str(datetime.now()) + ' - Error:' + str(error) + ', Data = ' + str(data) + '\n')

    #Close the database connection
    finally:
        if(connection != None):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is now closed")


# ## INSERT DATA to 3 TABLES

# In[8]:


def insert_datas(): 
    
    #Create a connection to the PostgreSQL database
    try:
        # Add some data        
        data = get_last_prices('AAPL,TSLA,GOLD')
        
        connection = psycopg2.connect(database="mydb", user='ser', password='ser', host='localhost', port='')

        #Create a cursor connection object 
        cursor = connection.cursor()
    
        #Get the column name of a 1 table inside the database and put some values
        pg_insert = """ INSERT INTO aapl (date, bidprice, askprice, lastprice, lastsize, totalvolume)
                    VALUES (%s,%s,%s,%s,%s,%s)"""
    
        pg_values = (data['AAPL']['tradeTimeInLong'], 
                     data['AAPL']['bidPrice'], 
                     data['AAPL']['askPrice'], 
                         data['AAPL']['lastPrice'], 
                         data['AAPL']['lastSize'],
                        data['AAPL']['totalVolume'])
                
        #Execute the pg_insert SQL 
        cursor.execute(pg_insert, pg_values)

        #Get the column name of a 2 table inside the database and put some values
        pg_insert = """ INSERT INTO tsla (date, bidprice, askprice, lastprice, lastsize, totalvolume)
                    VALUES (%s,%s,%s,%s,%s,%s)"""
       
        pg_values = (data['TSLA']['tradeTimeInLong'], 
                         data['TSLA']['bidPrice'], 
                         data['TSLA']['askPrice'], 
                         data['TSLA']['lastPrice'], 
                         data['TSLA']['lastSize'],
                        data['TSLA']['totalVolume'])
        
        #Execute the pg_insert SQL 
        cursor.execute(pg_insert, pg_values)
        
        #Get the column name of a 3 table inside the database and put some values
        pg_insert = """ INSERT INTO gold (date, bidprice, askprice, lastprice, lastsize, totalvolume)
                    VALUES (%s,%s,%s,%s,%s,%s)"""
       
        pg_values = (data['GOLD']['tradeTimeInLong'], 
                         data['GOLD']['bidPrice'], 
                         data['GOLD']['askPrice'], 
                         data['GOLD']['lastPrice'], 
                         data['GOLD']['lastSize'],
                        data['GOLD']['totalVolume'])
        
        #Execute the pg_insert SQL 
        cursor.execute(pg_insert, pg_values)
    
        # SAVE a DATA
        connection.commit()

    
    #Handle the error throws by the command that is useful when using python while working with PostgreSQL
    except(Exception, psycopg2.Error) as error:
        print("Error connecting to PostgreSQL database:", error)
        print('data = ', data)
        connection = None
        
        with open('insert_datas.log', 'a') as f:
            f.write(str(datetime.now()) + ' - Error:' + str(error) + ', Data = ' + str(data) + '\n')


    #Close the database connection
    finally:
        if(connection != None):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is now closed")   


# ### Repeat insert to DB every 0.5 sec

# In[13]:


def repeat_insert_data(scheduler):
    
    # If now a working time and busines day ==> insert data
    # repeat function since 0.5 sec
    now = datetime.now()
    now_time = datetime.now().time()
    open9_30am = now_time.replace(hour=7, minute=0, second=0, microsecond=0)
    close4pm = now_time.replace(hour=20, minute=0, second=0, microsecond=0)
    if (open9_30am < now_time < close4pm) and (is_bday(now)):
        insert_data()
        
    scheduler.enter(0.5, 1, repeat_insert_data, (scheduler,))


# ### Repeat insert to 3 tables DB every 0.5 sec

# In[14]:


def repeat_insert_datas(scheduler):
    
    # If now a working time and busines day ==> insert data
    # repeat function since 0.5 sec
    now = datetime.now()
    now_time = datetime.now().time()
    open9_30am = now_time.replace(hour=7, minute=0, second=0, microsecond=0)
    close4pm = now_time.replace(hour=20, minute=0, second=0, microsecond=0)
    if (open9_30am < now_time < close4pm) and (is_bday(now)):
        insert_datas()
        
    scheduler.enter(0.5, 1, repeat_insert_datas, (scheduler,))


# ## Main code:

# In[11]:


my_token = token()
my_scheduler = sched.scheduler(time.time, time.sleep)


# In[17]:


my_scheduler.enter(1, 1, repeat_refresh_token, 
                   (my_token, my_scheduler,))
my_scheduler.enter(2, 1, repeat_insert_datas, (my_scheduler,))
my_scheduler.run()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




