# -*- coding: utf-8 -*-
"""
Created on Thu Jan 15 11:02:30 2026

@author: Peter Matthews (p.c.matthews@durham.ac.uk)

Illustration for SQL access to wind turbine SCADA database, producing power curve

Also extracts list of farms readily available (basic JSON encoding), date ranges for each farm, and turbine list at each farm
"""


import sqlalchemy as sql
import pandas as pd
import json

import matplotlib.pyplot as plt


## GLOBAL SETUP

## Get SQL server details from JSON file

with open('../sql_cred.json') as f:
    sql_cred = json.load(f)

print ("SQL credentials:")
print (sql_cred)


## COLUMN NAMES ACROSS DATABASE TABLES
# DBname, Table, WT (name), Date, WindSp, Pwr

with open('sql_colmap.json') as f:
    sql_cm = json.load(f)




def get_power(date_start, date_end, wt, WF_DB):
    """
    Get the wind power data from specified turbine
    No error checking is done, so make sure that the wind turbine name is correct, and the date range is suitable
    
    Parameters
    ----------
    date_start : date string
        Start date for data collection
    date_end : date string
        End date for data collection
    wt : string
        Wind Turbine name
    WF_DB: string
        Name of wind farm database

    Returns
    -------
    Panda with wind speed and Power

    """
    
    # Map table names to SQL names onto local constant friendly names
    DB_cm = sql_cm[WF_DB]
    db_name = DB_cm['DBname']
    table = DB_cm['Table']
    WT_col = DB_cm['WT']
    windsp = DB_cm['WindSp']
    power = DB_cm['Pwr']
    date_col = DB_cm['Date']
    
    # Open up database connection
    print (f"mariadb+mariadbconnector://{sql_cred['user']}:{sql_cred['passwd']}@{sql_cred['server']}/{db_name}") # DEBUG
    engine = sql.create_engine(f"mariadb+mariadbconnector://{sql_cred['user']}:{sql_cred['passwd']}@{sql_cred['server']}/{db_name}")
    
    # Generate query
    
    query = f"SELECT {windsp} as WindSp, {power} as Pwr FROM {table} WHERE {WT_col}='{wt}' and {date_col} BETWEEN '{date_start}' AND '{date_end}'"
    print (query) # DEBUG / Informational
    
    # get data
    data = pd.read_sql(query, engine)
    
    # close engine
    engine.dispose()
    
    return(data)


def get_turbine_list(WF_DB):
    """
    

    Parameters
    ----------
    WF_DB : string
        Wind Farm name.

    Returns
    -------
    List of Turbines in Farm.

    """
    # Map table names to SQL names onto local constant friendly names
    DB_cm = sql_cm[WF_DB]
    db_name = DB_cm['DBname']
    table = DB_cm['Table']
    WT_col = DB_cm['WT']
    
    # Open up SQL connection
    engine = sql.create_engine(f"mariadb+mariadbconnector://{sql_cred['user']}:{sql_cred['passwd']}@{sql_cred['server']}/{db_name}")

    # SQL query to get list of turbines (unique values in the WT_col)
    query = f"SELECT UNIQUE({WT_col}) as WT_list FROM {table}"
    print(query)
    wt_list = pd.read_sql(query, engine)

    # close SQL
    engine.dispose()
    
    return(wt_list)


def farm_date_range(WF_DB):
    """
    

    Parameters
    ----------
    WF_DB : string
        Wind Farm name.

    Returns
    -------
    min/max dates (as string).

    """
    # Map table names to SQL names onto local constant friendly names
    DB_cm = sql_cm[WF_DB]
    db_name = DB_cm['DBname']
    table = DB_cm['Table']
    date_col = DB_cm['Date']

    # Open up SQL connection
    engine = sql.create_engine(f"mariadb+mariadbconnector://{sql_cred['user']}:{sql_cred['passwd']}@{sql_cred['server']}/{db_name}")

    # SQL query to get list of turbines (unique values in the WT_col)
    query = f"SELECT convert(convert(min({date_col}), DATE), char) as date_start, convert(convert(max({date_col}), DATE), char) as date_end FROM {table}"
    print(query)
    date_range = pd.read_sql(query, engine)
    
    # close SQL
    engine.dispose()
    
    return(date_range)



##
## SCRIPT STYLE ILLUSTRATION OF CODE
##

# list of wind farms

wf_list = list(sql_cm)
print(wf_list)


# getting the list of turbines

penm_list = get_turbine_list("Penmanshiel")
eg_list = get_turbine_list("EngieGreen")
edp_list = get_turbine_list("OpenEDP")

# get some date ranges

pen_range = farm_date_range("Penmanshiel")
eg_range = farm_date_range("EngieGreen")
edp_range = farm_date_range("OpenEDP")


# Some power curve plots

data_pen = get_power("2017-01-01", "2017-02-01", "T01", "Penmanshiel")
plt.figure()
data_pen.plot.scatter(x='WindSp', y='Pwr')
plt.title('Penmanshiel T01')
plt.show()

data_eg = get_power("2015-01-01", "2015-02-01", "R80711", "EngieGreen")
plt.figure()
data_eg.plot.scatter(x='WindSp', y='Pwr')
plt.title('EngieGreen R80711')
plt.show()

data_edp = get_power("2017-01-01", "2017-02-01", "T01", "OpenEDP")
plt.figure()
data_edp.plot.scatter(x='WindSp', y='Pwr')
plt.title('OpenEDP T01')
plt.show()


