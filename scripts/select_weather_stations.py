from read_cifs import read_cif
from pathlib import Path
import pandas as pd
import numpy as np
from scipy.optimize import minimize
import psycopg2

from login import pguser

def get_weather_station_locations(weather_data_path:Path):
    '''
    retrieves lat/lon geolocations for weather stations identified in the given
    weather data parquet file at weather_data_path.

    Parameters:
        weather_data_path - path object pointing to a parquet file containing
            weather data from one or more weather stations
    
    Returns:
        a dataframe containing three columns: unique weather station ids from
            the weather data in the parquet file at weather_data_path,
            latitudes, and longitudes from EZDB
    '''
    _,weather_data = read_cif(weather_data_path)
    weather_station_ids = pd.Series(weather_data.loc[:,'StationID'].unique()).sort_values()
    sql_str_template = '''
        WITH weather_stations ("StationID") AS ( VALUES
            {}
        )
        SELECT
            "StationID"
            ,"Lat" AS "WeaLat"
            ,"Lon" AS "WeaLon"
        FROM
            weather_stations AS a
        LEFT JOIN
            latlonmap AS b
        ON
            a."StationID"=b."StationName"
        ORDER BY "StationID"
    '''
    sql_str = sql_str_template.format('\n            ,'.join([f'(\'{s}\')' for s in weather_station_ids]))
    conn = psycopg2.connect(
        database=pguser['db_main'],
        user=pguser['uid'],
        password=pguser['passwd'],
        host=pguser['host']
    )
    with conn:
        with conn.cursor() as curs:
            print('Retrieving weather station locations from EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
    return pd.DataFrame(results,columns=['StationID','WeaLat','WeaLon'])

def get_resource_locations():
    '''
    Retrieves geospatial data for all available resources from
    the ezdb_ed_main.public.ceccaisoeiaplantid table on EZDB

    Returns:
        a dataframe containing three columns: resource_id, lat, and lon
    '''
    sql_str = '''
        SELECT DISTINCT ON ("ResourceID")
            "ResourceID"
            ,"UnitType"
            ,"ServiceTerritory"
            ,"ResLat"
            ,CASE
                WHEN "ResLon"<150 AND "ResLon">100 THEN 360-"ResLon"
                WHEN "ResLon">-150 AND "ResLon"<-100 THEN 360+"ResLon"
                ELSE "ResLon"
            END AS "ResLon"
        FROM (
            SELECT
                COALESCE(
                    c."ResID",
                    d."ResID"
                ) AS "ResourceID"
                ,d."UNIT_TYPE" AS "UnitType"
                ,d."PTO_AREA" AS "ServiceTerritory"
                ,COALESCE(c."Latitude",e."Latitude",0) AS "ResLat"
                ,COALESCE(c."Longitude",e."Longitude",0) AS "ResLon"
            FROM  ezdb_ed_main.public.ceccaisoeiaplantid AS c
            FULL OUTER JOIN ezdb_ed_main.public.caisomastercapability AS d
            ON c."ResID"=d."ResID"
            LEFT JOIN ezdb_ed_main.public.ceccaisoeiaplantid AS e
            ON d."PARENT_ResID"=e."ResID"
        ) AS a
        WHERE "ResLat"<>'nan' AND "ResLat"<>0 AND "ResLon"<>'nan' AND "ResLon"<>0
        ORDER BY "ResourceID"
    '''
    conn = psycopg2.connect(
        database=pguser['db_main'],
        user=pguser['uid'],
        password=pguser['passwd'],
        host=pguser['host']
    )
    with conn:
        with conn.cursor() as curs:
            print('Retrieving generator resources and locations from EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
    print('Retrieved {} resource locations.'.format(len(results)))
    return pd.DataFrame(results,columns=['ResourceID','UnitType','ServiceTerritory','ResLat','ResLon'])

def select_optimal_weather_stations(number_of_weather_stations:int):
    '''
    Retrieves locations for all known weather stations and resources, and
    determines the optimal set of a specified number of weather stations to
    minimize total distance between each resource and its closest weather
    station.
    '''
    # get weather stations and locations
    weather_stations = get_weather_station_locations(Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data\cif_temperature_15_25.parquet'))
    # get resources and locations
    resources = get_resource_locations()
    resources = resources.loc[(resources.loc[:,'UnitType']=='COMBUSTION TURBINE')|(resources.loc[:,'UnitType']=='COMBINED CYCLE'),:]
    # find distances between each pair of resource and weather station
    combined = weather_stations.merge(resources,how='cross')
    def f(r):
        '''
        calculates great-circle distance using haversine formula
        '''
        earth_radius_km = 6371
        phi_wea = r.loc['WeaLat'] * np.pi / 180
        phi_res = r.loc['ResLat'] * np.pi / 180
        delta_phi = phi_res - phi_wea
        lambda_wea = r.loc['WeaLon'] * np.pi / 180
        lambda_res = r.loc['ResLon'] * np.pi / 180
        delta_lambda = lambda_res - lambda_wea
        a = np.sin(delta_phi/2)**2 + np.cos(phi_wea)*np.cos(phi_res)*(np.sin(delta_lambda/2)**2)
        sweep_angle = 2*np.arctan2(np.sqrt(a),np.sqrt(1-a))
        return earth_radius_km * sweep_angle
    combined.loc[:,'Distance'] = combined.apply(f,axis='columns')
    combined.loc[:,'DistanceRank'] = combined.groupby('ResourceID').Distance.rank(method='first')
    max_min_distance=[combined.loc[(combined.loc[:,'DistanceRank']==1),'Distance'].max()]
    weather_stations.loc[:,'MinDistanceRank']=weather_stations.merge(combined.groupby('StationID').DistanceRank.min().reset_index(),on='StationID').loc[:,'DistanceRank']
    nearest_weather_stations = weather_stations.loc[(weather_stations.loc[:,'MinDistanceRank']==1),'StationID']
    combined = combined.set_index('StationID').loc[nearest_weather_stations,:].reset_index()
    # remove the weather stations with the fewest nearest resources until at
    # most the requested number_of_weather_stations remains:
    while len(combined.loc[:,'StationID'].unique())>number_of_weather_stations:
        unfit_weather_station = combined.loc[(combined.loc[:,'DistanceRank']==1),['StationID','DistanceRank']].groupby('StationID').count().reset_index().min().loc['StationID']
        combined = combined.loc[(combined.loc[:,'StationID']!=unfit_weather_station),:]
        combined.loc[:,'DistanceRank'] = combined.groupby('ResourceID').Distance.rank(method='first')
        max_min_distance+=[combined.loc[(combined.loc[:,'DistanceRank']==1),'Distance'].max()]
    resource_weather_station_pairs = combined.loc[(combined.loc[:,'DistanceRank']==1),:]
    return [resource_weather_station_pairs.loc[:,'StationID'].unique(),resource_weather_station_pairs.loc[:,['ResourceID','UnitType','ServiceTerritory','ResLat','ResLon','StationID','UnitType','WeaLat','WeaLon','Distance']]]

if __name__=='__main__':
    weather_stations,resource_weather_station_pairs = select_optimal_weather_stations(12)
    resource_weather_station_pairs.to_csv(Path(r'M:\Users\RH2\src\caiso_curtailments\results\resource_weather_station_pairs.csv'),index=False)