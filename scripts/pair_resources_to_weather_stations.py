import psycopg2
import pandas as pd
from pathlib import Path

from login import pguser

def retrieve_resource_locations(resource_ids:list):
    '''
    Retrieves geospatial data for each resource identified in resource_ids from
    the ezdb_ed_main.public.ceccaisoeiaplantid table on EZDB

    Parameters:
        resource_ids - a list of strings containing CAISO IDs for generation
            resources

    Returns:
        a dataframe containing three columns: resource_id, lat, and lon
    '''
    resource_ids.sort()
    sql_str_template = '''
        WITH curtailed_resources ("ResourceID") AS ( VALUES
            {}
        )

        SELECT DISTINCT ON ("ResourceID")
            "ResourceID"
            ,"UnitType"
            ,"ResLat"
            ,CASE
                WHEN "ResLon"<150 AND "ResLon">100 THEN 360-"ResLon"
                WHEN "ResLon">-150 AND "ResLon"<-100 THEN 360+"ResLon"
                ELSE "ResLon"
            END AS "ResLon"
            ,"Lat1"
            ,"Lat2"
            ,"Lat3"
            ,"Lat4"
            ,"Lat5"
            ,"Lon1"
            ,"Lon2"
            ,"Lon3"
            ,"Lon4"
            ,"Lon5"
        FROM (
            SELECT
                b."ResourceID"
                ,e."UNIT_TYPE" AS "UnitType"
                ,COALESCE(c."Latitude",d."Latitude",g."Latitude",h."Latitude",0) AS "ResLat"
                ,COALESCE(c."Longitude",d."Longitude",g."Longitude",h."Longitude",0) AS "ResLon"
                ,c."Latitude" AS "Lat1"
                ,d."Latitude" AS "Lat2"
                ,f."Latitude" AS "Lat3"
                ,g."Latitude" AS "Lat4"
                ,h."Latitude" AS "Lat5"
                ,c."Longitude" AS "Lon1"
                ,d."Longitude" AS "Lon2"
                ,f."Longitude" AS "Lon3"
                ,g."Longitude" AS "Lon4"
                ,h."Longitude" AS "Lon5"
            FROM curtailed_resources AS b
            LEFT JOIN (
                SELECT *
                FROM ezdb_ed_main.public.ceccaisoeiaplantid_current
                WHERE "Latitude"<>0 AND "Latitude" IS NOT NULL AND "Latitude"<>'nan' AND "Longitude"<>0 AND "Longitude" IS NOT NULL AND "Longitude"<>'nan'
            ) AS c
            ON b."ResourceID"=c."ResID"
            LEFT JOIN (
                SELECT *
                FROM ezdb_ed_main.public.ceccaisoeiaplantid_current
                WHERE "Latitude"<>0 AND "Latitude" IS NOT NULL AND "Latitude"<>'nan' AND "Longitude"<>0 AND "Longitude" IS NOT NULL AND "Longitude"<>'nan'
            ) AS d
            ON LEFT(b."ResourceID",POSITION('_' IN b."ResourceID"))=LEFT(d."ResID",POSITION('_' IN d."ResID"))
            LEFT JOIN ezdb_ed_main.public.caisomastercapability_current AS e
            ON b."ResourceID"=e."ResID"
            LEFT JOIN (
                SELECT *
                FROM ezdb_ed_main.public.ceccaisoeiaplantid_current
                WHERE "Latitude"<>0 AND "Latitude" IS NOT NULL AND "Latitude"<>'nan' AND "Longitude"<>0 AND "Longitude" IS NOT NULL AND "Longitude"<>'nan'
            ) AS f
            ON e."PARENT_ResID"=f."ResID"
            LEFT JOIN (
                SELECT *
                FROM ezdb_ed_main.public.eiaform860plant_current
                WHERE "Latitude"<>0 AND "Latitude" IS NOT NULL AND "Latitude"<>'nan' AND "Longitude"<>0 AND "Longitude" IS NOT NULL AND "Longitude"<>'nan'
            ) AS g
            ON g."EIAPlantID"=COALESCE(c."EIA_Plant_ID",d."EIA_Plant_ID")
            LEFT JOIN (
                SELECT *
                FROM ezdb_ed_main.public.cpucirpbaselineresource_current
                WHERE "Latitude"<>0 AND "Latitude" IS NOT NULL AND "Latitude"<>'nan' AND "Longitude"<>0 AND "Longitude" IS NOT NULL AND "Longitude"<>'nan'
            ) AS h
            ON h."ResID"=e."ResID"
        ) AS a
        ORDER BY "ResourceID"
    '''
    sql_str = sql_str_template.format('\n            ,'.join([f'(\'{s}\')' for s in resource_ids]))
    conn = psycopg2.connect(
        database=pguser['db_main'],
        user=pguser['uid'],
        password=pguser['passwd'],
        host=pguser['host']
    )
    with conn:
        with conn.cursor() as curs:
            print('Retrieving generator resource locations from EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
            print('Retrieved {} resource locations.'.format(len(results)))
    return pd.DataFrame(results,columns=['ResourceID','UnitType','ResLat','ResLon','Lat1','Lat2','Lat3','Lat4','Lat5','Lon1','Lon2','Lon3','Lon4','Lon5'])

def retrieve_resource_locations_alternative(resource_ids:list):
    '''
    Retrieves geospatial data for each resource identified in resource_ids from
    the ezdb_ed_main.public.ceccaisoeiaplantid table on EZDB

    Parameters:
        resource_ids - a list of strings containing CAISO IDs for generation
            resources

    Returns:
        a dataframe containing three columns: resource_id, lat, and lon
    '''
    resource_ids.sort()
    sql_str_template = '''
        WITH resources ("ResourceID") AS ( VALUES
            {}
        )

        SELECT DISTINCT ON (resources."ResourceID")
            resources."ResourceID"
            ,"Latitude" AS "ResLat"
            ,CASE
                WHEN "Longitude"<150 AND "Longitude">100 THEN 360-"Longitude"
                WHEN "Longitude">-150 AND "Longitude"<-100 THEN 360+"Longitude"
                ELSE "Longitude"
            END AS "ResLon"
        FROM
            resources
        LEFT JOIN
            ezdb_ed_main.public.ceccaisoeiaplantid_current
        ON resources."ResourceID"=ceccaisoeiaplantid_current."ResID"
        WHERE "Latitude"<>'nan' AND "Latitude"<>0 AND "Longitude"<>'nan' AND "Longitude"<>0
        ORDER BY "ResourceID"
    '''
    sql_str = sql_str_template.format('\n            ,'.join([f'(\'{s}\')' for s in resource_ids]))
    conn = psycopg2.connect(
        database=pguser['db_main'],
        user=pguser['uid'],
        password=pguser['passwd'],
        host=pguser['host'])
    with conn:
        with conn.cursor() as curs:
            print('Retrieving generator resource locations from EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
            print('Retrieved {} resource locations.'.format(len(results)))
    resources = pd.DataFrame(results,columns=['ResourceID','ResLat','ResLon'])
    resources.loc[:,'UnitType'] = ''
    return resources

def identify_weather_stations(resources:pd.DataFrame):
    '''
    identifies the closest weather station to each resource based on geospatial
    information in the second database

    parameters:
        resources - a dataframe containing a column with the ResourceID, Lat,
            Lon, and UnitType values for each resource with which weather
            stations are to be matched.

    returns:
        a dataframe containing ids and lat/lon coordinates for each resource and
        its closest weather station, along with the absolute distance between
        (in kilometers)
    '''
    sql_str_template = '''
        WITH constants ("radius_km","pi") AS ( VALUES (6563,3.14159265359) ),
        gen_locations ("ResourceID","UnitType","ResLat","ResLon") AS ( VALUES
            {}
        ),
        weather_stations ("StationName") AS ( VALUES
            ('KNKX'),
            ('KOAK'),
            ('KRDD'),
            ('KRNO'),
            ('KSAC'),
            ('KSAN'),
            ('KSBA'),
            ('KSCK'),
            ('KSFO'),
            ('KSJC'),
            ('KSMF'),
            ('KUKI')
        )

        SELECT
            "ResourceID"
            ,"UnitType"
            ,"StationName"
            ,"Dist"
            ,RANK() OVER (ORDER BY RIGHT('0000000'||CAST(ROUND(100*"Dist") AS TEXT),7)||"ResourceID") AS "DistRank"
            ,"ResLat"
            ,"ResLon"
            ,"WeaLat"
            ,"WeaLon"
        FROM (
            SELECT
                "ResourceID"
                ,"UnitType"
                ,"StationName"
                ,"Dist"
                ,"ResLat"
                ,"ResLon"
                ,"WeaLat"
                ,"WeaLon"
                ,RANK() OVER (PARTITION BY "ResourceID" ORDER BY "Dist" ASC) AS "R"
            FROM (
                SELECT
                    "ResourceID"
                    ,"UnitType"
                    ,"StationName"
                    ,SQRT(("ResX"-"WeaX")^2 + ("ResY"-"WeaY")^2 + ("ResZ"-"WeaZ")^2) AS "Dist"
                    ,"ResLat"
                    ,"ResLon"
                    ,"WeaLat"
                    ,"WeaLon"
                FROM (
                    SELECT
                        "ResourceID"
                        ,"UnitType"
                        ,"ResLat"
                        ,"ResLon"
                        ,"StationName"
                        ,"Lat" AS "WeaLat"
                        ,"Lon" AS "WeaLon"
                        ,"radius_km"*COS("pi"*"ResLon"/180.0)*COS("pi"*"ResLat"/180.0) AS "ResX"
                        ,"radius_km"*SIN("pi"*"ResLon"/180.0)*COS("pi"*"ResLat"/180.0) AS "ResY"
                        ,"radius_km"*SIN("pi"*"ResLat"/180.0) AS "ResZ"
                        ,"radius_km"*COS("pi"*"Lon"/180.0)*COS("pi"*"Lat"/180.0) AS "WeaX"
                        ,"radius_km"*SIN("pi"*"Lon"/180.0)*COS("pi"*"Lat"/180.0) AS "WeaY"
                        ,"radius_km"*SIN("pi"*"Lat"/180.0) AS "WeaZ"
                    FROM
                        gen_locations, constants
                    FULL OUTER JOIN
                        (
                            SELECT
                                weather_stations."StationName",
                                "Lat",
                                "Lon"
                            FROM weather_stations
                            LEFT JOIN latlonmap
                            ON weather_stations."StationName"=latlonmap."StationName"
                        ) AS latlonmap
                    ON
                        TRUE
                ) AS c
            ) AS b
        ) AS a
        WHERE "R"=1
        ORDER BY "ResourceID"
    '''
    resource_list = ['(\'{}\',\'{}\',{},{})'.format(row.loc['ResourceID'],row.loc['UnitType'],row.loc['ResLat'],row.loc['ResLon']) for _,row in resources.iterrows()]
    sql_str = sql_str_template.format('\n            ,'.join(resource_list))
    conn = psycopg2.connect(
        database=pguser['db_alt'],
        user=pguser['uid'],
        password=pguser['passwd'],
        host=pguser['host'])
    with conn:
        with conn.cursor() as curs:
            print('Pairing resources with nearest weather station locations in EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
            print('Retrieved {} resource-weather station pairs.'.format(len(results)))
    return pd.DataFrame(results,columns=['ResourceID','UnitType','WeatherStationID','Dist','DistRank','ResLat','ResLon','WeaLat','WeaLon'])

if __name__=='__main__':
    resource_ids = pd.read_csv(Path('M:\\Users\\RH2\\src\\caiso_curtailments\\geospatial\\curtailed_resources.csv'))
    resources = retrieve_resource_locations(list(resource_ids.loc[:,'RESOURCE ID']))
    resource_weather_station_pairs = identify_weather_stations(resources)
    resource_weather_station_pairs.to_csv(Path('M:\\Users\\RH2\\src\\caiso_curtailments\\geospatial\\resource_weather_stations.csv'),index=False)