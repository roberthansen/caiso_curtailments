import psycopg2
import pandas as pd
from pathlib import Path
from pandas import Timestamp as ts
from login import pguser

from retrieve_caiso_curtailments import CurtailmentDownloader

def get_resource_types(resource_ids:list):
    '''
    Retrieves resource unit-types for each resource id in input list.

    Parameters:
        resource_ids - a list of strings containing CAISO IDs for generation
            resources

    Returns:
        a dataframe containing three columns: resource_id, ,unit type, lat, and lon
    '''
    resource_ids.sort()
    sql_str_template = '''
        WITH curtailed_resources ("ResourceID") AS ( VALUES
            {}
        )

        SELECT DISTINCT ON ("ResourceID")
            "ResourceID"
            ,"ResourceType"
            ,"UnitType"
        FROM (
            SELECT
                b."ResourceID"
                ,c."RESOURCE_TYPE" AS "ResourceType"
                ,c."UNIT_TYPE" AS "UnitType"
            FROM curtailed_resources AS b
            LEFT JOIN ezdb_ed_main.public.caisomastercapability AS c
            ON b."ResourceID"=c."ResID"
        ) AS a
        ORDER BY "ResourceID"
    '''
    sql_str = sql_str_template.format('\n            ,'.join([f'(\'{s}\')' for s in resource_ids]))
    conn = psycopg2.connect(
        database=pguser['db_main'],
        user=pguser['postgres'],
        password=pguser['passwd'],
        host=pguser['host']
    )
    with conn:
        with conn.cursor() as curs:
            print('Retrieving generator resource locations from EZDB ...')
            curs.execute(sql_str)
            results = curs.fetchall()
            print('Retrieved {} resource locations.'.format(len(results)))
    return pd.DataFrame(results,columns=['RESOURCE ID','RESOURCE TYPE','UNIT TYPE'])

if __name__=='__main__':
    curtailment_downloader = CurtailmentDownloader(
        download_directory_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\caiso_curtailment_reports'),
        log_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\download_log.csv')
    )
    df0 = curtailment_downloader.extract_by_columns([('OUTAGE TYPE','FORCED')],effective_dates=[ts(2022,8,x) for x in range(1,32)])
    resource_ids = df0.loc[:,'RESOURCE ID'].unique()
    df1 = get_resource_types(resource_ids)
    df0 = df0.merge(df1,how='left')
    df0.loc[:,['OUTAGE MRID',
        'RESOURCE ID',
        'RESOURCE TYPE',
        'UNIT TYPE',
        'OUTAGE TYPE',
        'NATURE OF WORK',
        'CURTAILMENT START DATE TIME',
        'CURTAILMENT END DATE TIME',
        'CURTAILMENT MW',
        'RESOURCE PMAX MW',
        'NET QUALIFYING CAPACITY MW',
    ]].to_csv(Path('M:\\Users\\RH2\\src\\caiso_curtailments\\results\\curtailed_storage_resources.csv'),index=False)