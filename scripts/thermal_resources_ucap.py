import psycopg2
import numpy as np
import pandas as pd
from pathlib import Path
from pandas import Timestamp as ts,Timedelta as td
from pandas.tseries.offsets import MonthBegin,MonthEnd
from login import pguser

from retrieve_caiso_curtailments import CurtailmentDownloader

def get_resource_types(resource_ids:list):
    '''
    Retrieves resource unit-types for each resource id in input list from EZDB.

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

def calculate_unforced_outage_rates(curtailments,resources,nminutes=5):
    '''
    Calculates the monthly unforced outage rates for each resource in the input
    curtailment dataframe as:
        UFOR = (FOH + EFDH) / (SH + FOH + SynchHrs + PumpingHrs + EFDHRS)
    The curtailment data does not have information on service, synchronous,
    pumping, or demand hours, so the denominator is simplified to the number of
    hours in a given month, accounting for the date each resource comes into
    service.

        parameters:
            curtailments - a pandas dataframe containing curtailment data from
                CAISO's prior trade day curtailment reports.
            resources - a pandas dataframe containing resource service dates.
            nminutes - the number of minutes to use when blocking outages.
                Default is 5 minutes.
        outputs:
            a data frame listing each curtailed resource with its service hours,
            outage hours, and forced outage rates by month.

    '''

    # Extract resource id and start-up dates for battery storage resources from
    # resource table:
    df0 = resources.loc[
        (resources.loc[:,'ENERGY_SOURCE']=='LESR'),
        ['RESOURCE_ID','COD','NET_DEPENDABLE_CAPACITY']
    ]

    # Rename columns in resource table:
    df0 = df0.rename(
        columns={
            'RESOURCE_ID':'RESOURCE ID',
            'COD':'STARTUP DATE',
            'NET_DEPENDABLE_CAPACITY':'NET DEPENDABLE CAPACITY'
        }
    )

    # Remove resources with no startup date:
    df0 = df0.dropna(axis='index',subset=['STARTUP DATE'])

    # Determine first full month after startup:
    df0.loc[:,'STARTUP DATE'] = pd.to_datetime(df0.loc[:,'STARTUP DATE'])
    def get_first_month(t:ts):
        if not t.is_month_start:
            return t+MonthEnd(0)+td(days=1)
        else:
            return t
    df0.loc[:,'FIRST MONTH'] = df0.loc[:,'STARTUP DATE'].map(get_first_month)

    # Extract relevant columns from curtailment reports table:
    df1 = curtailments.loc[
        :,
        [
            'RESOURCE ID',
            'OUTAGE MRID',
            'OUTAGE TYPE',
            'NATURE OF WORK',
            'CURTAILMENT START DATE TIME',
            'CURTAILMENT END DATE TIME',
            'RESOURCE PMAX MW',
            'CURTAILMENT MW',
        ]
    ]

    # Standardize curtailment start and end date formats:
    df1.loc[:,'CURTAILMENT START DATE TIME'] = pd.to_datetime(df1.loc[:,'CURTAILMENT START DATE TIME'])
    df1.loc[:,'CURTAILMENT END DATE TIME'] = pd.to_datetime(df1.loc[:,'CURTAILMENT END DATE TIME'])

    # Remove all but the most recently submitted curtailment report for each
    # MRID and start time:
    df1 = df1.groupby(['RESOURCE ID','OUTAGE MRID','OUTAGE TYPE','NATURE OF WORK','CURTAILMENT START DATE TIME']).last().reset_index()


    # Determine last full month of curtailment data:
    last_datetime = df1.loc[:,'CURTAILMENT END DATE TIME'].max()
    if not last_datetime.is_month_end:
        last_datetime = (last_datetime + MonthEnd(-1)).replace(hour=23,minute=59,second=59)
    else:
        last_datetime = last_datetime.replace(hour=23,minute=59,second=59)

    # Merge curtailments with resource start-up dates:
    df1 = df1.merge(df0,on=['RESOURCE ID'],how='left')

    # expand curtailment records into time blocks rounded to a given number of
    # minutes:
    def expand_hours(df1_row,nminutes):
        start_datetime = df1_row.loc['CURTAILMENT START DATE TIME']
        start_datetime = start_datetime.round(str(nminutes) + 'min')
        end_datetime = df1_row.loc['CURTAILMENT END DATE TIME']
        end_datetime = max(
            start_datetime,
            end_datetime.round(str(nminutes) + 'min')
        )
        delta_datetime = end_datetime - start_datetime
        return [ts(start_datetime)+td(minutes=nminutes*x) for x in range(max(int(delta_datetime.seconds/(nminutes*60)),1))]
    df1.loc[:,'CURTAILMENT DATETIME'] = df1.apply(lambda r: expand_hours(r,nminutes),axis='columns')
    df1 = df1.explode('CURTAILMENT DATETIME')
    df1 = df1.groupby(
        [
            'RESOURCE ID',
            'OUTAGE MRID',
            'OUTAGE TYPE',
            'NATURE OF WORK',
            'FIRST MONTH',
            'CURTAILMENT DATETIME'
        ]
    ).last().reset_index()

    # Calculate and save durations of each curtailment:
    df3 = df1.loc[:,['RESOURCE ID','OUTAGE MRID','CURTAILMENT MW','CURTAILMENT DATETIME']].groupby(['RESOURCE ID','OUTAGE MRID','CURTAILMENT MW']).count().reset_index()
    df3.loc[:,'CURTAILMENT DURATION'] = df3.loc[:,'CURTAILMENT DATETIME'] / 60
    df3.loc[:,['RESOURCE ID','OUTAGE MRID','CURTAILMENT MW','CURTAILMENT DURATION']].to_csv('CurtailmentDurationsByMRID.csv',index=False)

    # Filter curtailment reports for only those during full months after startup:
    df1 = df1.loc[
        (df1.loc[:,'CURTAILMENT DATETIME']>=df1.loc[:,'FIRST MONTH'])&
        (df1.loc[:,'CURTAILMENT DATETIME']<=last_date),
        :
    ]

    # Filter for forced outages:
    natures_of_work = [
        'ENVIRONMENTAL_RESTRICTIONS',
        'ICCP',
        'METERING_TELEMETRY',
        'PLANT_TROUBLE',
        'RIMS_OUTAGE',
        'RIMS_TESTING',
        'RTU_RIG',
        'TECHNICAL_LIMITATIONS_NOT_IN_MARKET_MODEL',
        'TRANSITIONAL_LIMITATION',
        'TRANSMISSION_INDUCED'
    ]
    outage_types = ['FORCED'] * len(natures_of_work)
    outage_codes = pd.DataFrame(
        {
            'OUTAGE TYPE' : outage_types,
            'NATURE OF WORK' : natures_of_work,
        }
    )
    #df1 = df1.merge(outage_codes,on=['OUTAGE TYPE','NATURE OF WORK'],how='inner')
    df1 = df1.loc[(df1.loc[:,'OUTAGE TYPE']=='FORCED'),:]
    df2 = df1.loc[(df1.loc[:,'OUTAGE TYPE']=='PLANNED'),:]

    # Get month during which each curtailment occurred:
    df1.loc[:,'MONTH'] = df1.loc[:,'CURTAILMENT DATETIME'].map(lambda t: t.replace(day=1,hour=0,minute=0,second=0,microsecond=0))
    df2.loc[:,'MONTH'] = df2.loc[:,'CURTAILMENT DATETIME'].map(lambda t: t.replace(day=1,hour=0,minute=0,second=0,microsecond=0))

    # Calculate equivalent forced derated hours (EFDH) for each time block,
    # applying the formula EFDH = hrs * MW / NMC:
    df1.loc[:,'EFDH'] = nminutes / 60 * df1.loc[:,'CURTAILMENT MW'] / df1.loc[:,'NET DEPENDABLE CAPACITY']
    df2.loc[:,'EQUIVALENT PLANNED OUTAGE HOURS'] = nminutes / 60 * df2.loc[:,'CURTAILMENT MW'] / df2.loc[:,'NET DEPENDABLE CAPACITY']

    # Calculate total hours in month:
    df1.loc[:,'MONTH DURATION'] = df1.loc[:,'MONTH'].map(lambda t: ((t + MonthBegin(1))-t).total_seconds()/3600)

    # Calculate available hours for each resource, in each month:
    df2 = df2.loc[:,['RESOURCE ID','MONTH','EQUIVALENT PLANNED OUTAGE HOURS']].groupby(['RESOURCE ID','MONTH']).sum()
    df1 = df1.merge(df2,on=['RESOURCE ID','MONTH'],how='outer')
    df1.loc[:,'EFDH'] = df1.loc[:,'EFDH'].fillna(value=0)
    df1.loc[:,'EQUIVALENT PLANNED OUTAGE HOURS'] = df1.loc[:,'EQUIVALENT PLANNED OUTAGE HOURS'].fillna(value=0)
    # Remove at least 4 hours per day for charging, approximate reserve shutdowns as planned outages:
    # df1.loc[:,'AVAILABLE HOURS'] = 20/24 * df1.loc[:,'MONTH DURATION'] - df1.loc[:,'EQUIVALENT PLANNED OUTAGE HOURS']
    df1.loc[:,'AVAILABLE HOURS'] = df1.loc[:,'MONTH DURATION'] - df1.loc[:,'EQUIVALENT PLANNED OUTAGE HOURS']

    # Calculate monthly EFOR within each time block, applying the formula
    # EFOR = (FOH+EFDH)/(available hours in month), with FOH accounted for in
    # EFDH where curtailment MW=NMC:
    df1.loc[:,'EFOR'] = df1.loc[:,'EFDH'] / df1.loc[:,'AVAILABLE HOURS']

    # Sum EFDH and FOH:
    df1 = df1.loc[
        :,
        [
            'RESOURCE ID',
            'OUTAGE TYPE',
            'NATURE OF WORK',
            'MONTH',
            'EFDH',
            'MONTH DURATION',
            'AVAILABLE HOURS',
            'EFOR'
        ]
    ].groupby(
        [
            'RESOURCE ID',
            'OUTAGE TYPE',
            'NATURE OF WORK',
            'MONTH',
            'MONTH DURATION',
            'AVAILABLE HOURS'
        ]
    ).sum().reset_index()

    # Merge to full list of all resources and range of online months:
    df3 = df0.dropna(axis='index')
    def get_date_range(df0_row):
        first_datetime = max(
            df0_row.loc['FIRST MONTH'],
            pd.to_datetime(curtailments.loc[:,'CURTAILMENT START DATE TIME']).min() + MonthBegin(1)
        )
        return [ts(int(x/12),x%12+1,1) for x in range(first_datetime.year*12+first_datetime.month-1,last_datetime.year*12+last_datetime.month)]
    df3.loc[:,'MONTH'] = df3.apply(get_date_range,axis='columns')
    df3 = df3.explode('MONTH')
    df3 = df3.loc[:,['RESOURCE ID','MONTH']].merge(df1,on=['RESOURCE ID','MONTH'],how='left')
    df3.loc[:,'EFOR'] = df3.loc[:,'EFOR'].fillna(value=0)

    return df3

if __name__=='__main__':
    start_time = ts.now()
    first_date = ts(2021,6,18)
    last_date = ts(start_time.date())
    date_range = [first_date + td(days=d) for d in range((last_date-first_date).days)]
    download_directory_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\caiso_curtailment_reports')
    log_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\caiso_curtailment_reports\\download_log.csv')
    storage_curtailments_path = Path('M:\\Users\\RH2\\src\\caiso_curtailments\\thermal_ucap\\curtailed_thermal_resources.csv')
    if not storage_curtailments_path.is_file():
        curtailment_downloader = CurtailmentDownloader(
            download_directory_path=download_directory_path,
            log_path=log_path
        )
        # df0 = curtailment_downloader.extract_by_columns([('OUTAGE TYPE','FORCED')],effective_dates=date_range)
        df0 = curtailment_downloader.extract_all(effective_dates=date_range)
        resource_ids = df0.loc[:,'RESOURCE ID'].unique()

        # Save curtailment codes:
        df0.loc[:,['OUTAGE TYPE','NATURE OF WORK']].groupby(['OUTAGE TYPE','NATURE OF WORK']).first().reset_index().to_csv(r'M:\Users\RH2\src\caiso_curtailments\thermal_ucap\OutageCodes.csv',index=False)

        # retrieve unit type from EZDB:
        # df1 = get_resource_types(resource_ids)
        # alternatively retrieve resource type from csv extracted from MRD:
        df1 = pd.read_csv(r'M:\Users\RH2\src\caiso_curtailments\thermal_ucap\MasterCapabilityList_2024-01-22.csv')
        df1 = df1.rename(columns={'RESOURCE_ID':'RESOURCE ID','ENERGY_SOURCE':'ENERGY SOURCE'})
        df0 = df0.merge(df1,how='left',on='RESOURCE ID')
        df0.loc[
            (df0.loc[:,'ENERGY SOURCE']=='NATURAL GAS') |
            (df0.loc[:,'ENERGY SOURCE']=='BIOGAS') |
            (df0.loc[:,'ENERGY SOURCE']=='BIOMASS')|
            (df0.loc[:,'ENERGY SOURCE']=='WASTE TO POWER'),
            [
                'OUTAGE MRID',
                'RESOURCE ID',
                'ENERGY SOURCE',
                'OUTAGE TYPE',
                'NATURE OF WORK',
                'CURTAILMENT START DATE TIME',
                'CURTAILMENT END DATE TIME',
                'CURTAILMENT MW',
                'RESOURCE PMAX MW',
                'NET QUALIFYING CAPACITY MW'
            ]
        ].to_csv(storage_curtailments_path,index=False)
    else:
        df0 = pd.read_csv(storage_curtailments_path)

    # load master generator capability list
    df2 = pd.read_csv(r'M:\Users\RH2\src\caiso_curtailments\thermal_ucap\MasterCapabilityList_2024-01-22.csv')
    for nminutes in [1]:
        df3 = calculate_unforced_outage_rates(df0,df2)
        df3.to_csv(r'M:\Users\RH2\src\caiso_curtailments\thermal_ucap\MonthlyEFOR_{}min.csv'.format(nminutes),index=False)
