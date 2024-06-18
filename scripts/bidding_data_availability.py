import pandas as pd
from pandas import Timestamp as ts,Timedelta as td
import numpy as np
from pathlib import Path
from bidding_data import get_bidding_data_path, read_bidding_data

def evaluate_service_hours(bidding_data:pd.DataFrame):
    '''
    Evaluates the number of hours each resource is scheduled or dispatched
    within each month. a resource is considered scheduled or dispatched when
    positive or negative capacity is bid into either the real-time (RTM) or day-
    ahead (DAM) markets, or when residual unit commitment (RUC) is scheduled,
    with positive bids indicating service hours and negative indicating
    charging.

        parameters:
            bidding_data - a Pandas dataframe containing bidding data for one or
                more months.
        returns:
            service_hours - a Pandas dataframe containing a summary of bidding
                data grouped by month and resource, indicating the number of
                hours each resource is indicated available.
    '''
    bidding_data.loc[:,'RESOURCE_NAME'] = bidding_data.loc[:,'RESOURCE_NAME'].map(lambda s:s.replace(' ','_'))
    bidding_data.loc[:,'MONTH'] = bidding_data.loc[:,'TRADE_DATE'].map(lambda t:t.replace(day=1))
    bidding_data.loc[
        :,
        [
            'RTM_BID_QUANTITY',
            'DAM_SELFCHEDMW',
            'RUC_DISPATCH_QUANTITY'
        ]
    ] = bidding_data.loc[
        :,
        [
            'RTM_BID_QUANTITY',
            'DAM_SELFCHEDMW',
            'RUC_DISPATCH_QUANTITY'
        ]
    ].fillna(0)

    # filter bidding data for hours with scheduled or dispatched capacity:
    bidding_data = bidding_data.loc[
        (bidding_data.loc[:,'RTM_BID_QUANTITY']!=0)|
        (bidding_data.loc[:,'DAM_SELFCHEDMW']!=0)|
        (bidding_data.loc[:,'RUC_DISPATCH_QUANTITY']!=0),
        :
    ]
    service_hours = bidding_data

    # First allocate hours to service, then charging (e.g., if discrepancies
    # between DAM and RTM exist), avoiding double-counting:
    service_hours.loc[:,'SERVICE_HOURS'] = 1 * (
        (service_hours.loc[:,'RTM_BID_QUANTITY']>0) |
        (service_hours.loc[:,'DAM_SELFCHEDMW']>0) |
        (service_hours.loc[:,'RUC_DISPATCH_QUANTITY']>0)
    )
    service_hours.loc[:,'CHARGING_HOURS'] = 1 * (
        (service_hours.loc[:,'SERVICE_HOURS']==0) &
        (
            (service_hours.loc[:,'RTM_BID_QUANTITY']<0) |
            (service_hours.loc[:,'DAM_SELFCHEDMW']<0) |
            (service_hours.loc[:,'RUC_DISPATCH_QUANTITY']<0)
        )
    )

    # Calculate total MW allocated each hour toward service or charging:
    service_hours.loc[:,'SERVICE_MWH'] = service_hours.loc[
        :,
        [
            'RTM_BID_QUANTITY',
            'DAM_SELFCHEDMW',
            'RUC_DISPATCH_QUANTITY'
        ]
    ].apply(lambda r: max(max(r),0),axis='columns')
    service_hours.loc[:,'CHARGING_MWH'] = service_hours.loc[
        :,
        [
            'RTM_BID_QUANTITY',
            'DAM_SELFCHEDMW',
            'RUC_DISPATCH_QUANTITY'
        ]
    ].apply(lambda r: min(min(r),0),axis='columns')

    # Aggregate by resource and month:
    service_hours = service_hours.loc[
        :,
        [
            'RESOURCE_NAME',
            'MONTH',
            'SERVICE_HOURS',
            'CHARGING_HOURS',
            'SERVICE_MWH',
            'CHARGING_MWH'
        ]
    ].groupby(['RESOURCE_NAME','MONTH']).sum().reset_index()

    return service_hours


def evaluate_multiple_months_service_hours(months:list):
    '''
    Evaluates and collates service and charging hours from multiple months of
    bidding data.

    parameters:
        months - a list of Pandas timestamp objects indicating the first day of
            each month to be evaluated
    returns:
        data frame containing combined results of service hours for all months
            in input list
    '''
    service_hours = pd.DataFrame(
        {
            'RESOURCE_NAME' : pd.Series([],dtype='string'),
            'MONTH' : pd.Series([],dtype='datetime64[s]'),
            'SERVICE_HOURS' : pd.Series([],dtype='UInt64'),
            'CHARGING_HOURS' : pd.Series([],dtype='UInt64'),
            'SERVICE_MWH' : pd.Series([],dtype='UInt64'),
            'CHARGING_MWH' : pd.Series([],dtype='UInt64'),
        }
    )
    for month in months:
        bidding_data_path = get_bidding_data_path(month)
        bidding_data = read_bidding_data(bidding_data_path)
        new_service_hours = evaluate_service_hours(bidding_data)
        service_hours = pd.concat([service_hours,new_service_hours],ignore_index=True)
    return service_hours

if __name__=='__main__':
    all_months = [ts(y,m,1) for y in range(2021,2024) for m in range(1,13)]
    master_capability_list = pd.read_csv(r'M:\Users\RH2\src\caiso_curtailments\storage_ucap\MasterCapabilityList_2024-01-22.csv')
    service_hours = evaluate_multiple_months_service_hours(all_months)
    service_hours.to_csv(r'M:\Users\RH2\src\caiso_curtailments\bidding_data\service_hours_by_resource_and_month.csv',index=False)