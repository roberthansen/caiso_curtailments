import pandas as pd
from pandas import Timestamp as ts
from pathlib import Path
from bidding_data import read_bidding_data, get_bidding_data_path

def summarize_rtm_dispatch(bidding_data):
    '''
    Summarizes the real-time market (RTM) dispatch quantities and prices within
    the input bidding data, aggregated by resource and month.
    '''
    bidding_data = bidding_data.loc[
        :,
        [
            'RESOURCE_NAME',
            'UNIT_TYPE',
            'TRADE_DATE',
            'RTM_DISPATCH_QUANTITY',
            'RTM_DISPATCH_PRICE',
            'RTM_BID_QUANTITY',
            'RTM_BID_PRICE',
        ]
    ]

    bidding_data.loc[:,'MONTH'] = bidding_data.loc[:,'TRADE_DATE'].map(lambda t: t.replace(day=1))
    bidding_data = bidding_data.drop(columns=['TRADE_DATE'])
    bidding_data.loc[
        :,
        [
            'RTM_DISPATCH_QUANTITY',
            'RTM_DISPATCH_PRICE',
            'RTM_BID_QUANTITY',
            'RTM_BID_PRICE',
        ]
    ] = bidding_data.loc[
        :,
        [
            'RTM_DISPATCH_QUANTITY',
            'RTM_DISPATCH_PRICE',
            'RTM_BID_QUANTITY',
            'RTM_BID_PRICE',
        ]
    ].fillna(0)
    bidding_data.loc[:,'RECORD_COUNT'] = 1

    # Aggregate bidding data by resource and month:
    rtm_dispatch_summary = bidding_data.groupby(['RESOURCE_NAME','MONTH']).sum().reset_index()
    rtm_dispatch_summary.loc[:,'RTM_DISPATCH_PRICE'] = rtm_dispatch_summary.loc[:,'RTM_DISPATCH_PRICE'] / rtm_dispatch_summary.loc[:,'RECORD_COUNT']
    rtm_dispatch_summary.loc[:,'RTM_BID_PRICE'] = rtm_dispatch_summary.loc[:,'RTM_BID_PRICE'] / rtm_dispatch_summary.loc[:,'RECORD_COUNT']
    rtm_dispatch_summary = rtm_dispatch_summary.rename(
        columns={
            'RTM_DISPATCH_QUANTITY' : 'TOTAL_RTM_DISPATCH_QUANTITY',
            'RTM_DISPATCH_PRICE' : 'AVERAGE_RTM_DISPATCH_PRICE',
            'RTM_BID_QUANTITY' : 'TOTAL_RTM_BID_QUANTITY',
            'RTM_BID_PRICE' : 'AVERAGE_RTM_BID_PRICE',
        }
    )
    rtm_dispatch_summary = rtm_dispatch_summary.drop(columns=['RECORD_COUNT'])

    return rtm_dispatch_summary


def summarize_multiple_months_rtm_dispatch(months):
    '''
    Summarizes the real-time market dispatches for the given months.

    parameters:
        months - a list of Pandas timestamp objects indicating the first day of
            each month to be evaluated
    returns:
        data frame containing the combined monthly summaries of all resources
            in the bidding data within the requested months
    '''

    rtm_dispatch_summary = pd.DataFrame(
        {
            'RESOURCE_NAME' : pd.Series([],dtype='string'),
            'MONTH' : pd.Series([],dtype='datetime64[s]'),
            'TOTAL_RTM_DISPATCH_QUANTITY' : pd.Series([],dtype='float64'),
            'AVERAGE_RTM_DISPATCH_PRICE' : pd.Series([],dtype='float64'),
            'TOTAL_RTM_BID_QUANTITY' : pd.Series([],dtype='float64'),
            'AVERAGE_RTM_BID_PRICE' : pd.Series([],dtype='float64'),
        }
    )

    for month in months:
        bidding_data_path = get_bidding_data_path(month)
        bidding_data = read_bidding_data(bidding_data_path)
        new_rtm_dispatch_summary = summarize_rtm_dispatch(bidding_data)
        rtm_dispatch_summary = pd.concat([rtm_dispatch_summary,new_rtm_dispatch_summary],ignore_index=True)

    return rtm_dispatch_summary

if __name__=='__main__':
    months = [ts(y,m,1) for y in range(2021,2023) for m in range(1,13)]
    rtm_dispatch_summary = summarize_multiple_months_rtm_dispatch(months)
    rtm_dispatch_summary.to_csv(r'M:\Users\RH2\src\caiso_curtailments\bidding_data\rtm_dispatch_summary_by_resource_and_month.csv',index=False)
