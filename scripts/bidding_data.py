import pandas as pd
from pandas import Timestamp as ts
from pathlib import Path

bidding_data_dtypes = {
    'UNIT_TYPE' : 'string',
    'TRADE_DATE' : 'datetime64[s]',
    'TRADE_HOUR' : 'UInt8',
    'SCID' : 'string',
    'RESOURCE_NAME' : 'string',
    'RTM_DISPATCH_QUANTITY' : 'float64',
    'RTM_DISPATCH_PRICE' : 'float64',
    'RTM_BID_QUANTITY' : 'float64',
    'RTM_BID_PRICE' : 'float64',
    'DAM_DISPATCH_QUANTITY' : 'float64',
    'RUC_DISPATCH_QUANTITY' : 'float64',
    'DAM_DISPATCH_PRICE' : 'float64',
    'DAM_BID_QUANTITY' : 'float64',
    'DAM_BID_PRICE' : 'float64',
    'DAM_SELFCHEDMW' : 'float64'
}

def get_bidding_data_path(month:ts):
    bidding_data_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\bidding_data')
    bidding_data_filename_str = 'Econ Bids and Self-Schedules {}.csv'.format(month.strftime('%Y-%m'))
    bidding_data_path = bidding_data_directory / Path(bidding_data_filename_str)
    return bidding_data_path

def read_bidding_data(bidding_data_path:Path):
    if bidding_data_path.is_file():
        print('Reading Bidding Data: {}'.format(bidding_data_path.name))
        bidding_data = pd.read_csv(
            bidding_data_path,
        )
        for k in bidding_data_dtypes:
            bidding_data.loc[:,k] = bidding_data.loc[:,k].astype(bidding_data_dtypes[k])
        bidding_data = bidding_data.dropna(axis='index',how='any',subset=['RESOURCE_NAME','TRADE_DATE','TRADE_HOUR'])
    else:
        print('Bidding Data Not Found: {}'.format(bidding_data_path.name))
        bidding_data = pd.DataFrame(
            {k:pd.Series([],dtype=bidding_data_dtypes[k]) for k in bidding_data_dtypes.keys()}
        )
    return bidding_data

def read_month_bidding_data(month:ts):
    bidding_data_path = get_bidding_data_path(month.replace(day=1))
    print('Reading Bidding Data: {}'.format(bidding_data_path.name))
    bidding_data = read_bidding_data(bidding_data_path)
    return bidding_data

def read_multiple_months_bidding_data(months:list):
    bidding_data = pd.DataFrame(
        {k:pd.Series([],dtype=bidding_data_dtypes[k]) for k in bidding_data_dtypes.keys()}
    )
    for month in months:
        bidding_data_path = get_bidding_data_path(month)
        new_bidding_data = read_bidding_data(bidding_data_path)
        bidding_data = pd.concat([bidding_data,new_bidding_data],ignore_index=True)
    return bidding_data