import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import dask.dataframe as ddf
import re
from functools import reduce
from pathlib import Path
from pandas import Timedelta as td
from pandas import Timestamp as ts
import metpy.calc as mpcalc
from metpy.units import units

class CurtailmentModeller:
    '''
    A class to assist in modeling curtailments as a function of temperature
    using data from nearby weather stations
    '''
    resource_curtailments = pd.DataFrame()
    weather_data = pd.DataFrame()
    weather_station_map = pd.DataFrame()
    weather_station_placenames = pd.DataFrame()
    regression_by_resource = pd.DataFrame()
    regression_by_unit_type = pd.DataFrame()
    def __init__(self,data_paths:dict):
        self.set_data_paths(data_paths)

    def set_data_paths(self,data_paths:dict):
        self.data_paths = {
            'resource_curtailments_filename' : data_paths['resource_curtailments_filename'],
            'weather_data_directory' : data_paths['weather_data_directory'],
            'processed_weather_data_filename' : data_paths['processed_weather_data_filename'],
            'resources_to_weather_stations_map_filename' : data_paths['resources_to_weather_stations_map_filename'],
            'weather_station_placenames_filename' : data_paths['weather_station_placenames_filename'],
            'regression_by_resource_filename' : data_paths['regression_by_resource_filename'],
            'regression_by_unit_type_filename' : data_paths['regression_by_unit_type_filename'],
            'merged_data_filename' : data_paths['merged_data_filename'],
        }

    def load_resource_curtailments(self):
        '''
        Reads a file containing extracted prior trade day curtailment reports
        and loads the data into a Pandas DataFrame for analysis.
        '''
        print('Loading Resource Curtailment Reports ...')
        df = pd.read_csv(self.data_paths['resource_curtailments_filename'],low_memory=False)
        # use only the last report for a given MRID and start time:
        df = df.groupby(['OUTAGE MRID','CURTAILMENT START DATE TIME']).last().reset_index()
        df.drop(columns=['OUTAGE MRID','NET QUALIFYING CAPACITY MW'],inplace=True)
        df.dropna(axis='index',how='any',inplace=True)
        df.loc[:,'CURTAILMENT START DATE TIME'] = pd.to_datetime(df.loc[:,'CURTAILMENT START DATE TIME'])
        df.loc[:,'CURTAILMENT END DATE TIME'] = pd.to_datetime(df.loc[:,'CURTAILMENT END DATE TIME'])
        def expand_hours(df_row):
            start_datetime = df_row.loc['CURTAILMENT START DATE TIME'].replace(minute=0,second=0,microsecond=0)
            end_datetime = df_row.loc['CURTAILMENT END DATE TIME']
            delta_datetime = end_datetime - start_datetime
            return [ts(start_datetime)+td(hours=x) for x in range(max(int(delta_datetime.seconds/3600),1))]
        df.loc[:,'DATETIME'] = df.apply(expand_hours,axis='columns')
        df.drop(columns=['CURTAILMENT START DATE TIME','CURTAILMENT END DATE TIME'],inplace=True)
        self.resource_curtailments = ddf.from_pandas(df.explode('DATETIME').reset_index().drop(columns=['index']),npartitions=16)

    def load_weather(self,use_processed:bool=True):
        '''
        Reads a file containing hourly weather data in ISD format downloaded
        from ncei.noaa.gov and loads the data into a Pandas DataFrame for
        analysis. If weather data has already been processed and stored in a
        data file, the use_store_merged flag will read data from that file
        instead.
        '''
        def parse_temperature(temp_str:str):
            '''
            Per ISD specification, temperatures are reported as signed and
            zero-padded four-digit integers between -0932 and +0618 representing
            the temperature in degrees Celsius multiplied by 10, with missing
            values reported as +9999. A second term in the TMP field, separated
            by a comma, is an alphanumeric quality code where values of 0, 1, 4,
            5,9,A,C,I,or M are considered acceptable for the purpose of our
            purposes (see ISD specification p.11 for code definitions).
            '''
            temp_regex = re.compile(r'^([+-]\d{4}),[01459ACIM]$')
            if isinstance(temp_str,str) and temp_regex.search(temp_str):
                temp = int(temp_regex.match(temp_str).groups()[0])/10
                if temp==999.9:
                    temp = np.nan
            else:
                temp = np.nan
            return temp
        def parse_dew_point(dew_point_str:str):
            '''
            The DEW field is not explained in the ISD specification, but appears
            to have the same format as actual temperatures.
            '''
            return parse_temperature(dew_point_str)
        def parse_pressure(press_str:str):
            '''
            The SLP field contains the atmospheric pressure relative to mean sea
            level in hectopascals scaled by ten, followed by a quality code with
            values of 0, 1, 4, 5, and 9 being acceptable for our purposes. This
            function parses the string value and converts valid atmospheric
            pressure readings to kPa.
            '''
            press_regex = re.compile(r'^\d{5},[\d\w]{1},(\d{5}),[01459]$')
            if isinstance(press_str,str) and press_regex.search(press_str):
                press = int(press_regex.match(press_str).groups()[0])/100
                if press==999.99:
                    press = np.nan
            else:
                press = np.nan
            return press
        def calculate_wet_bulb_temperature(dry_bulb_temperature:float,dew_point:float,pressure:float):
            '''
            uses the metpy library to calculate wet bulb temperature using
            iterative Normand method (find lifting condensation level)

            parameters:
                dry_bulb_temperature - the dry-bulb temperature in degrees
                    celsius
                dew_point - the temperature in degrees celsius at which water
                    will condense from saturated air based on given conditions
                pressure - the atmospheric pressure in kPa
            returns:
                wet_bulb_temperature - the wet-bulb temperature in degrees
                    celsius
            '''
            # apply metpy units:
            dry_bulb_temperature = dry_bulb_temperature * units.degree_Celsius
            dew_point = dew_point *units.degree_Celsius
            pressure = pressure * units.kilopascal
            wet_bulb_temperature = mpcalc.wet_bulb_temperature(pressure,dry_bulb_temperature,dew_point)
            return np.round(wet_bulb_temperature.magnitude,1)
        if use_processed and self.data_paths['processed_weather_data_filename'].is_file():
            print('Loading Pre-Processed Weather Data ...')
            df = ddf.read_csv(self.data_paths['processed_weather_data_filename'])
            df['DATE'] = ddf.to_datetime(df['DATE'])
        else:
            print('Loading Original Weather Data Files ...')
            df = pd.DataFrame()
            fn_match = re.compile(r'[A-Z]{4}.csv')
            for fn in filter(lambda p:fn_match.search(p.name),self.data_paths['weather_data_directory'].iterdir()):
                print('\t{}'.format(fn.name))
                df = pd.concat([df,pd.read_csv(fn,low_memory=False)],axis='index')
            # replace missing call signs based on map from complete rows:
            station_list = df.loc[(df.loc[:,'CALL_SIGN']!='99999'),['STATION','CALL_SIGN']].drop_duplicates()
            for _,station in station_list.iterrows():
                df.loc[df['STATION']==station.loc['STATION'],'CALL_SIGN'] = station.loc['CALL_SIGN']
            df.loc[:,'CALL_SIGN'] = df.loc[:,'CALL_SIGN'].map(str.strip)
            # extract fields required for later operations:
            df = df.loc[:,['CALL_SIGN','DATE','TMP','DEW','MA1']]
            df.loc[:,'DATE'] = pd.to_datetime(df.loc[:,'DATE'])
            df.loc[:,'DATE'] = df.loc[:,'DATE'].map(lambda t: t.replace(minute=0,second=0,microsecond=0))
            df.loc[:,'DRY BULB TEMPERATURE'] = df.loc[:,'TMP'].map(parse_temperature)
            df.loc[:,'DEW POINT'] = df.loc[:,'DEW'].map(parse_dew_point)
            df.loc[:,'PRESSURE'] = df.loc[:,'MA1'].map(parse_pressure).round(1)
            df = df.drop(columns=['TMP','DEW','MA1'])
            # drop rows with missing data:
            df = df.dropna(how='any',subset=['DRY BULB TEMPERATURE','DEW POINT','PRESSURE'])
            df = df.groupby(by=['CALL_SIGN','DATE']).last().reset_index()
            ### START Removing wet bulb temperatures to reduce calculation time ###
            # wet_bulb_temperature_hash = pd.Series()
            # def f(r):
            #     nonlocal wet_bulb_temperature_hash
            #     i = int(1e9*(100+r.loc['DRY BULB TEMPERATURE'])+1e5*(100+r.loc['DEW POINT'])+1e1*r.loc['PRESSURE'])
            #     if i in wet_bulb_temperature_hash.index:
            #         wet_bulb_temperature = wet_bulb_temperature_hash.loc[i]
            #     else:
            #         wet_bulb_temperature = calculate_wet_bulb_temperature(r.loc['DRY BULB TEMPERATURE'],r.loc['DEW POINT'],r.loc['PRESSURE'])
            #         wet_bulb_temperature_hash.loc[i] = wet_bulb_temperature
            #         wet_bulb_temperature_hash.sort_index(inplace=True)
            #     return wet_bulb_temperature
            # df.loc[:,'WET BULB TEMPERATURE'] = df.apply(f,axis='columns')
            ### END Removing wet bulb temperatures to reduce calculation time ###
            df.to_csv(self.data_paths['processed_weather_data_filename'],index=False)
            df = ddf.from_pandas(df,npartitions=16)
        self.weather_data = df

    def load_weather_station_map(self):
        '''
        Reads a file containing 
        '''
        print('Loading Weather Station Locations ...')
        df = pd.read_csv(self.data_paths['resources_to_weather_stations_map_filename'],low_memory=False)
        self.weather_station_map = ddf.from_pandas(df.loc[:,['ResourceID','UnitType','WeatherStationID']],npartitions=1)

    def load_weather_station_placenames(self):
        self.weather_station_placenames = ddf.read_csv(self.data_paths['weather_station_placenames_filename'])

    def load_all(self,use_processed:bool=True):
        self.load_weather(use_processed)
        self.load_resource_curtailments()
        self.load_weather_station_map()
        self.load_weather_station_placenames()

    def impute_zero_curtailments(self):
        '''
        Inserts zero-valued curtailments into hours during which no curtailment
        were reported for each resource.

        Returns:
            Dataframe with curtailments and weather data including imputed 
            zero-valued curtailments
        '''
        # Determine times in full curtailment dataset:
        min_datetime = self.resource_curtailments.loc[:,'DATETIME'].min().compute()
        max_datetime = self.resource_curtailments.loc[:,'DATETIME'].max().compute()
        datetimes = pd.DataFrame({'DATETIME':[min_datetime+td(hours=d_hrs) for d_hrs in range(int((max_datetime-min_datetime).total_seconds()/3600))]})
        # Merge records into full list of hours, copying relevant metadata where needed:
        df = datetimes.merge(self.resource_curtailments.loc[:,'RESOURCE ID'].unique().compute(),how='cross')
        df = df.merge(
            self.resource_curtailments.groupby(['RESOURCE ID','DATETIME']).last().reset_index().compute(),
            how='left',
            on=['RESOURCE ID','DATETIME']
        )
        df.loc[df.loc[:,'RESOURCE NAME'].isna(),'RESOURCE NAME'] = df.loc[(df.loc[:,'RESOURCE NAME'].isna()),['RESOURCE ID']].merge(
            self.resource_curtailments.loc[:,['RESOURCE ID','RESOURCE NAME']].groupby('RESOURCE ID').first().reset_index().compute(),
            how='left',
            on='RESOURCE ID'
        ).loc[:,'RESOURCE NAME']
        df.loc[df.loc[:,'RESOURCE PMAX MW'].isna(),'RESOURCE PMAX MW'] = df.loc[(df.loc[:,'RESOURCE PMAX MW'].isna()),['RESOURCE ID']].merge(
            self.resource_curtailments.loc[:,['RESOURCE ID','RESOURCE PMAX MW']].groupby('RESOURCE ID').mean().reset_index().compute(),
            how='left',
            on='RESOURCE ID'
        ).loc[:,'RESOURCE PMAX MW']
        # Insert assumed values and metadata:
        df.loc[df.loc[:,'CURTAILMENT MW'].isna(),'CURTAILMENT MW'] = 0
        df.loc[df.loc[:,'OUTAGE TYPE'].isna(),'OUTAGE TYPE'] = 'FORCED'
        df.loc[df.loc[:,'NATURE OF WORK'].isna(),'NATURE OF WORK'] = 'AMBIENT_DUE_TO_TEMP'
        df = df.merge(
            self.weather_station_map.compute(),
            how='left',
            left_on='RESOURCE ID',
            right_on='ResourceID'
        )
        df = df.merge(
            self.weather_data.compute(),
            how='left',
            left_on=['WeatherStationID','DATETIME'],
            right_on=['CALL_SIGN','DATE']
        )
        # filter for forced outages due to temperature:
        df = df.dropna(axis='index',how='all',subset=['DRY BULB TEMPERATURE'])
        df = df.loc[(df.loc[:,'OUTAGE TYPE']=='FORCED')&(df.loc[:,'NATURE OF WORK']=='AMBIENT_DUE_TO_TEMP'),:]
        df = ddf.from_pandas(df.loc[:,['DATETIME','RESOURCE ID','RESOURCE NAME','UnitType','CURTAILMENT MW','RESOURCE PMAX MW','WeatherStationID','DRY BULB TEMPERATURE']],npartitions=16)
        return df

    def regress(self,use_processed:bool=True,target_curtailment:float=0,maximum_curtailment:float=1.0,minimum_rsquared:float=0.0,unit_types:list=None,normalize_temperatures:bool=True,impute_zeros:bool=False):
        '''
        Performs merges to associate curtailments with weather data and
        calculates the best fit linear relationship between temperature and
        curtailment.
        '''
        if use_processed:
            print('Loading Pre-Processed Merged Curtailment and Weather Data')
            df0 = ddf.read_csv(self.data_paths['merged_data_filename'])
        else:
            if impute_zeros:
                df0 = self.impute_zero_curtailments()
            else:
                # Use only hours reported in curtailment data:
                df0 = self.resource_curtailments.merge(
                    self.weather_station_map,
                    left_on='RESOURCE ID',
                    right_on='ResourceID'
                ).merge(
                    self.weather_data,
                    left_on=['WeatherStationID','DATETIME'],
                    right_on=['CALL_SIGN','DATE']
                )
                df0 = df0.loc['DATETIME','RESOURCE ID','RESOURCE NAME','UnitType','CURTAILMENT MW','RESOURCE PMAX MW','WeatherStationID','DRY BULB TEMPERATURE']
            # drop records not matching given unit_type:
            if isinstance(unit_types,list):
                df0 = df0.loc[reduce(lambda x,y:x|y,[df0['UnitType']==unit_type for unit_type in unit_types]),:]
            df0 = df0.assign(PERCENT_CURTAILMENT=lambda r:r['CURTAILMENT MW']/r['RESOURCE PMAX MW'])
            df0 = df0.rename(columns={'PERCENT_CURTAILMENT':'PERCENT CURTAILMENT'})
            # remove implausible or missing temperatures:
            df0 = df0.loc[(df0.loc[:,'DRY BULB TEMPERATURE']<=100),:]
            df0 = df0.dropna(subset=['DRY BULB TEMPERATURE','PERCENT CURTAILMENT'],how='any')
            # df0 = df0.loc[(df0.loc[:,'DRY BULB TEMPERATURE']<=100)&(df0.loc[:,'WET BULB TEMPERATURE']<=100),:]
            # df0 = df0.dropna(subset=['DRY BULB TEMPERATURE','WET BULB TEMPERATURE','PERCENT CURTAILMENT'],how='any')
            print('Saving Curtailments and Temperatures to File ...')
            df0.to_csv(self.data_paths['merged_data_filename'],single_file=True,index=False)

        if len(self.regression_by_resource)>0:
            df1 = self.regression_by_resource
        else:
            # extract unique resource ids for aggregation:
            print('Performing Regression Analyses ...\n\t\tTarget Curtailment={:.2f}%\n\t\tMaximum Curtailment:{:.2f}%\n\t\tMinimum R-Squared:{:.3f}'.format(target_curtailment,maximum_curtailment,minimum_rsquared))
            df1 = df0['RESOURCE ID'].unique().compute().to_frame()

            # append unit type to resource list:
            df1 = df1.merge(
                df0[['RESOURCE ID','UnitType']].groupby('RESOURCE ID').first().rename(columns={'UnitType':'UNIT TYPE'}).compute(),
                on='RESOURCE ID'
            )

            # count hours of curtailment for each resource (hours of curtailment):
            df1 = df1.merge(
                df0.groupby('RESOURCE ID')['DATETIME'].count().reset_index().rename(columns={'DATETIME':'NUMBER OF OBSERVATIONS'}).compute(),
                on='RESOURCE ID'
            )
            #df1 = df1.loc[(df1['NUMBER OF OBSERVATIONS']>300),:]

            # calculate correlations:
            df0_pd_g = df0.loc[(df0['PERCENT CURTAILMENT']<0.3),['RESOURCE ID','DRY BULB TEMPERATURE','PERCENT CURTAILMENT']].compute().groupby('RESOURCE ID')
            # df0_pd_g = df0.loc[(df0['PERCENT CURTAILMENT']<0.3),['RESOURCE ID','DRY BULB TEMPERATURE','WET BULB TEMPERATURE','PERCENT CURTAILMENT']].compute().groupby('RESOURCE ID')
            df1_pd = df0_pd_g.corr().reset_index()
            df1_pd = df1_pd.loc[(df1_pd.loc[:,'level_1']=='PERCENT CURTAILMENT'),:].rename(columns={'DRY BULB TEMPERATURE':'CORRELATION DRY BULB TEMPERATURE'})
            # df1_pd = df1_pd.loc[(df1_pd.loc[:,'level_1']=='PERCENT CURTAILMENT'),:].rename(columns={'DRY BULB TEMPERATURE':'CORRELATION DRY BULB TEMPERATURE','WET BULB TEMPERATURE':'CORRELATION WET BULB TEMPERATURE'})
            df1_pd = df1_pd.drop(columns=['level_1','PERCENT CURTAILMENT'])
            df1 = df1.merge(df1_pd,on='RESOURCE ID')

            # calculate covariances:
            df2_pd = df0_pd_g.cov().reset_index()
            df2_pd = df2_pd.loc[(df2_pd.loc[:,'level_1']=='PERCENT CURTAILMENT')].rename(columns={'DRY BULB TEMPERATURE':'COV DRY BULB TEMPERATURE'})
            # df2_pd = df2_pd.loc[(df2_pd.loc[:,'level_1']=='PERCENT CURTAILMENT')].rename(columns={'DRY BULB TEMPERATURE':'COV DRY BULB TEMPERATURE','WET BULB TEMPERATURE':'COV WET BULB TEMPERATURE'})
            df2_pd = df2_pd.drop(columns=['level_1','PERCENT CURTAILMENT'])
            df1 = df1.merge(df2_pd,on='RESOURCE ID')

            # find best-fit lines for each resource using both dry- and wet-bulb temperatures:
            def f(r):
                print('\tPerforming Linear Regression on Resource: {}'.format(r.loc['RESOURCE ID']))
                df0_f = df0.loc[
                    (df0['RESOURCE ID']==r['RESOURCE ID'])&
                    (df0['PERCENT CURTAILMENT']<maximum_curtailment),
                    [
                        'DRY BULB TEMPERATURE',
                        # 'WET BULB TEMPERATURE',
                        'PERCENT CURTAILMENT',
                    ]
                ].compute()
                t_d = df0_f.loc[:,'DRY BULB TEMPERATURE'].values.reshape(len(df0_f),1)
                # t_w = df0_f.loc[:,'WET BULB TEMPERATURE'].values.reshape(len(df0_f),1)
                c = df0_f.loc[:,'PERCENT CURTAILMENT'].values.reshape(len(df0_f),1)
                lr_d = LinearRegression().fit(t_d,c)
                # lr_w = LinearRegression().fit(t_w,c)
                sc_d = lr_d.score(t_d,c)
                # sc_w = lr_w.score(t_w,c)
                linear_regression_results = {
                    'DRY BULB SLOPE' : lr_d.coef_[0][0],
                    'DRY BULB INTERCEPT' : lr_d.intercept_[0],
                    'DRY BULB RSQUARED' : sc_d,
                    # 'WET BULB SLOPE' : lr_w.coef_[0][0],
                    # 'WET BULB INTERCEPT' : lr_w.intercept_[0],
                    # 'WET BULB RSQUARED' : sc_w,
                }
                print('\tC = {DRY BULB SLOPE:.2%}*T + {DRY BULB INTERCEPT:.2%}\tR-Squared: {DRY BULB RSQUARED:.4f}'.format(**linear_regression_results))
                return linear_regression_results
            df1 = pd.concat([df1,df1.apply(f,axis='columns',result_type='expand')],axis='columns')
            df1.to_csv(self.data_paths['regression_by_resource_filename'],index=False)
            self.regression_by_resource = df1

        df0 = df0.merge(df1.loc[:,['RESOURCE ID','DRY BULB SLOPE','DRY BULB INTERCEPT','DRY BULB RSQUARED']],on='RESOURCE ID')
        # normalize wet bulb temperatures based on regression coefficients:
        # df0 = df0.merge(df1.loc[:,['RESOURCE ID','DRY BULB SLOPE','DRY BULB INTERCEPT','DRY BULB RSQUARED','WET BULB SLOPE','WET BULB INTERCEPT','WET BULB RSQUARED']],on='RESOURCE ID')
        df0 = df0.assign(
            NORMALIZED_DRY_BULB_TEMPERATURE=lambda r:r['DRY BULB TEMPERATURE']+(r['DRY BULB INTERCEPT']-target_curtailment)/r['DRY BULB SLOPE'],
            # NORMALIZED_WET_BULB_TEMPERATURE=lambda r:r['WET BULB TEMPERATURE']+(r['WET BULB INTERCEPT']-target_curtailment)/r['WET BULB SLOPE']
        )
        df0 = df0.rename(columns={'NORMALIZED_DRY_BULB_TEMPERATURE':'NORMALIZED DRY BULB TEMPERATURE'})
        # df0 = df0.rename(columns={'NORMALIZED_DRY_BULB_TEMPERATURE':'NORMALIZED DRY BULB TEMPERATURE','NORMALIZED_WET_BULB_TEMPERATURE':'NORMALIZED WET BULB TEMPERATURE'})
        if normalize_temperatures:
            temperature_column = 'NORMALIZED DRY BULB TEMPERATURE'
        else:
            temperature_column = 'DRY BULB TEMPERATURE'
        df2 = pd.DataFrame(df0['UnitType'].unique().to_frame().rename(columns={'UnitType':'UNIT TYPE'}).compute())
        def f(r):
            print('\tPerforming Linear Regression on Unit Type: {}'.format(r.loc['UNIT TYPE']))
            df0_f = df0.loc[
                (df0['UnitType']==r['UNIT TYPE'])&
                (df0[temperature_column]>-1e6)&
                (df0[temperature_column]<1e6)&
                (df0['PERCENT CURTAILMENT']<maximum_curtailment),
                [
                    temperature_column,
                    # 'NORMALIZED WET BULB TEMPERATURE',
                    'PERCENT CURTAILMENT',
                    'DRY BULB RSQUARED',
                    # 'WET BULB RSQUARED',
                ]
            ].compute()
            df0_f_d = df0_f.loc[(df0_f.loc[:,'DRY BULB RSQUARED']>minimum_rsquared),[temperature_column,'PERCENT CURTAILMENT']]
            df0_f_d = df0_f.loc[(df0_f.loc[:,'DRY BULB RSQUARED']>minimum_rsquared),[temperature_column,'PERCENT CURTAILMENT']]
            # df0_f_w = df0_f.loc[(df0_f.loc[:,'WET BULB RSQUARED']>minimum_rsquared),['NORMALIZED WET BULB TEMPERATURE','PERCENT CURTAILMENT']]
            t_d = df0_f_d.loc[:,temperature_column].values.reshape(len(df0_f_d),1)
            # t_w = df0_f_w.loc[:,'NORMALIZED WET BULB TEMPERATURE'].values.reshape(len(df0_f_w),1)
            c_d = df0_f_d.loc[:,'PERCENT CURTAILMENT'].values.reshape(len(df0_f_d),1)
            # c_w = df0_f_w.loc[:,'PERCENT CURTAILMENT'].values.reshape(len(df0_f_w),1)
            if len(t_d)>0:
                lr_d = LinearRegression().fit(t_d,c_d)
                lr_d_coef = lr_d.coef_[0][0]
                lr_d_intercept = lr_d.intercept_[0]
                sc_d = lr_d.score(t_d,c_d)
            else:
                lr_d_coef = np.nan
                lr_d_intercept = np.nan
                sc_d = np.nan
            # if len(t_w)>0:
            #     lr_w = LinearRegression().fit(t_w,c_w)
            #     lr_w_coef = lr_w.coef_[0][0]
            #     lr_w_intercept = lr_w.intercept_[0]
            #     sc_w = lr_w.score(t_w,c_w)
            # else:
            #     lr_w_coef = np.nan
            #     lr_w_intercept = np.nan
            #     sc_w = np.nan
            linear_regression_results = {
                'DRY BULB SLOPE' : lr_d_coef,
                'DRY BULB INTERCEPT' : lr_d_intercept,
                'DRY BULB RSQUARED' : sc_d,
                # 'WET BULB SLOPE' : lr_w_coef,
                # 'WET BULB INTERCEPT' : lr_w_intercept,
                # 'WET BULB RSQUARED' : sc_w,
            }
            return linear_regression_results
        df2 = pd.concat([df2,df2.apply(f,axis='columns',result_type='expand')],axis='columns')
        df2.loc[:,'MAXIMUM CURTAILMENT'] = maximum_curtailment
        df2.loc[:,'MINIMUM RSQUARED'] = minimum_rsquared
        df2.loc[:,'TARGET CURTAILMENT'] = target_curtailment
        df2.to_csv(self.data_paths['regression_by_unit_type_filename'],index=False)
        self.curtailments_and_temperatures = df0
        self.regression_by_unit_type = df2

    def multilinear_regress(self,use_processed:bool=True,maximum_curtailment:float=1.0,unit_types:list=None,impute_zeros:bool=False):
        '''
        Performs merges to associate curtailments with weather data and
        calculates the best fit linear relationship between temperature and
        curtailment with additional binary variables to allow optimal alignment
        of each individual resource's curtailment data.
        '''
        if use_processed:
            print('Loading Pre-Processed Merged Curtailment and Weather Data')
            df0 = ddf.read_csv(self.data_paths['merged_data_filename'])
        else:
            if impute_zeros:
                df0 = self.impute_zero_curtailments()
            else:
                # Use only hours reported in curtailment data:
                df0 = self.resource_curtailments.merge(
                    self.weather_station_map,
                    left_on='RESOURCE ID',
                    right_on='ResourceID'
                ).merge(
                    self.weather_data,
                    left_on=['WeatherStationID','DATETIME'],
                    right_on=['CALL_SIGN','DATE']
                )
                df0 = df0.loc[:,['DATETIME','RESOURCE ID','RESOURCE NAME','UnitType','CURTAILMENT MW','RESOURCE PMAX MW','WeatherStationID','DRY BULB TEMPERATURE']]
            # drop records not matching given unit_type:
            if isinstance(unit_types,list):
                df0 = df0.loc[reduce(lambda x,y:x|y,[df0['UnitType']==unit_type for unit_type in unit_types]),:]
            df0 = df0.assign(PERCENT_CURTAILMENT=lambda r:r['CURTAILMENT MW']/r['RESOURCE PMAX MW'])
            df0 = df0.rename(columns={'PERCENT_CURTAILMENT':'PERCENT CURTAILMENT'})
            # remove implausible or missing temperatures:
            df0 = df0.loc[(df0.loc[:,'DRY BULB TEMPERATURE']<=100),:]
            df0 = df0.dropna(subset=['DRY BULB TEMPERATURE','PERCENT CURTAILMENT'],how='any')
            # df0 = df0.loc[(df0.loc[:,'DRY BULB TEMPERATURE']<=100)&(df0.loc[:,'WET BULB TEMPERATURE']<=100),:]
            # df0 = df0.dropna(subset=['DRY BULB TEMPERATURE','WET BULB TEMPERATURE','PERCENT CURTAILMENT'],how='any')
            print('Saving Curtailments and Temperatures to File ...')
            df0.to_csv(self.data_paths['merged_data_filename'],single_file=True,index=False)

        if len(self.regression_by_resource)>0:
            df1 = self.regression_by_resource
        else:
            # extract unique resource ids for aggregation:
            print('Performing Multilinear Regression Analyses ...\n\t\tMaximum Curtailment:{:.2f}%\n\t\tMinimum R-Squared:{:.3f}'.format(maximum_curtailment,minimum_rsquared))
            df1 = df0['RESOURCE ID'].unique().compute().to_frame()
            resource_ids = list(df1.loc[:,'RESOURCE ID'])
            for resource_id in resource_ids:
                df1 = df1.assign(new_col=df1.loc[:,'RESOURCE ID'].map(lambda s:1 if s==resource_id else 0))
                df1 = df1.rename(columns={'new_col':resource_id})
            df0 = df0.merge(ddf.from_pandas(df1,npartitions=1),how='left',on='RESOURCE ID')

            # append unit type to resource list:
            df1 = df1.merge(
                df0.loc[:,['RESOURCE ID','UnitType']].groupby('RESOURCE ID').first().rename(columns={'UnitType':'UNIT TYPE'}).compute(),
                on='RESOURCE ID'
            )

            # count hours of curtailment for each resource (hours of curtailment):
            df1 = df1.merge(
                df0.groupby('RESOURCE ID')['DATETIME'].count().reset_index().rename(columns={'DATETIME':'NUMBER OF OBSERVATIONS'}).compute(),
                on='RESOURCE ID'
            )
            #df1 = df1.loc[(df1['NUMBER OF OBSERVATIONS']>300),:]

        df2 = pd.DataFrame({'UNIT TYPE':unit_types})
        def f(r):
            print('\tPerforming Linear Regression on Unit Type: {}'.format(r.loc['UNIT TYPE']))
            resource_columns = list(df0.loc[(df0.loc[:,'UnitType']==r.loc['UNIT TYPE']),'RESOURCE ID'].unique().compute())
            df0_f = df0.loc[
                (df0['UnitType']==r['UNIT TYPE'])&
                (df0['DRY BULB TEMPERATURE']>-1e6)&
                (df0['DRY BULB TEMPERATURE']<1e6)&
                (df0['PERCENT CURTAILMENT']<maximum_curtailment),
                [
                    'DRY BULB TEMPERATURE',
                    'PERCENT CURTAILMENT',
                ] + resource_columns
            ].compute()
            t_d = df0_f.loc[:,['DRY BULB TEMPERATURE']+resource_columns].values.reshape(len(df0_f),len(resource_columns)+1)
            c_d = df0_f.loc[:,'PERCENT CURTAILMENT'].values.reshape(len(df0_f),1)
            # c_w = df0_f_w.loc[:,'PERCENT CURTAILMENT'].values.reshape(len(df0_f_w),1)
            if len(t_d)>0:
                lr_d = LinearRegression().fit(t_d,c_d)
                lr_d_coef = lr_d.coef_[0][0]
                lr_d_intercept = lr_d.intercept_[0]
                sc_d = lr_d.score(t_d,c_d)
            else:
                lr_d_coef = np.nan
                lr_d_intercept = np.nan
                sc_d = np.nan
            # if len(t_w)>0:
            #     lr_w = LinearRegression().fit(t_w,c_w)
            #     lr_w_coef = lr_w.coef_[0][0]
            #     lr_w_intercept = lr_w.intercept_[0]
            #     sc_w = lr_w.score(t_w,c_w)
            # else:
            #     lr_w_coef = np.nan
            #     lr_w_intercept = np.nan
            #     sc_w = np.nan
            linear_regression_results = {
                'DRY BULB SLOPE' : lr_d_coef,
                'DRY BULB INTERCEPT' : lr_d_intercept,
                'DRY BULB RSQUARED' : sc_d,
                # 'WET BULB SLOPE' : lr_w_coef,
                # 'WET BULB INTERCEPT' : lr_w_intercept,
                # 'WET BULB RSQUARED' : sc_w,
            }
            return linear_regression_results
        df2 = pd.concat([df2,df2.apply(f,axis='columns',result_type='expand')],axis='columns')
        df2.loc[:,'MAXIMUM CURTAILMENT'] = maximum_curtailment
        df2.loc[:,'MINIMUM RSQUARED'] = minimum_rsquared
        df2.loc[:,'TARGET CURTAILMENT'] = target_curtailment
        df2.to_csv(self.data_paths['regression_by_unit_type_filename'],index=False)
        self.curtailments_and_temperatures = df0
        self.regression_by_unit_type = df2

if __name__=='__main__':
    use_processed = False
    directory = Path(r'M:\Users\RH2\src\caiso_curtailments')
    # target_curtailments = [0.01,0.02,0.03,0.04,0.05,0.055,0.06,0.07,0.08,0.09,0.10,0.15,0.20,0.25,0.30]
    target_curtailments = [0.07]
    maximum_curtailment = 0.3
    # maximum_curtailment = 1.0
    # minimum_rsquared = 0.35
    minimum_rsquared = 0.0
    data_paths = {
        # 'resource_curtailments_filename' : directory / 'results/curtailments_ambient_due_to_temp.csv',
        'resource_curtailments_filename' : directory / 'results/curtailments_all.csv',
        'weather_data_directory' : directory / 'weather_data',
        'processed_weather_data_filename' : directory / 'weather_data/ambient_temperatures.csv',
        'resources_to_weather_stations_map_filename' : directory / 'geospatial/resource_weather_stations.csv',
        'weather_station_placenames_filename' : directory / 'geospatial/weather_station_placenames.csv',
        # 'regression_by_resource_filename' : directory / 'results/regression_parameters_by_resource.csv',
        'regression_by_resource_filename' : directory / 'results/regression_parameters_by_resource_imputed_zeros.csv',
        # 'regression_by_unit_type_filename' : directory / 'results/regression_parameters_by_unit_type_matchpoint{:02.0f}percent.csv'.format(100*target_curtailments[0]),
        # 'regression_by_unit_type_filename' : directory / 'results/regression_parameters_by_unit_type_matchpoint{:02.0f}percent_imputed_zeros_unnormalized.csv'.format(100*target_curtailments[0]),
        # 'regression_by_unit_type_filename' : directory / 'results/regression_parameters_by_unit_type_imputed_zeros_unnormalized.csv'.format(100*target_curtailments[0]),
        'regression_by_unit_type_filename' : directory / 'results/regression_parameters_by_unit_type_imputed_zeros_multilinear.csv'.format(100*target_curtailments[0]),
        # 'merged_data_filename' : directory / 'results/curtailments_and_temperatures.csv'
        'merged_data_filename' : directory / 'results/curtailments_and_temperatures_imputed_zeros.csv'
        # 'merged_data_filename' : directory / 'results/curtailments_and_temperatures_imputed_zeros_unnormalized.csv'

    }
    unit_types = ['COMBUSTION TURBINE','COMBINED CYCLE','STEAM','RECIPROCATING ENGINE']
    normalize_temperatures = True
    # normalize_temperatures = False
    impute_zeros = False
    curtailment_modeller = CurtailmentModeller(data_paths)
    curtailment_modeller.load_all(use_processed=use_processed)
    regression_by_unit_type = pd.DataFrame(columns=[
        'UNIT TYPE',
        'DRY BULB SLOPE',
        'DRY BULB INTERCEPT',
        'DRY BULB RSQUARED',
        # 'WET BULB SLOPE',
        # 'WET BULB INTERCEPT',
        # 'WET BULB RSQUARED',
        'MAXIMUM CURTAILMENT',
        'MINIMUM RSQUARED',
        'TARGET CURTAILMENT'
    ])
    for target_curtailment in target_curtailments:
        # data_paths['regression_by_unit_type_filename'] = directory / 'results/regression_parameters_by_unit_type_matchpoint{:02.0f}percent.csv'.format(100*target_curtailment)
        # data_paths['regression_by_unit_type_filename'] = directory / 'results/regression_parameters_by_unit_type_matchpoint{:02.0f}percent_imputed_zeros.csv'.format(100*target_curtailment)
        curtailment_modeller.set_data_paths(data_paths)
        # curtailment_modeller.regress(
        #     use_processed=use_processed,
        #     target_curtailment=target_curtailment,
        #     maximum_curtailment=maximum_curtailment,
        #     minimum_rsquared=minimum_rsquared,
        #     unit_types=unit_types,
        #     normalize_temperatures=normalize_temperatures,
        #     impute_zeros=impute_zeros,
        # )
        curtailment_modeller.multilinear_regress(
            use_processed=use_processed,
            maximum_curtailment=maximum_curtailment,
            unit_types=unit_types,
            impute_zeros=impute_zeros
        )
        regression_by_unit_type = pd.concat([regression_by_unit_type,curtailment_modeller.regression_by_unit_type],axis='index',ignore_index=True)
    # regression_by_unit_type.to_csv(directory / 'results/regression_parameters_by_unit_type.csv',index=False)
    # regression_by_unit_type.to_csv(directory / 'results/regression_parameters_by_unit_type_imputed_zeros.csv',index=False)
    regression_by_unit_type.to_csv(directory / 'results/regression_parameters_by_unit_type_imputed_zeros_unnormalized.csv',index=False)