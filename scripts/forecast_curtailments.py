from pathlib import Path
import pandas as pd
from pandas import Timestamp as ts
from read_cifs import read_cif
import json
import re

class DerateForecaster:
    '''
    Reads weather data and generates hourly ambient temperature derates
    according to input parameters.
    '''
    derate_parameters = {
        'slopes' : {
            # 2-stage regression results:
            # 'combined_cycle' : -0.001650,
            # 'combustion_turbine' : -0.002135,
            # 1-stage multilinear regression results:
            'combined_cycle' : -0.000967521223052699,
            'combustion_turbine' : -0.001381514787121,
        },
        'intercepts' : {
        },
        'rated_temperatures' : {
        }
    }
    def __init__(self,weather_data_path:Path):
        self.weather_data_path = weather_data_path
        self.weather_data_meta,self.weather_data = read_cif(self.weather_data_path)

    def get_derate_parameters(self,resource_class:dict):
        derate_parameters = {
            'slope' : self.derate_parameters['slopes'][resource_class['unit_type']],
            'intercept' : self.derate_parameters['intercepts'][resource_class['weather_station']][resource_class['unit_type']],
            'rated_temperature' : self.derate_parameters['rated_temperatures'][resource_class['weather_station']],
        }
        return derate_parameters

    def calculate_derate_intercepts(self):
        '''
        calculates the derate intercept parameters for each weather station as
        the lowest daily average temperature in the first year of weather data
        for a given weather station.
        '''
        print('Calculating derate intercepts ...')
        weather_data = self.weather_data
        f = lambda t: t.replace(hour=0,minute=0,second=0,microsecond=0)
        weather_data.loc[:,'Date'] = weather_data.loc[:,'DateTime'].map(f)
        f = lambda t: t.replace(month=1,day=1,hour=0,minute=0,second=0,microsecond=0)
        weather_data.loc[:,'Year'] = weather_data.loc[:,'DateTime'].map(f)
        min_temps = weather_data.groupby(['StationID','Year','Date']).mean().reset_index().groupby(['StationID','Year']).min().reset_index().loc[:,['StationID','Year','Temp']]
        for station_id in self.weather_data.loc[:,'StationID'].unique():
            # the 'rated temperature' is defined here as the lowest daily
            # average temperature in the first available year of weather data
            # for a given weather station:
            rated_temperature = min_temps.loc[(min_temps.loc[:,'StationID']==station_id)&(min_temps.loc[:,'Year']==min_temps.loc[(min_temps.loc[:,'StationID']==station_id),'Year'].min()),'Temp'].iloc[0]
            self.derate_parameters['rated_temperatures'][station_id] = rated_temperature
            self.derate_parameters['intercepts'][station_id] = {
                'combined_cycle' : 1 - self.derate_parameters['slopes']['combined_cycle'] * rated_temperature,
                'combustion_turbine' : 1 - self.derate_parameters['slopes']['combustion_turbine'] * rated_temperature,
            }

    def calculate_derate(self,resource_class:dict,temperature:float):
        derate_parameters = self.get_derate_parameters(resource_class)
        derate = max(min(1,derate_parameters['slope'] * temperature + derate_parameters['intercept']),0)
        return derate

    def calculate_derates(self):
        print('Forecasting derates for {} ...'.format(self.weather_data_path))
        self.derates = self.weather_data.loc[:,['StationID','DateTime','Temp']]
        # self.derates = self.weather_data.loc[(self.weather_data.loc[:,'StationID']=='KLGB')|(self.weather_data.loc[:,'StationID']=='KSAC'),['StationID','DateTime','Temp']]
        f = lambda r: pd.Series({
            'combined_cycle' : self.calculate_derate(resource_class={'unit_type':'combined_cycle','weather_station':r.loc['StationID']},temperature=r.loc['Temp']),
            'combustion_turbine' : self.calculate_derate(resource_class={'unit_type':'combustion_turbine','weather_station':r.loc['StationID']},temperature=r.loc['Temp']),
        })
        self.derates.loc[:,['combined_cycle','combustion_turbine']] = self.derates.apply(f,axis='columns',result_type='expand')
        self.derates.sort_values(by=['StationID','DateTime'])

    def save_derates(self,resource_class:dict,year:ts,save_directory:Path):
        derates = self.derates.loc[(self.derates.loc[:,'StationID']==resource_class['weather_station'])&(self.derates.loc[:,'DateTime'].map(lambda t:t.year)==year.year),['StationID','DateTime',resource_class['unit_type']]]
        derates.loc[:,'Weather Name'] = [resource_class['unit_type']+' '+resource_class['weather_station']] * len(derates)
        f = lambda t: int((t-ts(t.year,1,1)).total_seconds()/3600) + 1
        derates = derates.assign(
            Hour=derates.loc[:,'DateTime'].map(f),
            WeatherFactor=derates.loc[:,resource_class['unit_type']],
            RandomizeProfile=['FALSE']*len(derates)
        )
        derates.rename(columns={'WeatherFactor':'Weather Factor','RandomizeProfile':'Randomize Profile'},inplace=True)
        save_directory.mkdir(parents=True,exist_ok=True)
        save_path = save_directory / '{}-{}_{}.csv'.format(resource_class['unit_type'],resource_class['weather_station'],year.year)
        print('Saving derates to {}'.format(save_path.name))
        derates.loc[:,['Weather Name','Hour','Weather Factor','Randomize Profile']].to_csv(save_path,index=False)

if __name__=='__main__':
    # get parameters for historic weather years:
    historic_weather_data_path = Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data\ncdc_1978_2021.parquet')
    historic_derates = DerateForecaster(
        weather_data_path=historic_weather_data_path
    )
    historic_derates.calculate_derate_intercepts()
    historic_derates.calculate_derates()

    unit_types = ['combined_cycle','combustion_turbine']
    weather_stations = ['KNKX','KOAK','KRDD','KRNO','KSAC','KSAN','KSBA','KSCK','KSFO','KSJC','KSMF','KUKI']

    for unit_type in unit_types:
        for weather_station in weather_stations:
            for year in historic_derates.derates.loc[(historic_derates.derates.loc[:,'StationID']==weather_station),'DateTime'].map(lambda t: t.year).unique():
                historic_derates.save_derates({'unit_type':unit_type,'weather_station':weather_station},year=ts(year,1,1),save_directory=Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates')/'historic_weather')

    # calculate derates for climate-informed weather using derate parameters
    # from historic weather data:
    weather_data_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data')
    # cif_scenarios = ['15_25','15_50','15_75','20_25','20_50','20_75','30_25','30_50','30_75']
    cif_scenarios = ['20_75','30_25','30_50','30_75']
    unit_types = ['combined_cycle','combustion_turbine']
    weather_stations = ['KNKX','KOAK','KRDD','KRNO','KSAC','KSAN','KSBA','KSCK','KSFO','KSJC','KSMF','KUKI']
    for cif_scenario in cif_scenarios:
        weather_data_path = weather_data_directory / 'cif_temperature_{}.parquet'.format(cif_scenario)
        derate_forecaster = DerateForecaster(
            weather_data_path=weather_data_path
        )
        derate_forecaster.derate_parameters = historic_derates.derate_parameters
        derate_forecaster.calculate_derates()
        for unit_type in unit_types:
            for weather_station in weather_stations:
                for year in derate_forecaster.derates.loc[(derate_forecaster.derates.loc[:,'StationID']==weather_station),'DateTime'].map(lambda t: t.year).unique():
                    derate_forecaster.save_derates({'unit_type':unit_type,'weather_station':weather_station},year=ts(year,1,1),save_directory=Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates')/cif_scenario)