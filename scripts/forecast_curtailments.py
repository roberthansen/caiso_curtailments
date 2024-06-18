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
            # 'combined_cycle' : -0.000967521223052699,
            # 'combustion_turbine' : -0.001381514787121,
            # 1-stage multilinear regression results, updated 2024-01-04:
            'combined_cycle' : -0.001103443043600427,
            'combustion_turbine' : -0.0014410010374964729,
            'steam' : -0.0011541215631732035,
            'reciprocating_engine' : -0.0011576322644806548,
        },
        'intercepts' : {
            'KABQ' : {'combined_cycle': 0.9873827345897896, 'combustion_turbine': 0.9881959092821242},
            'KACV' : {'combined_cycle': 0.9981401856060664, 'combustion_turbine': 0.9982600494551983},
            'KAKO' : {'combined_cycle': 0.973149564870096, 'combustion_turbine': 0.9748800582548305},
            'KBCE' : {'combined_cycle': 0.9786222973899497, 'combustion_turbine': 0.9800000766612551},
            'KBFL' : {'combined_cycle': 1.003811095069536, 'combustion_turbine': 1.0035654724278722},
            'KBLH' : {'combined_cycle': 1.0032521344593373, 'combustion_turbine': 1.0030425364717843},
            'KBOI' : {'combined_cycle': 0.9820726087929028, 'combustion_turbine': 0.9832280176992888},
            'KBOK' : {'combined_cycle': 1.003320734170589, 'combustion_turbine': 1.003106714975486},
            'KBUR' : {'combined_cycle': 1.0067075273223833, 'combustion_turbine': 1.0062752314730552},
            'KBZN' : {'combined_cycle': 0.9585454485636342, 'combustion_turbine': 0.961217167911224},
            'KCAG' : {'combined_cycle': 0.9783010501875662, 'combustion_turbine': 0.9796995336357649},
            'KCQT' : {'combined_cycle': 0.9925652774912797, 'combustion_turbine': 0.9930444406056363},
            'KCRQ' : {'combined_cycle': 1.0169919785629176, 'combustion_turbine': 1.0158968564036521},
            'KCYS' : {'combined_cycle': 0.9756801319962678, 'combustion_turbine': 0.9772475319469377},
            'KDNR' : {'combined_cycle': 0.9770216374607446, 'combustion_turbine': 0.9785025782415487}
        },
        'rated_temperatures' : {
            'KABQ' : -10.345833333333333,
            'KACV' : -1.5250000000000001,
            'KAKO' : -22.016666666666666,
            'KBCE' : -17.529166666666665,
            'KBFL' : 3.125,
            'KBLH' : 2.6666666666666665,
            'KBOI' : -14.700000000000001,
            'KBOK' : 2.7229166666666664,
            'KBUR' : 5.5,
            'KBZN' : -33.99166666666667,
            'KCAG' : -17.792581115566897,
            'KCQT' : -6.0962813616139995,
            'KCRQ' : 13.932985674792661,
            'KCYS' : -19.941666666666666,
            'KDNR' : -18.841666666666665,
            'KEKA' : 2.960144927536232,
            'KEKO' : -16.45,
            'KELP' : -3.725,
            'KFAT' : 2.125,
            'KFLG' : -22.175,
            'KFSD' : -25.508333333333336,
            'KGEG' : -24.991666666666664,
            'KIDA' : -27.316666666666666,
            'KIGM' : -6.558333333333334,
        }
    }
    def __init__(self,weather_data_path:Path):
        self.weather_data_path = weather_data_path
        self.weather_data_meta,self.weather_data = read_cif(self.weather_data_path)
        self.weather_data.loc[:,'DateTime'] = self.weather_data.loc[:,'DateTime'].dt.tz_localize(None)

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
    historic_weather_data_path = Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data\ncdc_1978_2023.parquet')
    historic_derates = DerateForecaster(
        weather_data_path=historic_weather_data_path,
    )
    historic_derates.calculate_derate_intercepts()
    historic_derates.calculate_derates()

    unit_types = ['combined_cycle','combustion_turbine']
    weather_stations = ['KNKX','KOAK','KRDD','KRNO','KSAC','KSAN','KSBA','KSCK','KSFO','KSJC','KSMF','KUKI']

    for unit_type in unit_types:
        for weather_station in weather_stations:
            # for year in historic_derates.derates.loc[(historic_derates.derates.loc[:,'StationID']==weather_station),'DateTime'].map(lambda t: t.year).unique():
            for year in [2022,2023]:
                historic_derates.save_derates({'unit_type':unit_type,'weather_station':weather_station},year=ts(year,1,1),save_directory=Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates')/'historic_weather')

    # calculate derates for climate-informed weather using derate parameters
    # from historic weather data:
    weather_data_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data')
    # cif_scenarios = ['15_25','15_50','15_75','20_25','20_50','20_75','30_25','30_50','30_75']
    # cif_scenarios = ['20_75','30_25','30_50','30_75']
    cif_scenarios = []
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