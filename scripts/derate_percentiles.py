import pandas as pd
from pandas import Timestamp as ts, Timedelta as td
from pathlib import Path
import re

def calculate_derate_percentiles(derate_file:Path,percentiles:list):
    '''
    Calculates and returns the input percentiles for the Weather Factor column
    of a specified input file.
    '''
    derates = pd.read_csv(derate_file)
    values = []
    for p in percentiles:
        values += [derates.loc[:,'Weather Factor'].quantile(p)]
    return values

def calculate_monthly_derate_percentiles(derate_files:list,derate_percentiles:list):
    '''
    Calculates derate percentiles on a monthly basis from a set of listed derate
    files. The files may span multiple weather years, unit types, and weather
    stations.
    '''
    derates = pd.DataFrame(columns=['Weather Name','Year','Month','Hour','Weather Factor','Randomize Profile'])
    get_year = re.compile(r'.*(\d{4}).*')
    # read each file and add year and month columns based on filename and hour:
    for derate_file in derate_files:
        file_derates = pd.read_csv(derate_file)
        file_derates.loc[:,'Year'] = int(get_year.match(derate_file.name).groups()[0])
        def calculate_month(r):
            return (ts(r.loc['Year'],1,1,0,0,0)+td(hours=r.loc['Hour']-1)).month
        file_derates.loc[:,'Month'] = file_derates.apply(calculate_month,axis='columns')
        derates = pd.concat([derates,file_derates],axis='index',ignore_index=True)
    values = pd.DataFrame(columns=['Month']+[f'Percentile {100*p:.0f}'for p in derate_percentiles])
    for month in range(1,13):
        values.loc[month,'Month'] = month
        for p in derate_percentiles:
            values.loc[month,f'Percentile {100*p:.0f}'] = derates.loc[(derates.loc[:,'Month']==month),'Weather Factor'].quantile(p)
    return values

if __name__=='__main__':
    # calculate derate statistics for historic weather derates:
    # root_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\derates')
    filename_parser = re.compile(r'(\w*)-(\w{4})_(\d{4}).csv')
    derate_percentiles = [0.01,0.25,0.5,0.75,0.99]
    derate_statistics = pd.DataFrame(columns=['UnitType','WeatherStation','Year','ClimateScenario','ClimatePercentile']+list(map(lambda x:'DeratePercentile{:02.0f}'.format(100*x),derate_percentiles)))
    active_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\historic_weather')
    climate_scenario = 0
    climate_percentile = 100
    # climate_scenarios = [15,20,30]
    climate_scenarios = []
    if active_directory.is_dir():
        # print(f'Analyzing annual percentiles by file for historic weather')
        # for derate_file_path in active_directory.iterdir():
        #     if filename_parser.match(derate_file_path.name):
        #         print(f'Reading Derate File {derate_file_path.name}')
        #         unit_type,weather_station,year = filename_parser.match(derate_file_path.name).groups()
        #         derates = pd.read_csv(derate_file_path)
        #         new_row = pd.DataFrame(dict(zip(derate_statistics.columns,[unit_type,weather_station,year,climate_scenario,climate_percentile] + [derates.loc[:,'Weather Factor'].quantile(x) for x in derate_percentiles])),index=[0])
        #         derate_statistics = pd.concat([derate_statistics,new_row],ignore_index=True)
        print('Analyzing monthly percentiles by resource class for historic weather')
        resource_classes = pd.DataFrame(columns=['UnitType','WeatherStation'])
        for derate_file_path in filter(lambda p: filename_parser.match(p.name),active_directory.iterdir()):
            unit_type,weather_station,year = filename_parser.match(derate_file_path.name).groups()
            resource_classes = pd.concat([resource_classes,pd.DataFrame([{'UnitType':unit_type,'WeatherStation':weather_station}])],ignore_index=True)
        resource_classes = resource_classes.groupby(['UnitType','WeatherStation']).size().reset_index().drop(columns=[0])
        monthly_derate_statistics = pd.DataFrame(columns=['UnitType','WeatherStation','Month']+[f'Percentile {100*p:.0f}' for p in derate_percentiles])
        for _,resource_class in resource_classes.iterrows():
            print('\tWeather Station: {}\tUnit Type: {}'.format(resource_class.loc['WeatherStation'],resource_class.loc['UnitType']))
            derate_files = filter(lambda p:resource_class.loc['UnitType']==filename_parser.match(p.name).groups()[0] and resource_class.loc['WeatherStation']==filename_parser.match(p.name).groups()[1],active_directory.iterdir())
            values = calculate_monthly_derate_percentiles(derate_files,derate_percentiles)
            values.loc[:,'UnitType'] = resource_class.loc['UnitType']
            values.loc[:,'WeatherStation'] = resource_class.loc['WeatherStation']
            monthly_derate_statistics = pd.concat([monthly_derate_statistics,values],ignore_index=True)
        monthly_derate_statistics.to_csv(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\monthly_derate_statistics_multilinear.csv',index=False)
    for climate_scenario in climate_scenarios:
        for climate_percentile in [25,50,75]:
            root_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\{:.0f}_{:.0f}'.format(climate_scenario,climate_percentile))
            active_directory = root_directory
            if active_directory.is_dir():
                print(f'Reading Files for Scenario {climate_scenario:.0f}_{climate_percentile:.0f}')
                for derate_file_path in active_directory.iterdir():
                    if filename_parser.match(derate_file_path.name):
                        print(f'Reading Derate File {derate_file_path.name}')
                        unit_type,weather_station,year = filename_parser.match(derate_file_path.name).groups()
                        derates = pd.read_csv(derate_file_path)
                        new_row = pd.DataFrame(dict(zip(derate_statistics.columns,[unit_type,weather_station,year,climate_scenario,climate_percentile] + [derates.loc[:,'Weather Factor'].quantile(x) for x in derate_percentiles])),index=[0])
                        derate_statistics = pd.concat([derate_statistics,new_row],ignore_index=True)
    # derate_statistics.to_csv(r'M:\Users\RH2\src\caiso_curtailments\derates\derate_statistics.csv',index=False)
    derate_statistics.to_csv(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\derate_statistics_multilinear.csv',index=False)

    # #Calculate derate statistics for climate-informed derates:
    # root_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates')
    # filename_parser = re.compile(r'(\w*)-(\w{4})_(\d{4}).csv')
    # percentiles = [0.01,0.25,0.5,0.75,0.99]
    # derate_statistics = pd.DataFrame(columns=['UnitType','WeatherStation','Year','ClimateScenario','ClimatePercentile']+list(map(lambda x:'DeratePercentile{:02.0f}'.format(100*x),percentiles)))
    # for climate_scenario in [1.5,2.0,3.0]:
    #     for climate_percentile in [25,50,75]:
    #         active_directory = root_directory / '{:0.0f}_{:0.0f}'.format(climate_scenario*10,climate_percentile)
    #         if active_directory.is_dir():
    #             for derate_file_path in active_directory.iterdir():
    #                 if filename_parser.match(derate_file_path.name):
    #                     print('Reading Derate File: '+derate_file_path.name)
    #                     unit_type,weather_station,year = filename_parser.match(derate_file_path.name).groups()
    #                     derates = pd.read_csv(derate_file_path)
    #                     new_row = pd.DataFrame(dict(zip(derate_statistics.columns,[unit_type,weather_station,year,climate_scenario,climate_percentile] + [derates.loc[:,'Weather Factor'].quantile(x) for x in percentiles])),index=[0])
    #                     derate_statistics = pd.concat([derate_statistics,new_row],ignore_index=True)
    # print(derate_statistics)
    # derate_statistics.to_csv(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\derate_statistics.csv',index=False)