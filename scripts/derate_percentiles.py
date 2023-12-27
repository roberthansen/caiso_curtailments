import pandas as pd
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


if __name__=='__main__':
    # calculate derate statistics for historic weather derates:
    # root_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\derates')
    filename_parser = re.compile(r'(\w*)-(\w{4})_(\d{4}).csv')
    derate_percentiles = [0.01,0.25,0.5,0.75,0.99]
    derate_statistics = pd.DataFrame(columns=['UnitType','WeatherStation','Year','ClimateScenario','ClimatePercentile']+list(map(lambda x:'DeratePercentile{:02.0f}'.format(100*x),derate_percentiles)))
    active_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates\historic_weather')
    climate_scenario = 0
    climate_percentile = 100
    if active_directory.is_dir():
        print(f'Reading Files for Historic Weather')
        for derate_file_path in active_directory.iterdir():
            if filename_parser.match(derate_file_path.name):
                print(f'Reading Derate File {derate_file_path.name}')
                unit_type,weather_station,year = filename_parser.match(derate_file_path.name).groups()
                derates = pd.read_csv(derate_file_path)
                new_row = pd.DataFrame(dict(zip(derate_statistics.columns,[unit_type,weather_station,year,climate_scenario,climate_percentile] + [derates.loc[:,'Weather Factor'].quantile(x) for x in derate_percentiles])),index=[0])
                derate_statistics = pd.concat([derate_statistics,new_row],ignore_index=True)
    for climate_scenario in [15,20,30]:
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