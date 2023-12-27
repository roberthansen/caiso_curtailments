import re
import pandas as pd
from pathlib import Path

def combine_derate_files(file_paths:list,scenarios:list,output_path:Path):
    '''
    Concatenates a set of files specified in the input file_paths list
    corresponding to a set of input scenarios into a single dataframe, and saves
    the results to the output_path.

    Parameters:
        files_path - a flat list of pathlib Path objects pointing to files with
            combined nearest-weather station forecast temperatures and predicted
            ambient derate data.
        scenarios - a flat list of scenarios describing each of the files in
            files_path.
        output_path - a pathlib Path object specifying where to save the resulting
            dataframe in .csv format.
    '''
    derates = pd.DataFrame(columns=['Weather Name','Hour','Weather Factor','Randomize Profile'])
    for i,file_path,scenario in enumerate(zip(file_paths,scenarios)):
        print('Reading File {:02.0f} of {:02.0f} - {}'.format(i,len(file_paths),file_path.name))
        new_derates = pd.read_csv(file_path)
        station_id,unit_type,year = re.match(r'^([a-z_]+)-([A-Z]{4})_(\d{4}).*$',file_path.name).groups()
        new_derates.loc[:,'Weather Name']='{}_{}_{}_{}'.format(year,scenario,station_id,unit_type)
        new_derates.loc[:,'Weather Factor']=(new_derates.loc[:,'Weather Factor']*100).astype('int')
        new_derates = new_derates.iloc[range(8760)]
        derates = pd.concat([derates,new_derates])
    derates.to_csv(output_path,index=False)


if __name__=='__main__':
    resource_weather_station_pairs = pd.read_csv(Path(r'M:\Users\RH2\src\caiso_curtailments\results\resource_weather_station_pairs.csv'))
    weather_stations = resource_weather_station_pairs.loc[:,'StationID'].unique()
    # weather_stations = ['KSFO']
    unit_types = ['combined_cycle','combustion_turbine']
    years = list(range(1997,2021))
    scenarios = [f'{x}_{y}' for x in [15,20,30] for y in [25,50,75]] + ['historic_weather']
    derate_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\cif_derates')
    file_paths = []
    matched_scenarios = []
    for unit_type in unit_types:
        for year in years:
            for scenario in scenarios:
                for weather_station in weather_stations:
                    file_paths += [derate_directory / scenario / '{}-{}_{}.csv'.format(unit_type,weather_station,year)]
                    matched_scenarios += [scenario]
    combine_derate_files(file_paths,matched_scenarios,derate_directory / 'derates_for_servm_multilinear' / 'thermal_ambient_derates_weather_data.csv')