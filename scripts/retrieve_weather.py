import pycurl
import requests
import pandas as pd
from pathlib import Path
from pandas import Timestamp as ts

from caiso_logging import DataLogger

class WeatherDownloader:
    '''
    A class to manage downloads of NCEI/NOAA hourly global surface temperatures.
    '''
    download_directory_path = Path()
    weather_stations = []
    years = []
    logger = None
    weather_station_placenames_path = Path(r'M:\Users\RH2\src\caiso_curtailments\geospatial\weather_station_placenames.csv')
    def __init__(
        self,
        download_directory_path:Path=Path(r'M:\Users\RH2\src\caiso_curtailments\weather_data'),
        weather_stations:list=[],
        years:list=[],
        log_path:Path=(r'M:\Users\RH2\src\caiso_curtailments\weather_data\download_log.csv')
    ):
        log_dtypes = {
            'effective_date' : 'datetime64[D]',
            'weather_station' : 'string',
            'download_path' : 'string',
        }
        self.download_directory_path = download_directory_path
        self.weather_stations = weather_stations
        self.years = years
        self.logger = DataLogger(dtypes=log_dtypes,log_path=log_path,delimiter=',')

    def get_url(self,weather_station_id:str,year:ts):
        '''
        Generates the url to a NCEI/NOAA hourly global surface temperature data
        file based on a given weather station id and year.

        Parameters:
            weather_station_id - a unique four-letter abbreviation for a weather
                station corresponding to a row in the
                weather_station_placenames.csv file used for identifying data
                files on the NCEI/NOAA repository.
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
        '''
        url_template = r'https://www.ncei.noaa.gov/data/global-hourly/access/{}/{}.csv'
        weather_station_placenames = pd.read_csv(self.weather_station_placenames_path)
        weather_station_placenames.set_index('StationID',inplace=True)
        if weather_station_id in weather_station_placenames.index:
            file_id = weather_station_placenames.loc[weather_station_id,'FileID']
            url = url_template.format(year.year,file_id)
        else:
            url = ''
            raise NameError(f'Weather Station {weather_station_id} Not Found')
        return url

    def get_path(self,weather_station_id:str,year:ts):
        '''
        Defines a default path for data files based on the weather station data
        and year of its contents.

        Parameters:
            weather_station_id - a unique four-letter abbreviation for a weather
                station corresponding to a row in the
                weather_station_placenames.csv file used for identifying data
                files on the NCEI/NOAA repository.
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
        '''
        filename = f'{weather_station_id}-{year.year}.csv'
        return self.download_directory_path / filename

    def download_weather_data(
        self,
        weather_station_id:int,
        year:int,
        download_path:Path,
        overwrite:bool=True
    ):
        '''
        Downloads and saves a weather data file from the NCEI/NOAA hourly global
        surface temperature database based on the input weather station and
        year. When requesting weather data for the current year, only a partial
        year of data is retrieved.

        Parameters:
            weather_station_id - a unique four-letter abbreviation for a weather
                station corresponding to a row in the
                weather_station_placenames.csv file used for identifying data
                files on the NCEI/NOAA repository.
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
            download_path - a Path object pointing to where downloaded csv file
                should be saved.
            overwrite - a boolean value indicating whether files already
                downloaded should be overwritten. Default value is True.
        '''
        url = self.get_url(weather_station_id,year)
        if (
            (year==self.logger.data.loc[:,'effective_date']) & \
            (weather_station_id==self.logger.data.loc[:,'weather_station'])
        ).any() and not overwrite:
            filename = download_path.name
            print(f'Skipping file already downloaded: {filename}')
        else:
            try:
                with download_path.open('wb') as f:
                    c = pycurl.Curl()
                    c.setopt(c.URL,url)
                    c.setopt(c.WRITEDATA,f)
                    c.perform()
                    c.close()
                    log_entry= pd.Series({
                        'effective_date' : year,
                        'weather_station' : weather_station_id,
                        'download_path' : str(download_path)
                    })
                    self.logger.log(log_entry)
                    self.logger.commit()
                    print(f'Downloaded {download_path}')
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
                print('Specified File Not Available at Given URL')

    def download_all(self,overwrite:bool=True):
        '''
        Downloads all data files for the weather stations and years specified
        in the object attributes, and saves to the default locations.
        '''
        errors = []
        for year in self.years:
            for weather_station in self.weather_stations:
                output_path = self.get_path(weather_station,year)
                try:
                    self.download_weather_data(weather_station,year,output_path,overwrite)
                except (requests.exceptions.HTTPError,requests.exceptions.ConnectionError):
                    errors += [f'{weather_station} - {year.year}']
        if len(errors)>0:
            print('Unable to retrieve data files for the following weather stations and years:\n\t' + '\n\t'.join(errors))
        print('Downloads complete!')

if __name__=='__main__':
    download_directory = Path(r'M:\Users\RH2\src\caiso_curtailments\weather_data')
    # these twelve weather stations were selected in previous analyses for their
    # proximity to the majority of CC/CT resources in the prior trade-day
    # curtailment data:
    weather_stations = [
        'KNKX','KOAK','KRDD','KRNO',
        'KSAC','KSAN','KSBA','KSCK',
        'KSFO','KSJC','KSMF','KUKI'
    ]
    weather_stations = list(pd.read_csv(Path(r'M:\Users\RH2\src\caiso_curtailments\geospatial\weather_station_placenames.csv')).loc[:,'StationID'])
    years = [ts(year,1,1) for year in range(2021,2024)]
    log_path = download_directory / r'download_log.csv'
    weather_downloader = WeatherDownloader(download_directory,weather_stations,years,log_path)
    weather_downloader.download_all(overwrite=False)