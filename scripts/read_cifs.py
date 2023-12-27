import pyarrow.parquet as pq
from pathlib import Path

def read_cif(path:Path):
    md = pq.read_metadata(path)
    cif = pq.read_table(path).to_pandas()
    f = lambda s: s.split('_')[-1]
    if 'CallSign' in cif.columns:
        station_column = 'CallSign'
    elif 'StationLongName' in cif.columns:
        station_column = 'StationLongName'
    else:
        station_column = cif.columns[0]
    cif.loc[:,'StationID'] = cif.loc[:,station_column].map(f)
    return md,cif

if __name__=='__main__':
    p = Path(r'M:\Users\RH2\src\caiso_curtailments\climate_informed_weather_data\cif_temperature_15_25.parquet')
    md,cif = read_cif(p)
    print(cif)