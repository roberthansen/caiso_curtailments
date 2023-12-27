import pandas as pd
from pathlib import Path

def combine_csvs(input_file_paths:list,output_file_path:Path):
    df = pd.DataFrame(columns=['Weather Name','Hour','Weather Factor','Randomize Profile'])
    for input_file_path in input_file_paths:
        df = pd.concat([df,pd.read_csv(input_file_path)],axis='index',ignore_index=True)
    df.to_excel(output_file_path,sheet_name='CombinedCSVs',index=False)

if __name__=='__main__':
    input_file_paths = [Path(r'M:\Users\RH2\src\caiso_curtailments\derates\combustion_turbine-KLGB_{}.csv'.format(x)) for x in range(1997,2021)]
    output_file_path = Path(r'M:\Users\RH2\src\caiso_curtailments\derates\combustion_turbine-KLGB_allyears.xlsx')
    combine_csvs(input_file_paths,output_file_path)
    input_file_paths = [Path(r'M:\Users\RH2\src\caiso_curtailments\derates\combustion_turbine-KSAC_{}.csv'.format(x)) for x in range(1997,2021)]
    output_file_path = Path(r'M:\Users\RH2\src\caiso_curtailments\derates\combustion_turbine-KSAC_allyears.xlsx')
    combine_csvs(input_file_paths,output_file_path)