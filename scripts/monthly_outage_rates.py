import pandas as pd
import numpy as np
from pandas import Timestamp as ts, Timedelta as td
from pathlib import Path

from retrieve_caiso_curtailments import CurtailmentDownloader

if __name__=='__main__':
    resource_ids = [
        'ALAMIT_7_ES1',
        'BLKCRK_2_GMCBT1',
        'CALFTN_2_CFSBT1',
        'CHINO_2_APEBT1',
        'DRACKR_2_DSUBT1',
        'DRACKR_2_DSUBT2',
        'DRACKR_2_DSUBT3',
        'DSRTHV_2_DH2BT1',
        'ELCAJN_6_EB1BT1',
        'ESCNDO_6_EB1BT1',
        'ESCNDO_6_EB2BT2',
        'ESCNDO_6_EB3BT3',
        'GARLND_2_GARBT1',
        'GATEWY_2_GESBT1',
        'GOLETA_2_VALBT1',
        'JOANEC_2_STABT1',
        'KYCORA_6_KMSBT1',
        'MIRLOM_2_MLBBTA',
        'MIRLOM_2_MLBBTB',
        'MONLTH_6_BATTRY',
        'MOORPK_2_ACOBT1',
        'MRGT_6_TGEBT1',
        'PRCTVY_1_MIGBT1',
        'SANBRN_2_ESABT1',
        'SANTGO_2_MABBT1',
        'SNCLRA_2_SILBT1',
        'SNCLRA_2_VESBT1',
        'SWIFT_1_NAS',
        'VACADX_1_NAS',
        'VISTRA_5_DALBT1',
        'VISTRA_5_DALBT2',
        'VISTRA_5_DALBT3',
        'VISTRA_5_DALBT4',
        'VSTAES_6_VESBT1',
        'WSTWND_2_M89WD1',
        'WSTWND_2_M90BT1',
    ]
    effective_months = [ts(2021,x,1) for x in range(7,10)]
    curtailment_downloader = CurtailmentDownloader(
        download_directory_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\caiso_curtailment_reports'),
        log_path=Path('M:\\Users\\RH2\\src\\caiso_curtailments\\download_log.csv')
    )
    outage_rates = pd.DataFrame()
    for effective_month in effective_months:
        new_outage_rates = curtailment_downloader.calculate_monthly_outage_rates(resource_ids,effective_month)
        outage_rates = pd.concat([outage_rates,new_outage_rates],ignore_index=True)
    outage_rates.to_csv('M:\\Users\\RH2\\src\\caiso_curtailments\\results\\outage_rates_multirow_mrid.csv',index=False)