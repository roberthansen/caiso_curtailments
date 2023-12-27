from pair_resources_to_weather_stations import *

if __name__=='__main__':
    resource_ids = [
        'CARLS1_2_CARCT1',
        'CHILLS_7_UNITA1',
        'CHWCHL_1_UNIT',
        'COGNAT_1_UNIT',
        'COLTON_6_AGUAM1',
        'COLUSA_2_PL1X3',
        'CUMMNG_6_SUNCT1',
        'ELCAJN_6_LM6K',
        'FELLOW_7_QFUNTS',
        'GATWAY_2_PL1X3',
        'GLNARM_2_UNIT_5',
        'GLNARM_7_UNIT_3',
        'GLNARM_7_UNIT_4',
        'GRIFFI_2_LSPDYN_SCE',
        'GRNLF1_1_PL1X2',
        'HUMBPP_1_UNITS3',
        'HUMBPP_6_UNITS',
        'INDIGO_1_UNIT_1',
        'INDIGO_1_UNIT_2',
        'INDIGO_1_UNIT_3',
        'KELSO_2_UNITS',
        'LAPLMA_2_UNIT_1',
        'LAPLMA_2_UNIT_2',
        'LAPLMA_2_UNIT_3',
        'LAPLMA_2_UNIT_4',
        'LARKSP_6_UNIT_1',
        'LARKSP_6_UNIT_2',
        'LODIEC_2_PL1X2',
        'MALAGA_1_PL1X2',
        'MIDSUN_1_PL1X2',
        'OGROVE_6_PL1X2',
        'PLMSSR_6_HISIER',
        'PNCHEG_2_PL1X4',
        'PNCHPP_1_PL1X2',
        'REDBLF_6_UNIT',
        'RUSCTY_2_UNITS',
        'RVSIDE_6_SPRING',
        'SEARLS_7_ARGUS',
        'SNCLRA_2_UNIT1',
        'SNCLRA_6_OXGEN',
        'STANTN_2_STAGT1',
        'STANTN_2_STAGT2',
        'VESTAL_2_UNIT1',
        'VESTAL_2_WELLHD',
        'VICTORVILLECOGEN',
        'VISTA_2_FCELL'
    ]
    resource_locations = retrieve_resource_locations(resource_ids)
    # resource_locations_alternative = retrieve_resource_locations_alternative(resource_ids)
    print(resource_locations)
    # manual overwrite locations:
    resource_locations_overwrite = pd.DataFrame({
        'ResourceID' : [
            'GRIFFI_2_LSPDYN_SCE',
            'INDIGO_1_UNIT_1',
            'INDIGO_1_UNIT_2',
            'INDIGO_1_UNIT_3',
            'LAPLMA_2_UNIT_1',
            'LAPLMA_2_UNIT_2',
            'LAPLMA_2_UNIT_3',
            'LAPLMA_2_UNIT_4',
            'LARKSP_6_UNIT_1',
            'LARKSP_6_UNIT_2',
            'STANTN_2_STAGT1',
            'STANTN_2_STAGT2',
        ],
        'ResLat' : [
            35.053198,
            33.911133,
            33.911133,
            33.911133,
            35.295078,
            35.295078,
            35.295078,
            35.295078,
            32.567210,
            32.567210,
            33.807004,
            33.807004,
        ],
        'ResLon' : [
            245.866566,
            243.447035,
            243.447035,
            243.447035,
            240.407669,
            240.407669,
            240.407669,
            240.407669,
            243.05572,
            243.05572,
            242.0154,
            242.0154,
        ]
    })
    for _,r in resource_locations_overwrite.iterrows():
        resource_locations.loc[(resource_locations.loc[:,'ResourceID']==r.loc['ResourceID']),'ResLat'] = r.loc['ResLat']
        resource_locations.loc[(resource_locations.loc[:,'ResourceID']==r.loc['ResourceID']),'ResLon'] = r.loc['ResLon']

    resource_weather_station_pairs = identify_weather_stations(resource_locations)
    # resource_weather_station_pairs_alternative = identify_weather_stations(resource_locations_alternative)
    print(resource_weather_station_pairs)
    # print(resource_weather_station_pairs_alternative)
    resource_weather_station_pairs.to_csv(r'C:\Users\RH2\Downloads\ResourceWeatherStationPairs.csv',index=False)