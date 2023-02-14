# Junseo Kang invalidid56@saefarm.com
# get Ag-weather data from Open API from EPIS, return as Dataframe
# get_weather(lat, long)
import sys

import requests
import pandas as pd
import json
import configparser
from pyproj import Proj, transform
from time import sleep

config = configparser.ConfigParser()
config.read('config.ini')

url = config['WEATHER']['URL']


def get_weather(pnu, code='SFKR'):
    """
    :param lat:
    :param lon:
    :return:
    """

    #
    # For Each Month/Year, Read Weather Data and Append to Dataframe
    #
    days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    years_to_search = [2020, 2019, 2018, 2017]
    keys_to_select = ['obsrTm',
                      'ttp150',
                      'ltp150',
                      'slq',
                      'afp']

    temp_dfs = []
    df_per_year = []

    for year in years_to_search:
        for month, day in enumerate(days_per_month):
            month_to_read = str(year) + str(month + 1).zfill(2)
            params = {
                'serviceKey': config['WEATHER']['KEY'],
                'numOfRows': day,
                'pageNo': 1,
                'type': 'json',
                'pnuCode': pnu,
                'month': month_to_read,
                'yearCount': 1
            }

            content = requests.get(url,
                                   params=params).text

            try:
                json_ob = json.loads(content)
            except json.decoder.JSONDecodeError:
                continue

            body = [{key: item[key] for key in keys_to_select}
                    for item in json_ob['response']['body']['items']['item']]

            df = pd.json_normalize(body)
            temp_dfs.append(df)
            print(month_to_read)
        df_per_year.append(
            pd.concat(temp_dfs).reset_index(drop=True)
        )
        temp_dfs = []

    # Convert Dataframe to WTH File

    for df_yearly in df_per_year:
        year = df_yearly.loc[0]['obsrTm'][2:4]
        print(df_yearly.loc[0:3])
        filename = code.upper() + str(year) + '01.WTH'

        metadata = """
*WEATHER DATA : ISU Agronomy Farm

@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT
{INSI}   {LAT}   {LONG}   {ELEV}   {TAV} {AMP}   {REFHT}   {WNDHT}
@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND
""".format(
            INSI=code.upper(),
            LAT=0,
            LONG=0,
            ELEV=-99,
            TAV=-99,
            AMP=-99,
            REFHT=-99,
            WNDHT=-99
        )
        with open(filename, 'w') as wth:
            wth.write(metadata)
            for doy, sample in df_yearly.iterrows():
                line = "{DATE} {SRAD} {TMAX} {TMIN} {RAIN} {DEWP} {WIND}".format(
                    DATE=str(year)+str(doy).zfill(3),
                    SRAD=sample['slq'],
                    TMAX=sample['ttp150'],
                    TMIN=sample['ltp150'],
                    RAIN=sample['afp'],
                    DEWP=-99,
                    WIND=-99
                )
                wth.write(
                    line
                )
                wth.write('\n')

    return True


if __name__ == '__main__':
    get_weather(int(sys.argv[1]))