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
import os
from typing import List
import numpy as np

config = configparser.ConfigParser()
config.read('config.ini')

url = config['WEATHER']['URL']


def get_weather(pnu:int, lon: float, lat: float) \
        -> List[pd.DataFrame]:
    """
    :param lat: Latitude
    :param lon: Longitude
    :return: Weather Dataframe
    """
    #
    # Convert Coord from WGS84(LatLong) to UTM-K
    #
    proj_UTMK = Proj(init='epsg:5178')  # UTM-K(Bassel), 도로명주소
    proj_WGS84 = Proj(init='epsg:4326')  # WGS84, Long-Lat

    x, y = transform(proj_WGS84, proj_UTMK, lon, lat)

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
        sleep(1)
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
                print(content)
                continue

            body = [{key: item[key] for key in keys_to_select}
                    for item in json_ob['response']['body']['items']['item']]

            df = pd.json_normalize(body)

            #
            # Interpolation
            #
            """
            1. Tmax =< Tmin (ttp150, ltp150)
            2. SRAD == 0.0 (slq)
            """
            # df['slq']
            cols = ['slq', 'ttp150', 'ltp150']
            for col in cols:
                df[col] = df[col].map(lambda x: x if x != 0.0 else np.NAN)
            df['slq'] = df['slq'].fillna(28)
            try:
                df = df.interpolate(method='values')
            except TypeError:
                print(df)

            temp_dfs.append(df)
        df_per_year.append(
            pd.concat(temp_dfs).reset_index(drop=True)
        )
        temp_dfs = []

    return df_per_year


def write_wth(weather_df: List[pd.DataFrame], alias: str, lat: float, lon: float, weather_filebase: str) \
        -> bool:
    """
    :param weather_filebase:
    :param lon: longitude
    :param lat: latitude
    :param weather_df: Dataframe of Weather data
    :param alias: Code to make weather file
    :return:
    """
    # Convert Dataframe to WTH File

    for df_yearly in weather_df:
        year = df_yearly.loc[0]['obsrTm'][2:4]
        filename = alias.upper() + str(year) + '01.WTH'

        metadata = """
*WEATHER DATA : ISU Agronomy Farm

@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT
{INSI}   {LAT}   {LONG}   {ELEV}   {TAV} {AMP}   {REFHT}   {WNDHT}
@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND""".format(
            INSI=alias.upper(),
            LAT=lat,
            LONG=lon,
            ELEV=-99,
            TAV=-99,
            AMP=-99,
            REFHT=-99,
            WNDHT=-99
        )
        with open(os.path.join(os.getcwd(), weather_filebase, filename), 'w') as wth:
            wth.write(metadata)
            for doy, sample in df_yearly.iterrows():
                variables = [
                    sample['slq'], sample['ttp150'], sample['ltp150'], sample['afp']
                ]
                try:
                    variables = [str(round(x, 1)) if not isinstance(x, int) else str(round(float(x), 1)) for x in variables]
                except TypeError:
                    variables = [str(round(float(x), 1)) if x else '00.0' for x in variables]
                result = []
                minus_counter = False
                for var in variables:
                    if len(var) == 4:
                        result.append(var)
                    elif len(var) == 3:
                        result.append(' '+var)
                    elif len(var) == 5:
                        result.append(var)
                        minus_counter = True
                    else:
                        result.append(var)
                print(result)
                line_format = "{DATE}  {SRAD}  {TMAX}  {TMIN}  {RAIN}  {DEWP}  {WIND}" if not minus_counter\
                    else "{DATE}  {SRAD}  {TMAX} {TMIN}  {RAIN}  {DEWP}  {WIND}"
                line = line_format.format(
                    DATE=str(year) + str(doy).zfill(3),
                    SRAD=result[0],
                    TMAX=result[1],
                    TMIN=result[2],
                    RAIN=result[3],
                    DEWP=-99,
                    WIND=-99
                )
                wth.write(
                    line
                )
                wth.write('\n')

    return True


if __name__ == '__main__':
    df_per_year = get_weather(lon=float(sys.argv[1]),
                              lat=float(sys.argv[2]),
                              pnu=int(sys.argv[3]))
    write_wth(df_per_year,
              alias='JEXU',
              lon=float(sys.argv[1]),
              lat=float(sys.argv[2]),
              weather_filebase=''
              )