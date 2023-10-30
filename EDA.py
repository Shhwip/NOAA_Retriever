import pandas as pd
from io import StringIO
import requests
import numpy as np
import matplotlib.pyplot as plt


def get_data(station_number, station_name):
    # list of dates by month in the format yyyyMMdd from 2022-08-25 to 2023-10-25
    dates = []
    for date in pd.date_range(start='2022-08-25', end='2023-10-25', freq='M'):
        dates.append(date.strftime("%Y%m"))

    print(dates)
    df = pd.DataFrame()
    dfstation = pd.DataFrame()
    for i in range(len(dates) - 1):
        print(dates[i] + '25')
        print(dates[i + 1] + '25')
        params = {
        'begin_date': dates[i] + '25',
        'end_date': dates[i + 1] + '25',
        'station': station_number,
        'product': '',
        'datum': 'STND',
        'time_zone': 'gmt',
        'units': 'english',
        'format': 'csv',
        'application': 'Weber State University CS4580 Project'
        }
        
        products = ['water_level', 'predictions', 'air_temperature', 'wind', 'air_pressure', 'water_temperature']
        for product in products:
            params['product'] = product
            response = requests.get("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?", params=params)
            if(response.status_code == 200):
                if("No data was found." in response.text):
                    print("No data for station" + str(station_number) + str(station_name))
                    print(response.text)
                    return
                print("Success for file")
                print(station_number + "_" + product + '_' + dates[i] + '25' + '.csv')
                df2 = pd.read_csv(StringIO(response.text))
                for column in df2.columns:
                    if column in [' X', ' N', ' R ', ' L', ' F', ' R']:
                        df2.drop(columns=column, inplace=True)
                if product == 'water_level':
                    df = df2
                else:
                    df = df.merge(df2, on='Date Time', how='outer')
            else:
                print("Error: " + str(response))
                print(response.text)
                return
        dfstation = pd.concat([dfstation, df])
    dfstation['Station Number'] = station_number
    dfstation['Station Name'] = station_name
    dfstation.to_csv(station_number + '.csv', index=False)

def merge(station_number, station_name):
    # Read in data
    df1 = pd.read_csv('CO-OPS__' + station_number + '__bp.csv')
    df2 = pd.read_csv('CO-OPS__' + station_number + '__tt.csv')
    df3 = pd.read_csv('CO-OPS__' + station_number + '__wl.csv')
    df5 = pd.read_csv('CO-OPS__' + station_number + '__wt.csv')
    df4 = pd.read_csv('CO-OPS__' + station_number + '__ws.csv')
    df6 = pd.read_csv('CO-OPS__' + station_number + '__pr.csv')

    # Drop empty columns
    df1 = df1.drop(columns=[' X', ' N', ' R '])
    df2 = df2.drop(columns=[' X', ' N', ' R '])
    df3 = df3.drop(columns=[' F', ' R', ' L'])
    df4 = df4.drop(columns=[' X', ' R '])
    df5 = df5.drop(columns=[' X', ' N', ' R '])

    # Merge dataframes
    df = df1.merge(df2, on='Date Time', how='outer')
    print(df.columns)
    df = df.merge(df3, on='Date Time', how='outer')
    print(df.columns)
    df = df.merge(df4, on='Date Time', how='outer')
    print(df.columns)
    df = df.merge(df5, on='Date Time', how='outer')
    print(df.columns)
    df = df.merge(df6, on='Date Time', how='outer')
    df['Station Number'] = station_number
    print(df.columns)
    print(df.head())
    # Save to csv
    df.to_csv(station_number + '.csv', index=False)

def iterateStations():
    with open("HarmonicStationNumbers.txt", 'r') as f:
        stations = f.readlines()
        for station in stations:
            get_data(station[:7], station[8:-1])

if __name__ == '__main__':

    iterateStations()
    # get_data('9455920')
    # merge('9452210')
    # merge('9455920')
    # merge('9468756')

    # df1 = pd.read_csv('9452210.csv')
    # df2 = pd.read_csv('9455920.csv')
    # df3 = pd.read_csv('9468756.csv')

    # df1['Station Name'] = 'Juneau, AK'
    # df2['Station Name'] = 'Anchorage, AK'
    # df3['Station Name'] = 'Nome, AK'

    # df = pd.concat([df1, df2, df3])
    # df.to_csv('all_stations.csv', index=False)
