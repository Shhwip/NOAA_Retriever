import pandas as pd
import os
from io import StringIO
import requests


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

def merge():

    df = pd.DataFrame()

    for filename in os.listdir("stationData"):
        f = os.path.join("stationData", filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(f)
            df2 = pd.read_csv(f)
            df = pd.concat([df, df2])

    print(df.columns)
    print(df.head())
    # Save to csv
    df.to_csv('fullData.csv', index=False)

def iterateStations():
    with open("HarmonicStationNumbers.txt", 'r') as f:
        stations = f.readlines()
        for station in stations:
            get_data(station[:7], station[8:-1])

if __name__ == '__main__':

    #iterateStations()
    #merge()