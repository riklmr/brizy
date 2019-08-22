"""
get_stations_details_aqualim.py

This script does the actual work - download station details and store them in PostgreSQL.
"""
import pandas as pd

import brizy

# local dir to store resulting CSV files
DIR_STATIONS = '.'

# create df with all stations available on the www and save into CSV file
stations_www = brizy.get_stations_www()
stations_www.to_csv(f'{DIR_STATIONS}/stations_aqualim.csv')
stations_www = pd.read_csv(f'{DIR_STATIONS}/stations_aqualim.csv', index_col='code')


# execute the heavy scraping for all three station types and store into CSV files
stations_precipitation = brizy.scrape_stations(stations_www, "precipitation")
stations_precipitation.to_csv(f'{DIR_STATIONS}/stations_precipitation_aqualim.csv')
print("precipitation saved to csv")

stations_hauteur = brizy.scrape_stations(stations_www, "hauteur")
stations_hauteur.to_csv(f'{DIR_STATIONS}/stations_hauteur_aqualim.csv')
print("hauteur saved to csv")

stations_debit = brizy.scrape_stations(stations_www, "debit")
stations_debit.to_csv(f'{DIR_STATIONS}/stations_debit_aqualim.csv')
print("debit saved to csv")


# store dataframes from CSV to RDBMS
stations_precipitation = pd.read_csv(f'{DIR_STATIONS}/stations_precipitation_aqualim.csv', index_col='code', na_values={"Date d'installation": ''})
print("precipitation read from csv")
brizy.insert_records_station(stations_precipitation, "precipitation")
print("precipitation stored in DB")

stations_hauteur = pd.read_csv(f'{DIR_STATIONS}/stations_hauteur_aqualim.csv',index_col='code', na_values={"Date d'installation": ''})
print("hauteur read from csv")
brizy.insert_records_station(stations_hauteur, "hauteur")
print("hauteur stored in DB")

stations_debit = pd.read_csv(f'{DIR_STATIONS}/stations_debit_aqualim.csv', index_col='code', na_values={"Date d'installation": ''})
print("debit read from csv")
brizy.insert_records_station(stations_debit, "debit")
print("debit stored in DB")


# done
