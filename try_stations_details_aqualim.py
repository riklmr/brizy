import pandas as pd

import brizy

driver = brizy.start_driver()

df = brizy.get_stations_www(driver)
print(df)

station_details = brizy.retrieveMesure(driver, 'L6510')
print(station_details)
