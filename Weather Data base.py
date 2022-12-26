#Purpose: Generate database to weather forecast for the following months
#Name: Oscar Sanchez
#Date: 12/25/2022

from noaa_sdk import noaa
import sqlite3
import datetime

# parameters for retrieving NOAA weather forecast data
zipCode = '91792' # zip code for the weather forecast
country = 'US' 
#date-time format is yyyy-mm-ddThh:mm:ssZ, timestamps are in Zulu format (GMT time)
#returns the most recent 30 days worth of data
today = datetime.datetime.now()
past = today - datetime.timedelta(days=30)
startDate = past.strftime("%Y-%m-%dT00:00:00Z")
endDate = today.strftime("%Y-%m-%dT23:59:59Z")

#creating the connection to the weather forecast databse - will create a database if once does not exist
print("Preparing the weather forecast database...")
dbFile = "weatherforecast.db"
conn =  sqlite3.connect(dbFile)
#creating the cursor to execute SQL commands - (Cursors allow for manipulation of records in a database)
cur = conn.cursor()

#drop previous version of the table so restart with a fresh table every time
dropTableCmd = "DROP TABLE IF EXISTS observations;"
cur.execute(dropTableCmd)

#create new table to store Weather Forecast Observations
createTableCmd = """CREATE TABLE IF NOT EXISTS observations (
                        timestamp TEXT NOT NULL PRIMARY KEY,
                        windSpeed REAL,
                        temperature REAL,
                        relativeHumidity REAL,
                        windDirection INTEGER,
                        barometricPressure INTEGER,
                        visibility INTEGER,
                        textDescription TEXT
                        ) ; """
cur.execute(createTableCmd)
print("Databse prepared")

#retrieve hourly weather observations from NOAA Weather Service API
print("Retrieving weather data...")
n = noaa.NOAA()
observations = n.get_observations(zipCode, country, startDate, endDate)

#generate table with weather observations
print("Inserting rows...")
insertCmd = """INSERT INTO observations
                    (timestamp, windSpeed, temperature, relativeHumidity,
                    windDirection, barometricPressure, visibility, textDescription)
                Values
                    (?, ?, ?, ?, ?, ?, ?, ?) """
count = 0
for obs in observations:
    insertValues = (obs["timestamp"],
                    obs["windSpeed"]["value"],
                    obs["temperature"]["value"],
                    obs["relativeHumidity"]["value"],
                    obs["windDirection"]["value"],
                    obs["barometricPressure"]["value"],
                    obs["visibility"]["value"],
                    obs["textDescription"])
    cur.execute(insertCmd, insertValues)
    count += 1
if count > 0:
    cur.execute("COMMIT;")
print(count, "rows inserted")
print("Databse upload complete!")
                    

