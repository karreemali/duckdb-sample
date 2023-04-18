import pandas as pd
import duckdb
from datetime import datetime

# Data can be downloaded from here https://dasil.grinnell.edu/DataRepository/NIBRS/IncidentLevelCSV.zip , removed Individual_incident_2005.csv

# Run DB in Memory
conn = duckdb.connect(database=':memory:')

startTime = datetime.now()

# Import data from all csvs to crime data table in duckdb
crime_data=conn.execute("CREATE TABLE crime_data AS SELECT * FROM read_csv_auto('CSV/*.csv');")

# Sample Query
large_query = conn.execute("""
            SELECT state,incident_number, date_SIF, property_value

            FROM crime_data
            where property_value >5000
            order by property_value desc
            limit 5

            """).df()
            
# Print query results
print(large_query)
print("Loaded and Analyzed 2.96 GB of data in "+str(datetime.now() - startTime)+" seconds with DuckDb")

# Exexcutes data to csv.
# conn.execute("COPY crime_data TO 'large_query.csv' (HEADER, DELIMITER ',');")