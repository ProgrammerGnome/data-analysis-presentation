import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# 1. Reading the CSV file
csv_file_path = "missile_data.csv"  # Replace with the actual file path
data = pd.read_csv(csv_file_path)

# 2. Extracting facility names and coordinates
facility_locations = data[
    ["Facility Location", "Facility Longitude", "Facility Latitude"]
].dropna()

# Filtering to ensure only valid numbers are present
facility_locations = facility_locations[
    facility_locations["Facility Longitude"]
    .apply(pd.to_numeric, errors="coerce")
    .notnull()
]
facility_locations = facility_locations[
    facility_locations["Facility Latitude"]
    .apply(pd.to_numeric, errors="coerce")
    .notnull()
]

locations_df = facility_locations.rename(
    columns={
        "Facility Location": "location_name",
        "Facility Longitude": "longitude",
        "Facility Latitude": "latitude",
    }
)

# 3. Creating a PostgreSQL connection
db_config = {
    "user": "postgres",
    "password": "welcome",
    "host": "localhost",
    "port": "5432",
    "database": "w43",
}

# Creating an SQLAlchemy engine
engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# 4. Enabling the PostGIS extension
try:
    # Connecting to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
    )
    cursor = conn.cursor()

    # Installing and enabling the PostGIS extension
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("PostGIS extension enabled.")

    # Saving changes
    conn.commit()

except Exception as e:
    print(f"Error enabling PostGIS extension: {e}")
    conn.rollback()

finally:
    # Closing the connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")

# 5. Creating tables (if they don't exist)
try:
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
    )
    cursor = conn.cursor()

    # Creating the `facility_locations` table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS facility_locations (
            location_name TEXT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            geom geometry(Point, 4326)
        );
    """)
    print("The facility_locations table has been created.")

    # Inserting data directly
    for index, row in locations_df.iterrows():
        cursor.execute(
            """
            INSERT INTO facility_locations (location_name, longitude, latitude, geom)
            VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
        """,
            (
                row["location_name"],
                row["longitude"],
                row["latitude"],
                row["longitude"],
                row["latitude"],
            ),
        )

    # Saving changes
    conn.commit()
    print("Data inserted.")

except Exception as e:
    print(f"Error creating table or inserting data: {e}")
    conn.rollback()

finally:
    # Closing the connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Connection closed.")
