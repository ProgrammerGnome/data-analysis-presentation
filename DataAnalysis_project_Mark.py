import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# 1. CSV fájl beolvasása
csv_file_path = "missile_data.csv"  # Cseréld le a tényleges fájl elérési útra
data = pd.read_csv(csv_file_path)

# 2. Településnevek és koordináták kinyerése
facility_locations = data[
    ["Facility Location", "Facility Longitude", "Facility Latitude"]
].dropna()

# Szűrés, hogy csak érvényes számok legyenek
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

# 3. PostgreSQL kapcsolat létrehozása
db_config = {
    "user": "postgres",
    "password": "welcome",
    "host": "localhost",
    "port": "5432",
    "database": "w43",
}

# SQLAlchemy motor létrehozása
engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# 4. PostGIS bővítmény engedélyezése
try:
    # Kapcsolat a PostgreSQL adatbázishoz
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
    )
    cursor = conn.cursor()

    # PostGIS bővítmény telepítése és engedélyezése
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("PostGIS bővítmény engedélyezve.")

    # Változások mentése
    conn.commit()

except Exception as e:
    print(f"Hiba történt a PostGIS engedélyezésekor: {e}")
    conn.rollback()

finally:
    # Kapcsolat lezárása
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Kapcsolat lezárva.")

# 5. Táblák létrehozása (ha még nem léteznek)
try:
    conn = psycopg2.connect(
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
    )
    cursor = conn.cursor()

    # `facility_locations` tábla létrehozása, ha még nem létezik
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS facility_locations (
            location_name TEXT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            geom geometry(Point, 4326)
        );
    """)
    print("A facility_locations tábla létrehozva.")

    # Az adatokat közvetlenül szúrjuk be
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

    # Változások mentése
    conn.commit()
    print("Adatok beszúrva.")

except Exception as e:
    print(f"Hiba történt a tábla létrehozásakor és adatok beszúrásakor: {e}")
    conn.rollback()

finally:
    # Kapcsolat lezárása
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Kapcsolat lezárva.")
