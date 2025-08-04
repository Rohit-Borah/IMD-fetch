#Author - Rohit Borah
#Date - 02-08-2025
#Title- IMD data fetch

import requests
import psycopg2
from datetime import datetime
import getpass
import socket


# --- FETCH JSON ---
response = requests.get(JSON_URL)
response.raise_for_status()
data = response.json()
if isinstance(data, dict):
    data = [data]

# --- CONNECT TO DATABASE ---
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --- CREATE TABLE IF NOT EXISTS ---
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ID TEXT,
    CALL_SIGN TEXT,
    DISTRICT TEXT,
    STATE TEXT,
    STATION TEXT,
    DATE DATE,
    TIME TIME,
    CURR_TEMP FLOAT,
    DEW_POINT_TEMP FLOAT,
    RH FLOAT,
    WIND_DIRECTION INT,
    WIND_SPEED FLOAT,
    MSLP FLOAT,
    MIN_TEMP FLOAT,
    MAX_TEMP FLOAT,
    Latitude FLOAT,
    Longitude FLOAT,
    WEATHER_CODE TEXT,
    NEBULOSITY TEXT,
    RAINFALL_SEL TEXT,
    RAINFALL FLOAT,
    FEEL_LIKE FLOAT,
    WEATHER_ICON TEXT,
    WEATHER_MESSAGE TEXT,
    BACKGROUND TEXT,
    BACKGROUND_URL TEXT,
    PRIMARY KEY (ID, DATE, TIME)
);
"""
cur.execute(create_table_query)
conn.commit()

# --- CREATE AUDIT TABLE IF NOT EXISTS ---
create_audit_table = """
CREATE TABLE IF NOT EXISTS IMD_audit (
    run_id SERIAL PRIMARY KEY,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_inserted INT,
    records_skipped INT,
    error_messages TEXT,
    sys_user TEXT,
    host_name TEXT,
    remarks TEXT
);
"""
cur.execute(create_audit_table)
conn.commit()


# --- INSERT QUERY ---
insert_query = f"""
INSERT INTO {TABLE_NAME} (
    ID, CALL_SIGN, DISTRICT, STATE, STATION, DATE, TIME, CURR_TEMP,
    DEW_POINT_TEMP, RH, WIND_DIRECTION, WIND_SPEED, MSLP, MIN_TEMP,
    MAX_TEMP, Latitude, Longitude, WEATHER_CODE, NEBULOSITY,
    RAINFALL_SEL, RAINFALL, FEEL_LIKE, WEATHER_ICON,
    WEATHER_MESSAGE, BACKGROUND, BACKGROUND_URL
) VALUES (
    %(ID)s, %(CALL_SIGN)s, %(DISTRICT)s, %(STATE)s, %(STATION)s, %(DATE)s, %(TIME)s, %(CURR_TEMP)s,
    %(DEW_POINT_TEMP)s, %(RH)s, %(WIND_DIRECTION)s, %(WIND_SPEED)s, %(MSLP)s, %(MIN_TEMP)s,
    %(MAX_TEMP)s, %(Latitude)s, %(Longitude)s, %(WEATHER_CODE)s, %(NEBULOSITY)s,
    %(RAINFALL_SEL)s, %(RAINFALL)s, %(Feel Like)s, %(WEATHER_ICON)s,
    %(WEATHER_MESSAGE)s, %(BACKGROUND)s, %(BACKGROUND_URL)s
)
ON CONFLICT (ID,DATE,TIME) DO NOTHING;
"""

error_messages = []
system_user = getpass.getuser()
host_name = socket.gethostname()

# --- PROCESS AND INSERT RECORDS ---
inserted_count = 0
skipped_count = 0

for record in data:
    try:
        # Clean values
        record = {k: (None if v in [None, "NULL"] else v) for k, v in record.items()}
        cur.execute(insert_query, record)
        inserted_count += 1
    except Exception as e:
        # conn.rollback()
        # print(f"Skipping record ID={record.get('ID')} due to error: {e}")
        # skipped_count += 1
        conn.rollback()
        msg = f"ID={record.get('ID')} → {str(e)}"
        error_messages.append(msg)
        print(f"⚠️ Skipping record {msg}")
        skipped_count += 1


# --- FINAL COMMIT ---
conn.commit()

# --- INSERT AUDIT RECORD ---
audit_query = """
INSERT INTO IMD_audit (
    records_inserted, records_skipped, error_messages,
    sys_user, host_name, remarks
) VALUES (%s, %s, %s, %s, %s, %s);
"""

cur.execute(
    audit_query,
    (
        inserted_count,
        skipped_count,
        "\n".join(error_messages) if error_messages else None,
        system_user,
        host_name,
        'Daily IMD fetch'
    )
)
conn.commit()

# --- CLEAN UP ---
cur.close()
conn.close()

# --- LOG TIMESTAMP ---
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n✅ Inserted: {inserted_count}, Skipped: {skipped_count}")
print(f"🕒 Data inserted at: {timestamp}")

# # ---Write audit to log file ---
# with open("imd_fetch_log.txt", "a") as log_file:
#     log_file.write(f"[{datetime.now()}] Inserted: {inserted_count}, Skipped: {skipped_count}\n")
#     if error_messages:
#         for msg in error_messages:
#             log_file.write(f"    Error: {msg}\n")
#     log_file.write("\n")