import requests
import os, time
import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection
from datetime import datetime, timedelta, timezone
import pytz

# -------------------
# LOAD CONFIG
# -------------------

load_dotenv()

API_KEY = os.getenv("RAPID_API_KEY")
API_HOST = os.getenv("API_HOST")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

AIRPORTS = [
    "DEL", "BOM", "BLR", "HYD", "JFK", "LAX", "DXB",
    "SIN", "LHR", "CDG", "CCU", "PNQ", "GOI", "MAA", "MYQ"
]

CONN = get_connection()
aircraft_set = set()

# Create tables
with open("schema.sql", "r") as f:
    CONN.executescript(f.read())

# -------------------
# HELPER FUNCTIONS
# -------------------

def safe_str(value):
    if isinstance(value, (dict, list)):
        return str(value)
    return value

def fetch_airport(iata):
    url = f"https://{API_HOST}/airports/iata/{iata}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def insert_airport(conn, data):
    cur = conn.cursor()

    country = data.get("country")
    if isinstance(country, dict):
        country = country.get("name")

    continent = data.get("continent")
    if isinstance(continent, dict):
        continent = continent.get("name")

    cur.execute("""
        INSERT OR IGNORE INTO airport
        (icao_code, iata_code, name, city, country, continent,
         latitude, longitude, timezone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        safe_str(data.get("icao")),
        safe_str(data.get("iata")),
        safe_str(data.get("fullName") or data.get("shortName")),
        safe_str(data.get("municipalityName")),
        safe_str(country),
        safe_str(continent),
        data.get("location", {}).get("lat"),
        data.get("location", {}).get("lon"),
        safe_str(data.get("timeZone"))
    ))

    conn.commit()


def fetch_flights(iata: str, date_str: str):
    """
    Fetch ALL flights for a full calendar day (IST)
    using two API calls:
      - 00:00 → 12:00
      - 12:01 → 23:59

    Returns a clean, packed structure:
    {
        "iata": "DEL",
        "date": "2024-12-14",
        "timezone": "Asia/Kolkata",
        "arrivals": [...],
        "departures": [...],
        "meta": {...}
    }
    """

    tz = pytz.timezone("Asia/Kolkata")

    day_start = tz.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    mid_day = day_start + timedelta(hours=12)
    day_end = day_start + timedelta(hours=23, minutes=59)

    ranges = [
        (day_start, mid_day),
        (mid_day + timedelta(minutes=1), day_end)
    ]

    arrivals = []
    departures = []

    # Used for deduplication
    seen_arrivals = set()
    seen_departures = set()

    querystring = {
        "withLeg": "true",
        "direction": "Both",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
        "withLocation": "true"
    }

    for start, end in ranges:
        from_ts = start.strftime("%Y-%m-%dT%H:%M")
        to_ts = end.strftime("%Y-%m-%dT%H:%M")

        url = f"https://{API_HOST}/flights/airports/iata/{iata}/{from_ts}/{to_ts}"

        print(f"➡️ Fetching flights: {from_ts} → {to_ts}")

        r = requests.get(url, headers=HEADERS, params=querystring)
        r.raise_for_status()

        data = r.json()

        # ---- Pack arrivals ----
        for flight in data.get("arrivals", []):
            fid = flight.get("flight", {}).get("iataNumber") or id(flight)
            if fid not in seen_arrivals:
                arrivals.append(flight)
                seen_arrivals.add(fid)

        # ---- Pack departures ----
        for flight in data.get("departures", []):
            fid = flight.get("flight", {}).get("iataNumber") or id(flight)
            if fid not in seen_departures:
                departures.append(flight)
                seen_departures.add(fid)

    return {
        "iata": iata,
        "date": date_str,
        "timezone": "Asia/Kolkata",
        "arrivals": arrivals,
        "departures": departures,
        "meta": {
            "arrival_count": len(arrivals),
            "departure_count": len(departures),
            "windows_used": len(ranges)
        }
    }

def insert_flight(conn, f, iata):
    cur = conn.cursor()

    dep = f.get("departure", {})
    arr = f.get("arrival", {})
    aircraft = f.get("aircraft", {})

    cur.execute("""
        INSERT OR IGNORE INTO flights
        (flight_id, flight_number, aircraft_registration,
         origin_iata, destination_iata,
         scheduled_departure, actual_departure,
         scheduled_arrival, actual_arrival,
         status, airline_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        safe_str(f.get("number")),
        safe_str(f.get("number")),
        safe_str(aircraft.get("reg")),
        safe_str(dep.get("airport", {}).get("iata") or iata),
        safe_str(arr.get("airport", {}).get("iata") or iata),
        safe_str(dep.get("scheduledTime", {}).get("utc", None)),
        safe_str(dep.get("actualTimeLocal")),
        safe_str(arr.get("scheduledTime", {}).get("utc", None)),
        safe_str(arr.get("actualTimeLocal")),
        safe_str(f.get("status")),
        safe_str(f.get("airline", {}).get("iata"))
    ))

    conn.commit()

def batch_insert_flights(conn, df: pd.DataFrame):
    if df.empty:
        print("⚠️ No flights to insert")
        return

    cur = conn.cursor()

    records = df[[
        "flight_id",
        "flight_number",
        "aircraft_registration",
        "origin_iata",
        "destination_iata",
        "scheduled_departure",
        "actual_departure",
        "scheduled_arrival",
        "actual_arrival",
        "status",
        "airline_code"
    ]].values.tolist()

    cur.executemany("""
        INSERT OR IGNORE INTO flights (
            flight_id,
            flight_number,
            aircraft_registration,
            origin_iata,
            destination_iata,
            scheduled_departure,
            actual_departure,
            scheduled_arrival,
            actual_arrival,
            status,
            airline_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)

    conn.commit()
    print(f"✅ Inserted {cur.rowcount} flights")


def flights_to_dataframe(flights: dict, iata: str) -> pd.DataFrame:
    rows = []

    # -------- DEPARTURES --------
    for f in flights.get("departures", []):
        airline_code = f.get("airline", {}).get("iata")
        flight_number = f.get("number")

        scheduled_dep = f.get("departure", {}).get("scheduledTime", {}).get("utc")
        actual_dep = f.get("departure", {}).get("revisedTime", {}).get("utc") or scheduled_dep

        scheduled_arr = f.get("arrival", {}).get("scheduledTime", {}).get("utc")
        actual_arr = f.get("arrival", {}).get("revisedTime", {}).get("utc") or scheduled_arr

        origin = f.get("departure", {}).get("airport", {}).get("iata") or iata
        destination = f.get("arrival", {}).get("airport", {}).get("iata")

        flight_id = f"{airline_code}_{flight_number}_{scheduled_dep}"

        rows.append({
            "flight_id": flight_id,
            "flight_number": flight_number,
            "aircraft_registration": f.get("aircraft", {}).get("reg"),
            "origin_iata": origin,
            "destination_iata": destination,
            "scheduled_departure": scheduled_dep,
            "actual_departure": actual_dep,
            "scheduled_arrival": scheduled_arr,
            "actual_arrival": actual_arr,
            "status": f.get("status"),
            "airline_code": airline_code
        })

    # -------- ARRIVALS --------
    for f in flights.get("arrivals", []):
        airline_code = f.get("airline", {}).get("iata")
        flight_number = f.get("number")

        scheduled_arr = f.get("arrival", {}).get("scheduledTime", {}).get("utc")
        actual_arr = f.get("arrival", {}).get("revisedTime", {}).get("utc") or scheduled_arr

        scheduled_dep = f.get("departure", {}).get("scheduledTime", {}).get("utc")
        actual_dep = f.get("departure", {}).get("revisedTime", {}).get("utc") or scheduled_dep

        origin = f.get("departure", {}).get("airport", {}).get("iata")
        destination = f.get("arrival", {}).get("airport", {}).get("iata") or iata

        flight_id = f"{airline_code}_{flight_number}_{scheduled_arr}"

        rows.append({
            "flight_id": flight_id,
            "flight_number": flight_number,
            "aircraft_registration": f.get("aircraft", {}).get("reg"),
            "origin_iata": origin,
            "destination_iata": destination,
            "scheduled_departure": scheduled_dep,
            "actual_departure": actual_dep,
            "scheduled_arrival": scheduled_arr,
            "actual_arrival": actual_arr,
            "status": f.get("status"),
            "airline_code": airline_code
        })

    df = pd.DataFrame(rows)

    # Remove duplicates (same flight seen in arrival + departure)
    df.drop_duplicates(subset=["flight_id"], inplace=True)

    return df

def run_etl(date_str):
    for iata in AIRPORTS:
        print(f"\n===== Processing {iata} =====")

        try:
            # Airport
            airport_data = fetch_airport(iata)
            insert_airport(CONN, airport_data)

            time.sleep(2)

            # Flights
            flights = fetch_flights(iata, date_str)
            flights_df = flights_to_dataframe(flights, iata)

            print(f"📊 {len(flights_df)} flights collected for {iata}")

            batch_insert_flights(CONN, flights_df)

            filtered_df = flights_df[
                (flights_df["origin_iata"].str.upper() == iata) &
                (
                    (
                        flights_df["scheduled_departure"].notna() &
                        flights_df["actual_departure"].notna()
                    )
                )
            ]

            filtered_df["scheduled_departure"] = pd.to_datetime(
                filtered_df["scheduled_departure"], errors="coerce"
            )
            filtered_df["actual_departure"] = pd.to_datetime(
                filtered_df["actual_departure"], errors="coerce"
            )


            filtered_df["departure_delay_min"] = (
                filtered_df["actual_departure"] -
                filtered_df["scheduled_departure"]
            ).dt.total_seconds() / 60

            filtered_df["departure_delay_min"] = filtered_df["departure_delay_min"].clip(lower=0)

            filtered_df = flights_df[
                (flights_df["destination_iata"].str.upper() == "DEL") &
                (
                    (
                        flights_df["scheduled_arrival"].notna() &
                        flights_df["actual_arrival"].notna()
                    )
                )
            ]


            filtered_df["scheduled_arrival"] = pd.to_datetime(
                filtered_df["scheduled_arrival"], errors="coerce"
            )
            filtered_df["actual_arrival"] = pd.to_datetime(
                filtered_df["actual_arrival"], errors="coerce"
            )


            filtered_df["arrival_delay_min"] = (
                filtered_df["actual_arrival"] -
                filtered_df["scheduled_arrival"]
            ).dt.total_seconds() / 60

            filtered_df["arrival_delay_min"] = filtered_df["arrival_delay_min"].clip(lower=0)



        except Exception as e:
            print(f"❌ Error processing {iata}: {e}")

        # remove this break when ready for all airports
        break


# -------------------
# ENTRY POINT
# -------------------

if __name__ == "__main__":
    run_etl("2024-12-14")
