import requests
import os, time
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

# -------------------
# HELPER FUNCTIONS
# -------------------

def safe_str(value):
    if isinstance(value, (dict, list)):
        return str(value)
    return value

# # -------------------
# # API FUNCTIONS
# # -------------------

def fetch_airport(iata):
    url = f"https://{API_HOST}/airports/iata/{iata}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def fetch_flights(iata):
    """
    Correct AeroDataBox endpoint (relative time window)
    """
    url = f"https://{API_HOST}/flights/airports/iata/{iata}"

    params = {
        "offsetMinutes": "-120",     # start 2 hours ago
        "durationMinutes": "720",    # 12-hour window
        "direction": "Both",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
        "withLeg": "true",
        "withLocation": "false"
    }

    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def fetch_flights_for_date(iata: str, date_str: str, code_type: str = "iata"):
    """
    Fetch flights for a selected date using absolute time windows.
    Splits the day into two calls:
      - 00:00 → 12:00
      - 12:00 → 24:00

    Returns arrivals and departures separately.
    """

    API_HOST = "aerodatabox.p.rapidapi.com"
    BASE_URL = f"https://{API_HOST}/flights/airports/{code_type}/{iata}"

    PARAMS = {
        "withLeg": "true",
        "direction": "Both",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
        "withLocation": "true",
    }

    airport_tz = pytz.timezone("Asia/Kolkata")

    start_day = airport_tz.localize(
        datetime.strptime(date_str, "%Y-%m-%d")
    )

    windows = [
        (start_day, start_day + timedelta(hours=12)),
        (start_day + timedelta(hours=12), start_day + timedelta(days=1)),
    ]

    all_departures = []
    all_arrivals = []

    for start, end in windows:
        start_ts = start.strftime("%Y-%m-%dT%H:%M")
        end_ts = end.strftime("%Y-%m-%dT%H:%M")

        url = f"{BASE_URL}/{start_ts}/{end_ts}"
        print(f"Calling API: {url}")

        response = requests.get(url, headers=HEADERS, params=PARAMS)
        response.raise_for_status()

        data = response.json()

        departures = data.get("departures", [])
        arrivals = data.get("arrivals", [])

        all_departures.extend(departures)
        all_arrivals.extend(arrivals)

        print(
            f"Fetched {len(departures)} departures, "
            f"{len(arrivals)} arrivals"
        )

    return {
        "iata": iata,
        "date": date_str,
        "departureCount": len(all_departures),
        "arrivalCount": len(all_arrivals),
        "departures": all_departures,
        "arrivals": all_arrivals,
    }
# def fetch_aircraft(icao24):
#     url = f"https://{API_HOST}/aircrafts/icao24/{icao24}"
#     r = requests.get(url, headers=HEADERS)
#     if r.status_code != 200:
#         return None
#     return r.json()

# def fetch_delays(iata):
#     url = f"https://{API_HOST}/airports/iata/{iata}/statistics/delays"
#     r = requests.get(url, headers=HEADERS)
#     if r.status_code != 200:
#         return None
#     return r.json()

# # -------------------
# # DATABASE INSERTS
# # -------------------

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

# def insert_aircraft(conn, data):
#     if not data:
#         return

#     cur = conn.cursor()

#     cur.execute("""
#         INSERT OR IGNORE INTO aircraft
#         (registration, model, manufacturer, icao_type_code, owner)
#         VALUES (?, ?, ?, ?, ?)
#     """, (
#         safe_str(data.get("registration")),
#         safe_str(data.get("model")),
#         safe_str(data.get("manufacturer")),
#         safe_str(data.get("icaoType")),
#         safe_str(data.get("owner"))
#     ))

#     conn.commit()

# def insert_delays(conn, iata, data):
#     if not data:
#         return

#     cur = conn.cursor()

#     cur.execute("""
#         INSERT INTO airport_delays
#         (airport_iata, delay_date, total_flights,
#          delayed_flights, avg_delay_min,
#          median_delay_min, canceled_flights)
#         VALUES (?, ?, ?, ?, ?, ?, ?)
#     """, (
#         iata,
#         safe_str(data.get("date")),
#         data.get("totalFlights"),
#         data.get("delayedFlights"),
#         data.get("avgDelay"),
#         data.get("medianDelay"),
#         data.get("cancelledFlights")
#     ))

#     conn.commit()

# # -------------------
# # MAIN ETL PIPELINE
# # -------------------

def run_etl():
    conn = get_connection()
    aircraft_set = set()

    # Create tables
    with open("schema.sql", "r") as f:
        conn.executescript(f.read())

    print("Fetching recent flights using relative time window")

    for iata in AIRPORTS:
        print(f"\n===== Processing {iata} =====")
        try:
            airport_data = fetch_airport(iata)
            print(airport_data)
            insert_airport(conn, airport_data)

            time.sleep(2)  # To respect API rate limits

            flight_data = fetch_flights_for_date(iata, "2025-12-13")
            arrivals = flight_data.get("arrivals", [])
            departures = flight_data.get("departures", [])

            print(f"Total arrivals: {len(arrivals)}")
            print(f"Total departures: {len(departures)}")

            for f in flight_data["arrivals"]:
                insert_flight(conn, f, iata)

            for f in flight_data["departures"]:
                insert_flight(conn, f, iata)

#             delay_data = fetch_delays(iata)
#             insert_delays(conn, iata, delay_data)

        except Exception as e:
            print(f"Error processing {iata}: {e}")

        break

#     print("\nFetching Aircraft Details...")
#     for icao in aircraft_set:
#         try:
#             aircraft = fetch_aircraft(icao)
#             insert_aircraft(conn, aircraft)
#         except:
#             pass

#     conn.close()
#     print("\n🎉 ETL Completed Successfully!")

# -------------------
# ENTRY POINT
# -------------------

if __name__ == "__main__":
    run_etl()
