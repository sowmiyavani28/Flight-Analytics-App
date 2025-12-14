import streamlit as st
import sqlite3
import pandas as pd
from db_connection import get_connection as db_get_connection

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(
    page_title="✈️ Flight Analytics Dashboard",
    page_icon="✈️",
    layout="wide"
)



# -------------------------
# HELPER FUNCTION
# -------------------------
def run_query(query, params=None):
    conn = db_get_connection()
    return pd.read_sql(query, conn, params=params)

# -------------------------
# SIDEBAR
# -------------------------
st.sidebar.title("✈️ Flight Dashboard")
page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "Flights Explorer",
        "Aircraft Analytics",
        "Airport Analytics",
        "Airline Performance",
        "Delays & Cancellations"
    ]
)

# -------------------------
# OVERVIEW
# -------------------------
if page == "Overview":
    st.title("📊 Aviation Operations Overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Flights", run_query("SELECT COUNT(*) c FROM flights")["c"][0])
    col2.metric("Total Aircraft", run_query("SELECT COUNT(*) c FROM aircraft")["c"][0])
    col3.metric("Total Airports", run_query("SELECT COUNT(*) c FROM airport")["c"][0])
    col4.metric("Airlines", run_query(
        "SELECT COUNT(DISTINCT airline_code) c FROM flights WHERE airline_code IS NOT NULL"
    )["c"][0])

    st.divider()

    st.subheader("Flight Status Distribution")
    status_df = run_query("""
        SELECT status, COUNT(*) count
        FROM flights
        GROUP BY status
    """)
    st.bar_chart(status_df.set_index("status"))

# -------------------------
# FLIGHTS EXPLORER
# -------------------------
elif page == "Flights Explorer":
    st.title("🛫 Flights Explorer")

    col1, col2, col3 = st.columns(3)

    airline = col1.selectbox(
        "Airline",
        ["All"] + run_query(
            "SELECT DISTINCT airline_code FROM flights WHERE airline_code IS NOT NULL"
        )["airline_code"].tolist()
    )

    status = col2.selectbox(
        "Status",
        ["All"] + run_query(
            "SELECT DISTINCT status FROM flights WHERE status IS NOT NULL"
        )["status"].tolist()
    )

    limit = col3.slider("Records", 10, 500, 50)

    query = """
        SELECT flight_number, origin_iata, destination_iata,
               scheduled_departure, scheduled_arrival,
               status, airline_code
        FROM flights
        WHERE 1=1
    """
    params = []

    if airline != "All":
        query += " AND airline_code = ?"
        params.append(airline)

    if status != "All":
        query += " AND status = ?"
        params.append(status)

    query += " ORDER BY scheduled_departure DESC LIMIT ?"
    params.append(limit)

    df = run_query(query, params)
    st.dataframe(df, use_container_width=True)

# -------------------------
# AIRCRAFT ANALYTICS
# -------------------------
elif page == "Aircraft Analytics":
    st.title("🛩️ Aircraft Analytics")

    st.subheader("Flights by Aircraft Model")
    model_df = run_query("""
        SELECT a.model, COUNT(f.flight_id) total_flights
        FROM flights f
        JOIN aircraft a ON f.aircraft_registration = a.registration
        WHERE a.model IS NOT NULL
        GROUP BY a.model
        ORDER BY total_flights DESC
    """)
    st.bar_chart(model_df.set_index("model"))

    st.subheader("Most Used Aircraft")
    st.dataframe(model_df.head(10), use_container_width=True)

# -------------------------
# AIRPORT ANALYTICS
# -------------------------
elif page == "Airport Analytics":
    st.title("🏢 Airport Analytics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Departure Airports")
        dep_df = run_query("""
            SELECT ap.name, COUNT(*) departures
            FROM flights f
            JOIN airport ap ON f.origin_iata = ap.iata_code
            GROUP BY ap.name
            ORDER BY departures DESC
            LIMIT 10
        """)
        st.bar_chart(dep_df.set_index("name"))

    with col2:
        st.subheader("Top Arrival Airports")
        arr_df = run_query("""
            SELECT ap.name, COUNT(*) arrivals
            FROM flights f
            JOIN airport ap ON f.destination_iata = ap.iata_code
            GROUP BY ap.name
            ORDER BY arrivals DESC
            LIMIT 10
        """)
        st.bar_chart(arr_df.set_index("name"))

# -------------------------
# AIRLINE PERFORMANCE
# -------------------------
elif page == "Airline Performance":
    st.title("🏷️ Airline Performance")

    perf_df = run_query("""
        SELECT 
            airline_code,
            SUM(CASE WHEN status IN ('Departed','Arrived') THEN 1 ELSE 0 END) AS on_time,
            SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) AS delayed,
            SUM(CASE WHEN status IN ('Canceled','CanceledUncertain') THEN 1 ELSE 0 END) AS cancelled
        FROM flights
        WHERE airline_code IS NOT NULL
        GROUP BY airline_code
        ORDER BY airline_code
    """)

    st.dataframe(perf_df, use_container_width=True)

# -------------------------
# DELAYS & CANCELLATIONS
# -------------------------
elif page == "Delays & Cancellations":
    st.title("⏱️ Delays & Cancellations")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Airport Delay Summary")
        delay_df = run_query("""
            SELECT airport_iata, delay_date, delayed_flights, canceled_flights
            FROM airport_delays
            ORDER BY delay_date DESC
            LIMIT 20
        """)
        st.dataframe(delay_df, use_container_width=True)

    with col2:
        st.subheader("Cancelled Flights")
        cancelled_df = run_query("""
            SELECT flight_number, origin_iata, destination_iata, scheduled_departure
            FROM flights
            WHERE status IN ('Canceled','CanceledUncertain')
            ORDER BY scheduled_departure DESC
            LIMIT 20
        """)
        st.dataframe(cancelled_df, use_container_width=True)
