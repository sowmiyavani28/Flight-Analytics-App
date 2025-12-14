import sqlite3


conn = sqlite3.connect("airtracker.db")
cur = conn.cursor()

cur.execute("SELECT * FROM airport LIMIT 5")
rows = cur.fetchall()
for r in rows:
    print(r)

conn.close()