# Flight-Analytics-Airport-tracker
import requests

# Fix: Replace {codeType} with a valid type, e.g., 'iata'
url = "https://aerodatabox.p.rapidapi.com/airports/iata/DEL"

headers = {
	"x-rapidapi-key": "1a08aa05bamshfd016d2ed153ddcp1fa188jsn96526e10b88f",
	"x-rapidapi-host": "aerodatabox.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

# Add a check for the status code before attempting to parse JSON
if response.status_code == 200:
    print(response.json())
else:
    print(f"Error: {response.status_code} - {response.text}")