import requests
import psycopg2
from datetime import datetime

API_KEY = 'eba97fc9e3b008c33d755bf55006e8a3'
CITY = 'Paris'
DB_PARAMS = {
    'dbname': 'weather_db',
    'user': 'weather_user',
    'password': 'weather_pass',
    'host': 'localhost',
    'port': 5432
}

def fetch_and_store():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url).json()

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            city TEXT,
            temperature FLOAT,
            humidity INT,
            description TEXT,
            timestamp TIMESTAMP
        );
    """)

    cur.execute("""
        INSERT INTO weather_raw (city, temperature, humidity, description, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        CITY,
        response['main']['temp'],
        response['main']['humidity'],
        response['weather'][0]['description'],
        datetime.utcnow()
    ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    fetch_and_store()
