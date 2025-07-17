SELECT
  city,
  temperature,
  humidity,
  description,
  timestamp::date AS date
FROM weather_raw
