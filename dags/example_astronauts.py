"""
## Astronaut ETL example DAG with Weather Data Correlation and Spacecraft Information

This DAG queries the list of astronauts currently in space from the
Open Notify API and enriches the data with detailed spacecraft information
including models, operating agencies, and countries. It also fetches weather
data and analyzes the correlation between the number of astronauts in space
and weather conditions.

There are multiple tasks:
1. Get astronaut data from the API
2. Enrich astronaut data with spacecraft models, agencies, and countries
3. Display detailed astronaut and spacecraft information
4. Get weather data from Open-Meteo API (ISS location)
5. Combine enriched astronaut data with weather data
6. Perform correlation analysis

All tasks are written in Python using Airflow's TaskFlow API, which allows
you to easily turn Python functions into Airflow tasks, and automatically
infer dependencies and pass data.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import pandas as pd


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Dataset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        r = requests.get("http://api.open-notify.org/astros.json")
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def enrich_spacecraft_data(astronauts: list[dict]) -> list[dict]:
        """
        Enriches astronaut data with spacecraft models, operating agencies,
        and countries. Maps spacecraft names to their detailed information.
        """
        # Spacecraft information mapping
        spacecraft_info = {
            "ISS": {
                "model": "International Space Station",
                "type": "Space Station",
                "agencies": ["NASA", "Roscosmos", "ESA", "JAXA", "CSA"],
                "countries": ["USA", "Russia", "Europe", "Japan", "Canada"],
                "launch_year": 1998,
                "crew_capacity": 7,
            },
            "Tiangong": {
                "model": "Tiangong Space Station (CSS)",
                "type": "Space Station",
                "agencies": ["CNSA"],
                "countries": ["China"],
                "launch_year": 2021,
                "crew_capacity": 6,
            },
            "Shenzhou": {
                "model": "Shenzhou Spacecraft",
                "type": "Crew Vehicle",
                "agencies": ["CNSA"],
                "countries": ["China"],
                "launch_year": 1999,
                "crew_capacity": 3,
            },
        }

        enriched_astronauts = []
        for astronaut in astronauts:
            craft_name = astronaut["craft"]
            enriched = astronaut.copy()

            # Try to match spacecraft info
            spacecraft = spacecraft_info.get(
                craft_name,
                {
                    "model": craft_name,
                    "type": "Unknown",
                    "agencies": ["Unknown"],
                    "countries": ["Unknown"],
                    "launch_year": None,
                    "crew_capacity": None,
                },
            )

            enriched.update(
                {
                    "spacecraft_model": spacecraft["model"],
                    "spacecraft_type": spacecraft["type"],
                    "operating_agencies": spacecraft["agencies"],
                    "operating_countries": spacecraft["countries"],
                    "launch_year": spacecraft["launch_year"],
                    "crew_capacity": spacecraft["crew_capacity"],
                }
            )

            enriched_astronauts.append(enriched)

        print(
            f"Enriched {len(enriched_astronauts)} astronaut records with spacecraft data"
        )
        return enriched_astronauts

    @task(outlets=[Dataset("weather_data")])
    def get_weather_data(**context) -> dict:
        """
        This task fetches current weather data from Open-Meteo API.
        Uses ISS approximate location (latitude 0, longitude 0 as example).
        Returns weather metrics including temperature, wind speed, and cloud cover.
        """
        # Using Open-Meteo free API (no authentication required)
        # ISS orbits Earth, so using a general location for demonstration
        lat, lon = 0, 0  # Equator example location

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,wind_speed_10m,cloud_cover",
            "timezone": "UTC",
        }

        r = requests.get(url, params=params)
        weather_data = r.json()

        current_weather = {
            "temperature": weather_data["current"]["temperature_2m"],
            "wind_speed": weather_data["current"]["wind_speed_10m"],
            "cloud_cover": weather_data["current"]["cloud_cover"],
            "timestamp": weather_data["current"]["time"],
        }

        context["ti"].xcom_push(key="weather_data", value=current_weather)
        return current_weather

    @task
    def display_enriched_astronaut_data(enriched_astronauts: list[dict]) -> None:
        """
        Displays detailed information about each astronaut including
        spacecraft models, operating agencies, and countries.
        """
        print("\n" + "=" * 80)
        print("ASTRONAUTS CURRENTLY IN SPACE - DETAILED REPORT")
        print("=" * 80 + "\n")

        for idx, person in enumerate(enriched_astronauts, 1):
            name = person["name"]
            craft = person["craft"]
            spacecraft_model = person.get("spacecraft_model", craft)
            spacecraft_type = person.get("spacecraft_type", "Unknown")
            agencies = person.get("operating_agencies", [])
            countries = person.get("operating_countries", [])
            launch_year = person.get("launch_year")
            crew_capacity = person.get("crew_capacity")

            print(f"[{idx}] {name}")
            print(f"    Spacecraft: {craft}")
            print(f"    Model: {spacecraft_model}")
            print(f"    Type: {spacecraft_type}")
            print(f"    Operating Agencies: {', '.join(agencies)}")
            print(f"    Operating Countries: {', '.join(countries)}")
            if launch_year:
                print(f"    Launch Year: {launch_year}")
            if crew_capacity:
                print(f"    Crew Capacity: {crew_capacity}")
            print()

        print("=" * 80)
        print(f"Total astronauts in space: {len(enriched_astronauts)}")
        print("=" * 80 + "\n")

    @task
    def combine_data(
        enriched_astronauts: list[dict], weather: dict, **context
    ) -> pd.DataFrame:
        """
        Combines astronaut and weather data into a pandas DataFrame
        for analysis. Creates a record with astronaut count and weather metrics.
        """
        number_of_astronauts = len(enriched_astronauts)

        # Extract spacecraft information for summary
        spacecrafts = {}
        for person in enriched_astronauts:
            craft = person["craft"]
            if craft not in spacecrafts:
                spacecrafts[craft] = {
                    "model": person.get("spacecraft_model", craft),
                    "count": 0,
                    "agencies": person.get("operating_agencies", []),
                    "countries": person.get("operating_countries", []),
                }
            spacecrafts[craft]["count"] += 1

        # Display spacecraft summary
        print("\n" + "=" * 80)
        print("SPACECRAFT SUMMARY")
        print("=" * 80)
        for craft, info in spacecrafts.items():
            print(f"\n{craft} ({info['model']})")
            print(f"  Astronauts aboard: {info['count']}")
            print(f"  Agencies: {', '.join(info['agencies'])}")
            print(f"  Countries: {', '.join(info['countries'])}")
        print("=" * 80 + "\n")

        # Create a DataFrame with combined data
        data = {
            "timestamp": [weather["timestamp"]],
            "num_astronauts": [number_of_astronauts],
            "num_spacecraft": [len(spacecrafts)],
            "temperature": [weather["temperature"]],
            "wind_speed": [weather["wind_speed"]],
            "cloud_cover": [weather["cloud_cover"]],
        }

        df = pd.DataFrame(data)
        print(f"\nCombined Analysis Data:\n{df}\n")
        return df

    @task
    def analyze_correlation(df: pd.DataFrame) -> dict:
        """
        Performs correlation analysis between number of astronauts
        and weather metrics. Returns correlation coefficients and p-values.

        Note: This is a demonstration. In practice, you'd need historical
        data over time to compute meaningful correlations.
        """
        # For demonstration with single data point, we'll show the approach
        # In real scenarios, you'd accumulate data over multiple DAG runs

        results = {
            "message": "Single data point collected. Correlation analysis requires historical data.",
            "data_collected": {
                "num_astronauts": int(df["num_astronauts"].iloc[0]),
                "temperature": float(df["temperature"].iloc[0]),
                "wind_speed": float(df["wind_speed"].iloc[0]),
                "cloud_cover": float(df["cloud_cover"].iloc[0]),
            },
        }

        # If we had multiple data points, correlation would look like:
        # correlation, p_value = pearsonr(df["num_astronauts"], df["temperature"])

        print("Correlation Analysis Results:")
        print(
            f"Number of astronauts in space: {results['data_collected']['num_astronauts']}"
        )
        print(f"Temperature: {results['data_collected']['temperature']}°C")
        print(f"Wind speed: {results['data_collected']['wind_speed']} km/h")
        print(f"Cloud cover: {results['data_collected']['cloud_cover']}%")
        print(f"\n{results['message']}")
        print("\nTo perform actual correlation analysis, accumulate this data")
        print("over multiple DAG runs and store in a database or file system.")

        return results

    # Define task dependencies
    astronaut_list = get_astronauts()
    enriched_astronauts = enrich_spacecraft_data(astronaut_list)
    weather = get_weather_data()

    # Display enriched astronaut data with spacecraft information
    display_enriched_astronaut_data(enriched_astronauts)

    # Combine enriched data with weather and analyze correlation
    combined = combine_data(enriched_astronauts, weather)
    analyze_correlation(combined)


# Instantiate the DAG
example_astronauts()
