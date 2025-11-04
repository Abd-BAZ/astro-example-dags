"""
## Astronaut ETL example DAG with Weather Data Correlation and Spacecraft Information

This DAG queries the list of astronauts currently in space from the
Open Notify API and enriches the data with comprehensive spacecraft information
including models, operating agencies, countries, orbital speeds, velocities,
mission goals, and historical achievements. It also fetches weather data and
analyzes the correlation between the number of astronauts in space and weather conditions.

There are multiple tasks:
1. Get astronaut data from the API
2. Enrich astronaut data with spacecraft models, agencies, countries, orbital velocity, mission goals, and history
3. Display detailed astronaut and spacecraft information with speed/velocity data
4. Display spacecraft history, mission goals, notable achievements, and specifications
5. Get weather data from Open-Meteo API (ISS location)
6. Combine enriched astronaut data with weather data
7. Perform correlation analysis

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
    schedule=None,  # Set to None to disable automatic runs
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
        try:
            r = requests.get("http://api.open-notify.org/astros.json", timeout=10)
            r.raise_for_status()  # Raise an exception for bad status codes

            data = r.json()  # Parse JSON once and store it
            number_of_people_in_space = data["number"]
            list_of_people_in_space = data["people"]

            context["ti"].xcom_push(
                key="number_of_people_in_space", value=number_of_people_in_space
            )

            print(
                f"Successfully retrieved data for {number_of_people_in_space} astronauts in space"
            )
            return list_of_people_in_space

        except requests.exceptions.RequestException as e:
            print(f"Error fetching astronaut data: {e}")
            raise
        except (KeyError, ValueError) as e:
            print(f"Error parsing astronaut data: {e}")
            raise

    @task
    def enrich_spacecraft_data(astronauts: list[dict]) -> list[dict]:
        """
        Enriches astronaut data with spacecraft models, operating agencies,
        and countries. Maps spacecraft names to their detailed information.
        """
        # Spacecraft information mapping with orbital velocity data and history
        spacecraft_info = {
            "ISS": {
                "model": "International Space Station",
                "type": "Space Station",
                "agencies": ["NASA", "Roscosmos", "ESA", "JAXA", "CSA"],
                "countries": ["USA", "Russia", "Europe", "Japan", "Canada"],
                "launch_year": 1998,
                "crew_capacity": 7,
                "orbital_speed_kmh": 27600,  # km/h
                "orbital_speed_mph": 17150,  # mph
                "orbital_velocity_ms": 7660,  # m/s
                "altitude_km": 408,  # Average altitude in km
                "orbital_period_min": 92.68,  # Time to complete one orbit in minutes
                "history": {
                    "first_module": "Zarya (launched Nov 20, 1998)",
                    "first_crew": "Expedition 1 (Nov 2, 2000)",
                    "assembly_period": "1998-2011",
                    "continuous_occupation": "Since November 2, 2000 (20+ years)",
                    "total_visitors": "Over 270 people from 20+ countries",
                    "mass_kg": 420000,
                    "pressurized_volume_m3": 916,
                    "solar_array_area_m2": 2500,
                    "cost_usd_billion": 150,
                    "mission_goals": [
                        "Scientific Research: Conduct experiments in microgravity, biology, physics, astronomy, and materials science",
                        "Technology Development: Test new technologies for deep space exploration",
                        "Human Health: Study long-term effects of space on human body and develop countermeasures",
                        "Earth Observation: Monitor climate change, natural disasters, and environmental conditions",
                        "International Cooperation: Foster peaceful collaboration between nations in space exploration",
                        "Commercial Opportunities: Support commercial space activities and research",
                        "Education and Outreach: Inspire future generations through space education programs",
                    ],
                    "notable_achievements": [
                        "Longest continuously inhabited space station in history",
                        "Largest human-made structure in space",
                        "Platform for over 3,000 scientific experiments",
                        "International collaboration between 5 space agencies",
                    ],
                },
            },
            "Tiangong": {
                "model": "Tiangong Space Station (CSS)",
                "type": "Space Station",
                "agencies": ["CNSA"],
                "countries": ["China"],
                "launch_year": 2021,
                "crew_capacity": 6,
                "orbital_speed_kmh": 27840,  # km/h
                "orbital_speed_mph": 17300,  # mph
                "orbital_velocity_ms": 7733,  # m/s
                "altitude_km": 400,  # Average altitude in km
                "orbital_period_min": 91.6,  # Time to complete one orbit in minutes
                "history": {
                    "first_module": "Tianhe core module (launched Apr 29, 2021)",
                    "first_crew": "Shenzhou 12 (Jun 17, 2021)",
                    "assembly_period": "2021-2022",
                    "continuous_occupation": "Since June 2022",
                    "total_visitors": "Multiple crews via Shenzhou missions",
                    "mass_kg": 66000,
                    "pressurized_volume_m3": 110,
                    "solar_array_area_m2": 138,
                    "cost_usd_billion": 11,
                    "mission_goals": [
                        "Space Science: Conduct research in space life science, biotechnology, and space medicine",
                        "Space Technology: Test and validate new technologies for future deep space missions",
                        "Space Applications: Develop applications for Earth observation and space resource utilization",
                        "Microgravity Experiments: Study material science and fluid physics in zero gravity",
                        "National Capability: Demonstrate China's independent human spaceflight capability",
                        "Long-Duration Missions: Support 6-month crew rotations for extended research",
                        "International Collaboration: Welcome international partners and experiments",
                    ],
                    "notable_achievements": [
                        "First modular space station built by China",
                        "Third operational space station after ISS",
                        "Features advanced life support systems",
                        "Open to international cooperation",
                    ],
                },
            },
            "Shenzhou": {
                "model": "Shenzhou Spacecraft",
                "type": "Crew Vehicle",
                "agencies": ["CNSA"],
                "countries": ["China"],
                "launch_year": 1999,
                "crew_capacity": 3,
                "orbital_speed_kmh": 27840,  # km/h (when docked or in orbit)
                "orbital_speed_mph": 17300,  # mph
                "orbital_velocity_ms": 7733,  # m/s
                "altitude_km": 400,  # Typical altitude in km
                "orbital_period_min": 91.6,  # Time to complete one orbit in minutes
                "history": {
                    "first_launch": "Shenzhou 1 (Nov 20, 1999, uncrewed)",
                    "first_crewed": "Shenzhou 5 (Oct 15, 2003, Yang Liwei)",
                    "total_missions": "17+ missions (as of 2024)",
                    "based_on": "Russian Soyuz design with Chinese modifications",
                    "mass_kg": 7840,
                    "length_m": 9.25,
                    "diameter_m": 2.8,
                    "mission_goals": [
                        "Crew Transportation: Safely transport taikonauts to and from Tiangong space station",
                        "Technology Demonstration: Prove Chinese capability for independent human spaceflight",
                        "Orbital Operations: Conduct solo orbital missions for testing and training",
                        "Rendezvous and Docking: Perfect automated and manual docking procedures",
                        "Spacewalk Support: Provide platform for extravehicular activities (EVAs)",
                        "Emergency Capability: Serve as emergency escape vehicle when docked to station",
                        "National Pride: Demonstrate China's technological advancement in space exploration",
                    ],
                    "notable_achievements": [
                        "Made China the 3rd country to independently launch humans to space",
                        "Successfully conducted China's first spacewalk (Shenzhou 7, 2008)",
                        "Primary crew transport vehicle for Tiangong station",
                        "Reliable workhorse with perfect safety record",
                    ],
                },
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
                    "orbital_speed_kmh": None,
                    "orbital_speed_mph": None,
                    "orbital_velocity_ms": None,
                    "altitude_km": None,
                    "orbital_period_min": None,
                    "history": {},
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
                    "orbital_speed_kmh": spacecraft["orbital_speed_kmh"],
                    "orbital_speed_mph": spacecraft["orbital_speed_mph"],
                    "orbital_velocity_ms": spacecraft["orbital_velocity_ms"],
                    "altitude_km": spacecraft["altitude_km"],
                    "orbital_period_min": spacecraft["orbital_period_min"],
                    "history": spacecraft["history"],
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
        try:
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

            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()  # Raise an exception for bad status codes
            weather_data = r.json()

            current_weather = {
                "temperature": weather_data["current"]["temperature_2m"],
                "wind_speed": weather_data["current"]["wind_speed_10m"],
                "cloud_cover": weather_data["current"]["cloud_cover"],
                "timestamp": weather_data["current"]["time"],
            }

            context["ti"].xcom_push(key="weather_data", value=current_weather)
            print(
                f"Successfully retrieved weather data: {current_weather['temperature']}°C"
            )
            return current_weather

        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            raise
        except (KeyError, ValueError) as e:
            print(f"Error parsing weather data: {e}")
            raise

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
            orbital_speed_kmh = person.get("orbital_speed_kmh")
            orbital_speed_mph = person.get("orbital_speed_mph")
            orbital_velocity_ms = person.get("orbital_velocity_ms")
            altitude_km = person.get("altitude_km")
            orbital_period_min = person.get("orbital_period_min")

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

            # Display orbital velocity information
            if orbital_speed_kmh:
                print(
                    f"    Orbital Speed: {orbital_speed_kmh:,} km/h ({orbital_speed_mph:,} mph)"
                )
            if orbital_velocity_ms:
                print(f"    Orbital Velocity: {orbital_velocity_ms:,} m/s")
            if altitude_km:
                print(f"    Altitude: {altitude_km} km")
            if orbital_period_min:
                print(
                    f"    Orbital Period: {orbital_period_min:.2f} minutes (~{orbital_period_min / 60:.2f} hours)"
                )
            print()

        print("=" * 80)
        print(f"Total astronauts in space: {len(enriched_astronauts)}")
        print("=" * 80 + "\n")

    @task
    def display_spacecraft_history(enriched_astronauts: list[dict]) -> None:
        """
        Displays historical information about each unique spacecraft
        including mission goals, notable achievements, specifications, and milestones.
        """
        # Get unique spacecraft from the astronaut list
        unique_spacecraft = {}
        for person in enriched_astronauts:
            craft = person["craft"]
            if craft not in unique_spacecraft:
                unique_spacecraft[craft] = person

        print("\n" + "=" * 80)
        print("SPACECRAFT HISTORY & ACHIEVEMENTS")
        print("=" * 80 + "\n")

        for craft, person in unique_spacecraft.items():
            history = person.get("history", {})
            if not history:
                continue

            spacecraft_model = person.get("spacecraft_model", craft)
            print(f"{'=' * 80}")
            print(f"{craft} - {spacecraft_model}")
            print(f"{'=' * 80}")

            # Display historical milestones
            if "first_module" in history:
                print(f"\n  First Module: {history['first_module']}")
            if "first_launch" in history:
                print(f"\n  First Launch: {history['first_launch']}")
            if "first_crew" in history or "first_crewed" in history:
                first_crew = history.get("first_crew", history.get("first_crewed"))
                print(f"  First Crew: {first_crew}")
            if "assembly_period" in history:
                print(f"  Assembly Period: {history['assembly_period']}")
            if "continuous_occupation" in history:
                print(f"  Continuous Occupation: {history['continuous_occupation']}")
            if "total_missions" in history:
                print(f"  Total Missions: {history['total_missions']}")
            if "total_visitors" in history:
                print(f"  Total Visitors: {history['total_visitors']}")
            if "based_on" in history:
                print(f"  Design: {history['based_on']}")

            # Display mission goals
            if "mission_goals" in history and history["mission_goals"]:
                print("\n  Mission Goals:")
                for goal in history["mission_goals"]:
                    print(f"    • {goal}")

            # Display specifications
            print("\n  Specifications:")
            if "mass_kg" in history:
                print(f"    Mass: {history['mass_kg']:,} kg")
            if "length_m" in history:
                print(f"    Length: {history['length_m']} m")
            if "diameter_m" in history:
                print(f"    Diameter: {history['diameter_m']} m")
            if "pressurized_volume_m3" in history:
                print(
                    f"    Pressurized Volume: {history['pressurized_volume_m3']:,} m³"
                )
            if "solar_array_area_m2" in history:
                print(f"    Solar Array Area: {history['solar_array_area_m2']:,} m²")
            if "cost_usd_billion" in history:
                print(f"    Estimated Cost: ${history['cost_usd_billion']} billion USD")

            # Display notable achievements
            if "notable_achievements" in history and history["notable_achievements"]:
                print("\n  Notable Achievements:")
                for achievement in history["notable_achievements"]:
                    print(f"    • {achievement}")

            print()

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
                    "orbital_speed_kmh": person.get("orbital_speed_kmh"),
                    "orbital_velocity_ms": person.get("orbital_velocity_ms"),
                    "altitude_km": person.get("altitude_km"),
                    "orbital_period_min": person.get("orbital_period_min"),
                }
            spacecrafts[craft]["count"] += 1

        # Display spacecraft summary with velocity information
        print("\n" + "=" * 80)
        print("SPACECRAFT SUMMARY")
        print("=" * 80)
        for craft, info in spacecrafts.items():
            print(f"\n{craft} ({info['model']})")
            print(f"  Astronauts aboard: {info['count']}")
            print(f"  Agencies: {', '.join(info['agencies'])}")
            print(f"  Countries: {', '.join(info['countries'])}")
            if info["orbital_speed_kmh"]:
                print(f"  Orbital Speed: {info['orbital_speed_kmh']:,} km/h")
            if info["orbital_velocity_ms"]:
                print(f"  Orbital Velocity: {info['orbital_velocity_ms']:,} m/s")
            if info["altitude_km"]:
                print(f"  Altitude: {info['altitude_km']} km")
            if info["orbital_period_min"]:
                print(f"  Orbital Period: {info['orbital_period_min']:.2f} minutes")
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
    def filter_astronauts_by_craft(astronauts: list[dict]) -> dict[str, list[str]]:
        """
        Filters astronauts into groups by their spacecraft name.
        Returns a dictionary mapping spacecraft names to lists of astronaut names.
        """
        spacecraft_groups = {}

        for astronaut in astronauts:
            craft_name = astronaut["craft"]
            astronaut_name = astronaut["name"]

            if craft_name not in spacecraft_groups:
                spacecraft_groups[craft_name] = []

            spacecraft_groups[craft_name].append(astronaut_name)

        # Display the grouped results
        print("\n" + "=" * 80)
        print("ASTRONAUTS GROUPED BY SPACECRAFT")
        print("=" * 80)
        for craft, astronaut_names in spacecraft_groups.items():
            print(f"\n{craft}: {len(astronaut_names)} astronaut(s)")
            for name in astronaut_names:
                print(f"  - {name}")
        print("=" * 80 + "\n")

        return spacecraft_groups

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

    # Filter astronauts by spacecraft
    filter_astronauts_by_craft(astronaut_list)

    # Display enriched astronaut data with spacecraft information
    display_enriched_astronaut_data(enriched_astronauts)

    # Display spacecraft history and achievements
    display_spacecraft_history(enriched_astronauts)

    # Combine enriched data with weather and analyze correlation
    combined = combine_data(enriched_astronauts, weather)
    analyze_correlation(combined)


# Instantiate the DAG
example_astronauts()
