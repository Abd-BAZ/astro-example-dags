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
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover,weather_code",
                "timezone": "UTC",
            }

            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()  # Raise an exception for bad status codes
            weather_data = r.json()

            current_weather = {
                "temperature": weather_data["current"]["temperature_2m"],
                "humidity": weather_data["current"]["relative_humidity_2m"],
                "wind_speed": weather_data["current"]["wind_speed_10m"],
                "cloud_cover": weather_data["current"]["cloud_cover"],
                "weather_code": weather_data["current"]["weather_code"],
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
    def map_spacecraft_assignments(astronauts: list[dict]) -> dict[str, list[str]]:
        """
        Maps spacecraft names to lists of astronauts currently assigned to them.
        Returns a dictionary where keys are spacecraft names and values are lists of astronaut names.
        """
        spacecraft_assignments = {}

        for astronaut in astronauts:
            spacecraft_name = astronaut["craft"]
            astronaut_name = astronaut["name"]

            if spacecraft_name not in spacecraft_assignments:
                spacecraft_assignments[spacecraft_name] = []

            spacecraft_assignments[spacecraft_name].append(astronaut_name)

        print(
            f"\nMapped {len(astronauts)} astronauts to {len(spacecraft_assignments)} spacecraft"
        )
        return spacecraft_assignments

    @task
    def calculate_mission_distance(enriched_astronauts: list[dict]) -> dict[str, dict]:
        """
        Calculates the total kilometers traveled per astronaut based on their spacecraft's
        orbital speed and estimated mission duration. Uses orbital speed and period data.
        """
        astronaut_distances = {}

        # Simulated mission duration in days (for demonstration)
        # In a real scenario, this would come from actual mission start dates
        estimated_mission_days = 180  # ~6 months typical mission duration

        print("\n" + "=" * 80)
        print("MISSION DISTANCE CALCULATIONS")
        print("=" * 80)
        print(f"Estimated mission duration: {estimated_mission_days} days\n")

        for astronaut in enriched_astronauts:
            name = astronaut["name"]
            craft = astronaut["craft"]
            orbital_speed_kmh = astronaut.get("orbital_speed_kmh")
            orbital_period_min = astronaut.get("orbital_period_min")

            if orbital_speed_kmh:
                # Calculate distance traveled
                hours_in_mission = estimated_mission_days * 24
                total_distance_km = orbital_speed_kmh * hours_in_mission

                # Calculate number of orbits completed
                orbits_completed = 0
                if orbital_period_min:
                    minutes_in_mission = estimated_mission_days * 24 * 60
                    orbits_completed = minutes_in_mission / orbital_period_min

                astronaut_distances[name] = {
                    "spacecraft": craft,
                    "orbital_speed_kmh": orbital_speed_kmh,
                    "mission_days": estimated_mission_days,
                    "total_distance_km": total_distance_km,
                    "total_distance_miles": total_distance_km
                    * 0.621371,  # Convert to miles
                    "orbits_completed": orbits_completed,
                    "distance_per_day_km": total_distance_km / estimated_mission_days,
                }

                print(f"{name} ({craft}):")
                print(f"  Orbital Speed: {orbital_speed_kmh:,} km/h")
                print(
                    f"  Total Distance: {total_distance_km:,.0f} km ({total_distance_km * 0.621371:,.0f} miles)"
                )
                print(
                    f"  Distance per Day: {total_distance_km / estimated_mission_days:,.0f} km"
                )
                if orbits_completed > 0:
                    print(f"  Orbits Completed: {orbits_completed:,.0f}")
                print()
            else:
                # Handle cases where orbital speed is not available
                astronaut_distances[name] = {
                    "spacecraft": craft,
                    "orbital_speed_kmh": None,
                    "mission_days": estimated_mission_days,
                    "total_distance_km": None,
                    "total_distance_miles": None,
                    "orbits_completed": None,
                    "distance_per_day_km": None,
                }
                print(f"{name} ({craft}): Orbital data not available\n")

        print("=" * 80 + "\n")
        return astronaut_distances

    @task
    def evaluate_health_metrics(astronauts: list[dict]) -> dict[str, dict]:
        """
        Evaluates health metrics for each astronaut and assigns risk scores.
        Simulates health data including oxygen saturation, heart rate, and blood pressure.

        Risk Categories:
        - Normal: All vitals within healthy ranges
        - Monitor: One or more vitals slightly elevated
        - At Risk: One or more vitals significantly outside normal ranges
        - Critical: Multiple vitals in dangerous ranges
        """
        import random

        health_evaluations = {}

        print("\n" + "=" * 80)
        print("ASTRONAUT HEALTH METRICS EVALUATION")
        print("=" * 80)
        print("Note: Simulated health data for demonstration purposes\n")

        for astronaut in astronauts:
            name = astronaut["name"]
            craft = astronaut["craft"]

            # Simulate health metrics (in real scenario, this would come from medical sensors)
            # Most astronauts have normal vitals, with occasional variations
            oxygen_saturation = random.randint(92, 100)  # Normal: 95-100%
            heart_rate = random.randint(55, 115)  # Normal: 60-100 bpm
            systolic_bp = random.randint(100, 145)  # Normal: 110-130 mmHg
            diastolic_bp = random.randint(60, 95)  # Normal: 70-85 mmHg
            body_temp = round(random.uniform(36.1, 38.2), 1)  # Normal: 36.5-37.5°C

            # Evaluate each metric
            risk_factors = []
            risk_score = 0

            # Oxygen saturation evaluation
            if oxygen_saturation < 90:
                risk_factors.append("Critical oxygen level")
                risk_score += 3
            elif oxygen_saturation < 95:
                risk_factors.append("Low oxygen saturation")
                risk_score += 2

            # Heart rate evaluation
            if heart_rate > 120 or heart_rate < 50:
                risk_factors.append("Critical heart rate")
                risk_score += 3
            elif heart_rate > 100 or heart_rate < 60:
                risk_factors.append("Elevated/Low heart rate")
                risk_score += 1

            # Blood pressure evaluation
            if systolic_bp > 140 or systolic_bp < 90:
                risk_factors.append("Abnormal blood pressure")
                risk_score += 2
            elif systolic_bp > 130 or diastolic_bp > 85:
                risk_factors.append("Slightly elevated blood pressure")
                risk_score += 1

            # Body temperature evaluation
            if body_temp > 38.0 or body_temp < 36.0:
                risk_factors.append("Abnormal body temperature")
                risk_score += 2
            elif body_temp > 37.5 or body_temp < 36.5:
                risk_factors.append("Slight temperature variation")
                risk_score += 1

            # Determine overall health status
            if risk_score == 0:
                health_status = "Normal"
                status_emoji = "✓"
            elif risk_score <= 2:
                health_status = "Monitor"
                status_emoji = "⚠"
            elif risk_score <= 4:
                health_status = "At Risk"
                status_emoji = "⚠⚠"
            else:
                health_status = "Critical"
                status_emoji = "⚠⚠⚠"

            health_evaluations[name] = {
                "spacecraft": craft,
                "oxygen_saturation": oxygen_saturation,
                "heart_rate": heart_rate,
                "systolic_bp": systolic_bp,
                "diastolic_bp": diastolic_bp,
                "body_temp": body_temp,
                "risk_score": risk_score,
                "health_status": health_status,
                "risk_factors": risk_factors,
            }

            # Display health report
            print(f"{name} ({craft}) - {health_status} {status_emoji}")
            print(f"  Oxygen Saturation: {oxygen_saturation}% (Normal: 95-100%)")
            print(f"  Heart Rate: {heart_rate} bpm (Normal: 60-100 bpm)")
            print(
                f"  Blood Pressure: {systolic_bp}/{diastolic_bp} mmHg (Normal: 110-130/70-85)"
            )
            print(f"  Body Temperature: {body_temp}°C (Normal: 36.5-37.5°C)")
            print(f"  Risk Score: {risk_score}")
            if risk_factors:
                print(f"  Risk Factors: {', '.join(risk_factors)}")
            print()

        # Summary statistics
        status_counts = {}
        for evaluation in health_evaluations.values():
            status = evaluation["health_status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        print("-" * 80)
        print("HEALTH STATUS SUMMARY:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count} astronaut(s)")
        print("=" * 80 + "\n")

        return health_evaluations

    @task
    def analyze_team_diversity(astronauts: list[dict]) -> dict[str, dict]:
        """
        Analyzes crew diversity for each spacecraft across multiple dimensions:
        - Gender diversity
        - Nationality diversity
        - Experience level diversity

        Returns diversity scores and metrics for each spacecraft.
        """
        import random

        # Simulate demographic data (in real scenario, this would come from crew database)
        genders = ["Male", "Female", "Non-binary"]
        nationalities = [
            "USA",
            "Russia",
            "China",
            "Japan",
            "Canada",
            "Germany",
            "France",
            "Italy",
            "UK",
            "India",
        ]
        experience_levels = ["Rookie", "Intermediate", "Veteran", "Commander"]

        # Add simulated diversity data to astronauts
        enriched_data = []
        for astronaut in astronauts:
            enriched = astronaut.copy()
            enriched["gender"] = random.choice(genders)
            enriched["nationality"] = random.choice(nationalities)
            enriched["experience_level"] = random.choice(experience_levels)
            enriched["missions_completed"] = random.randint(0, 6)
            enriched_data.append(enriched)

        # Group by spacecraft
        spacecraft_crews = {}
        for astronaut in enriched_data:
            craft = astronaut["craft"]
            if craft not in spacecraft_crews:
                spacecraft_crews[craft] = []
            spacecraft_crews[craft].append(astronaut)

        diversity_analysis = {}

        print("\n" + "=" * 80)
        print("SPACECRAFT CREW DIVERSITY ANALYSIS")
        print("=" * 80)
        print("Note: Simulated diversity data for demonstration purposes\n")

        for craft, crew in spacecraft_crews.items():
            crew_size = len(crew)

            # Gender diversity calculation
            gender_counts = {}
            for member in crew:
                gender = member["gender"]
                gender_counts[gender] = gender_counts.get(gender, 0) + 1

            # Calculate gender diversity score (0-1, higher is more diverse)
            # Using Simpson's Diversity Index: 1 - sum((n/N)^2)
            gender_diversity = 1 - sum(
                (count / crew_size) ** 2 for count in gender_counts.values()
            )

            # Nationality diversity calculation
            nationality_counts = {}
            for member in crew:
                nationality = member["nationality"]
                nationality_counts[nationality] = (
                    nationality_counts.get(nationality, 0) + 1
                )

            nationality_diversity = 1 - sum(
                (count / crew_size) ** 2 for count in nationality_counts.values()
            )

            # Experience diversity calculation
            experience_counts = {}
            for member in crew:
                experience = member["experience_level"]
                experience_counts[experience] = experience_counts.get(experience, 0) + 1

            experience_diversity = 1 - sum(
                (count / crew_size) ** 2 for count in experience_counts.values()
            )

            # Overall diversity score (average of all dimensions)
            overall_diversity = (
                gender_diversity + nationality_diversity + experience_diversity
            ) / 3

            # Calculate additional metrics
            avg_missions = sum(m["missions_completed"] for m in crew) / crew_size
            unique_nationalities = len(nationality_counts)
            unique_genders = len(gender_counts)
            unique_experience_levels = len(experience_counts)

            # Diversity rating
            if overall_diversity >= 0.7:
                diversity_rating = "Highly Diverse"
            elif overall_diversity >= 0.5:
                diversity_rating = "Moderately Diverse"
            elif overall_diversity >= 0.3:
                diversity_rating = "Low Diversity"
            else:
                diversity_rating = "Homogeneous"

            diversity_analysis[craft] = {
                "crew_size": crew_size,
                "gender_diversity_score": round(gender_diversity, 3),
                "nationality_diversity_score": round(nationality_diversity, 3),
                "experience_diversity_score": round(experience_diversity, 3),
                "overall_diversity_score": round(overall_diversity, 3),
                "diversity_rating": diversity_rating,
                "gender_distribution": gender_counts,
                "nationality_distribution": nationality_counts,
                "experience_distribution": experience_counts,
                "unique_nationalities": unique_nationalities,
                "unique_genders": unique_genders,
                "unique_experience_levels": unique_experience_levels,
                "average_missions_completed": round(avg_missions, 1),
                "crew_members": [
                    {
                        "name": m["name"],
                        "gender": m["gender"],
                        "nationality": m["nationality"],
                        "experience": m["experience_level"],
                        "missions": m["missions_completed"],
                    }
                    for m in crew
                ],
            }

            # Display diversity report
            print(f"{craft}")
            print("-" * 80)
            print(f"Crew Size: {crew_size}")
            print(
                f"Overall Diversity Score: {overall_diversity:.3f} - {diversity_rating}"
            )
            print()
            print(f"Gender Diversity Score: {gender_diversity:.3f}")
            print(f"  Distribution: {dict(gender_counts)}")
            print(f"  Unique Genders: {unique_genders}")
            print()
            print(f"Nationality Diversity Score: {nationality_diversity:.3f}")
            print(f"  Distribution: {dict(nationality_counts)}")
            print(f"  Unique Nationalities: {unique_nationalities}")
            print()
            print(f"Experience Diversity Score: {experience_diversity:.3f}")
            print(f"  Distribution: {dict(experience_counts)}")
            print(f"  Unique Experience Levels: {unique_experience_levels}")
            print(f"  Average Missions Completed: {avg_missions:.1f}")
            print()
            print("Crew Members:")
            for member in crew:
                print(
                    f"  • {member['name']}: {member['gender']}, {member['nationality']}, "
                    f"{member['experience_level']} ({member['missions_completed']} missions)"
                )
            print()

        # Overall summary
        print("=" * 80)
        print("DIVERSITY SUMMARY ACROSS ALL SPACECRAFT:")
        avg_overall_diversity = sum(
            d["overall_diversity_score"] for d in diversity_analysis.values()
        ) / len(diversity_analysis)
        print(f"Average Overall Diversity Score: {avg_overall_diversity:.3f}")
        print()
        for craft, data in diversity_analysis.items():
            print(
                f"  {craft}: {data['overall_diversity_score']:.3f} ({data['diversity_rating']})"
            )
        print("=" * 80 + "\n")

        return diversity_analysis

    @task
    def summarize_weather_conditions(weather: dict) -> dict:
        """
        Summarizes weather data including temperature, humidity, and weather descriptions.
        Converts weather codes to human-readable descriptions.
        """
        # WMO Weather interpretation codes mapping
        weather_code_descriptions = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            71: "Slight snow fall",
            73: "Moderate snow fall",
            75: "Heavy snow fall",
            77: "Snow grains",
            80: "Slight rain showers",
            81: "Moderate rain showers",
            82: "Violent rain showers",
            85: "Slight snow showers",
            86: "Heavy snow showers",
            95: "Thunderstorm",
            96: "Thunderstorm with slight hail",
            99: "Thunderstorm with heavy hail",
        }

        weather_code = weather.get("weather_code", 0)
        weather_description = weather_code_descriptions.get(
            weather_code, f"Unknown (code: {weather_code})"
        )

        summary = {
            "temperature": weather.get("temperature"),
            "humidity": weather.get("humidity"),
            "wind_speed": weather.get("wind_speed"),
            "cloud_cover": weather.get("cloud_cover"),
            "weather_description": weather_description,
            "weather_code": weather_code,
            "timestamp": weather.get("timestamp"),
        }

        # Display the summary
        print("\n" + "=" * 80)
        print("WEATHER CONDITIONS SUMMARY")
        print("=" * 80)
        print(f"Timestamp: {summary['timestamp']}")
        print(f"Temperature: {summary['temperature']}°C")
        print(f"Humidity: {summary['humidity']}%")
        print(f"Wind Speed: {summary['wind_speed']} km/h")
        print(f"Cloud Cover: {summary['cloud_cover']}%")
        print(f"Weather: {summary['weather_description']}")
        print("=" * 80 + "\n")

        return summary

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

    # Map spacecraft assignments
    map_spacecraft_assignments(astronaut_list)

    # Evaluate health metrics
    evaluate_health_metrics(astronaut_list)

    # Analyze team diversity
    analyze_team_diversity(astronaut_list)

    # Calculate mission distances
    calculate_mission_distance(enriched_astronauts)

    # Summarize weather conditions
    summarize_weather_conditions(weather)

    # Display enriched astronaut data with spacecraft information
    display_enriched_astronaut_data(enriched_astronauts)

    # Display spacecraft history and achievements
    display_spacecraft_history(enriched_astronauts)

    # Combine enriched data with weather and analyze correlation
    combined = combine_data(enriched_astronauts, weather)
    analyze_correlation(combined)


# Instantiate the DAG
example_astronauts()
