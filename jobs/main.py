import json
import os
import uuid
from confluent_kafka import SerializingProducer
import datetime
import random
import time
import requests
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# API Keys (You'll need to register for free accounts)
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "7d7c65d956208ba603fc9ac680e53663")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "ulVcCl89ueHANpf5nkDK6EFM96iob9Ew")
OPENTRIPMAP_API_KEY = os.getenv("OPENTRIPMAP_API_KEY", "your_opentrippmap_api_key")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

# Simulation state
random.seed(42)
start_time = datetime.datetime.now()
current_location = LONDON_COORDINATES.copy()


class RealDataAPIs:
    """Class to handle real API integrations"""

    @staticmethod
    def get_real_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get real weather data from OpenWeatherMap API
        Free tier: 1000 calls/day
        Sign up: https://openweathermap.org/api
        """
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': OPENWEATHER_API_KEY,
                'units': 'metric'
            }
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return {
                    'temperature': data['main']['temp'],
                    'weatherCondition': data['weather'][0]['main'],
                    'humidity': data['main']['humidity'],
                    'windSpeed': data['wind']['speed'],
                    'pressure': data['main']['pressure'],
                    'visibility': data.get('visibility', 10000)
                }
        except Exception as e:
            logger.warning(f"Weather API error: {e}. Using fallback data.")
        return None

    @staticmethod
    def get_traffic_conditions(lat: float, lon: float, radius: int = 5000) -> Optional[Dict[str, Any]]:
        """
        Get traffic conditions from TomTom Traffic API
        Free tier: 2500 transactions/day
        Sign up: https://developer.tomtom.com/traffic-api
        """
        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
            params = {
                'point': f"{lat},{lon}",
                'unit': 'KMPH',
                'key': TOMTOM_API_KEY
            }
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                flow = data['flowSegmentData']
                return {
                    'currentSpeed': flow['currentSpeed'],
                    'freeFlowSpeed': flow['freeFlowSpeed'],
                    'confidence': flow['confidence'],
                    'roadClosure': flow.get('roadClosure', False),
                    'trafficLevel': 'HEAVY' if flow['currentSpeed'] < flow['freeFlowSpeed'] * 0.5 else 'MODERATE' if
                    flow['currentSpeed'] < flow['freeFlowSpeed'] * 0.8 else 'LIGHT'
                }
        except Exception as e:
            logger.warning(f"Traffic API error: {e}. Using fallback data.")
        return None

    @staticmethod
    def get_emergency_incidents(lat: float, lon: float, radius: int = 10000) -> Optional[Dict[str, Any]]:
        """
        Get emergency incidents from OpenStreetMap/Overpass API
        Note: This is simulated as real emergency APIs are usually restricted
        Free and no API key required
        """
        try:
            # Using Overpass API to get nearby amenities that could indicate incidents
            overpass_query = f"""
            [out:json][timeout:25];
            (
              node["amenity"~"hospital|police|fire_station"](around:{radius},{lat},{lon});
              node["emergency"~"yes"](around:{radius},{lat},{lon});
            );
            out body;
            """
            url = "https://overpass-api.de/api/interpreter"
            response = requests.post(url, data={'data': overpass_query}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                elements = data.get('elements', [])
                if elements:
                    # Simulate incidents based on nearby emergency services
                    incident_types = ['Accident', 'Medical', 'Fire', 'Police']
                    return {
                        'nearbyServices': len(elements),
                        'incidentType': random.choice(incident_types),
                        'serviceTypes': list(set([elem.get('tags', {}).get('amenity', 'unknown') for elem in elements]))
                    }
        except Exception as e:
            logger.warning(f"Emergency API error: {e}. Using fallback data.")
        return None

    @staticmethod
    def get_vehicle_makes_models():
        """Get real vehicle makes and models"""
        # Using a free vehicle API or static data
        vehicles = [
            {'make': 'BMW', 'model': 'M4', 'year': 2023, 'fuelType': 'Petrol'},
            {'make': 'Audi', 'model': 'A4', 'year': 2022, 'fuelType': 'Diesel'},
            {'make': 'Mercedes', 'model': 'C-Class', 'year': 2023, 'fuelType': 'Hybrid'},
            {'make': 'Tesla', 'model': 'Model 3', 'year': 2023, 'fuelType': 'Electric'},
            {'make': 'Toyota', 'model': 'Prius', 'year': 2022, 'fuelType': 'Hybrid'},
            {'make': 'Ford', 'model': 'Focus', 'year': 2022, 'fuelType': 'Petrol'},
            {'make': 'Volkswagen', 'model': 'Golf', 'year': 2023, 'fuelType': 'Petrol'},
            {'make': 'Nissan', 'model': 'Leaf', 'year': 2023, 'fuelType': 'Electric'},
        ]
        return random.choice(vehicles)


def get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    """Simulate vehicle movement along London to Birmingham route"""
    global current_location

    # Calculate movement increments
    lat_increment = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
    lon_increment = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

    current_location["latitude"] += lat_increment
    current_location["longitude"] += lon_increment

    # Add some randomness to simulate real driving
    current_location["latitude"] += random.uniform(-0.001, 0.001)
    current_location["longitude"] += random.uniform(-0.001, 0.001)

    return current_location.copy()


def generate_gps_data(device_id: str, timestamp: str, vehicle_type: str = 'private') -> Dict[str, Any]:
    """Generate GPS data with realistic values"""
    # Realistic speed based on urban/rural areas
    if current_location["latitude"] < 51.8:  # London area
        speed = random.uniform(5, 30)  # Urban speeds
    else:
        speed = random.uniform(40, 70)  # Rural/highway speeds

    # Direction based on coordinates
    directions = ['North', 'North-East', 'East', 'South-East', 'South', 'South-West', 'West', 'North-West']

    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': round(speed, 2),
        'direction': random.choice(directions),
        'vehicleType': vehicle_type,
        'coordinates': f"{current_location['latitude']:.6f},{current_location['longitude']:.6f}"
    }


def generate_traffic_camera_data(device_id: str, timestamp: str, location: str, camera_id: str) -> Dict[str, Any]:
    """Generate traffic camera data with real traffic conditions"""
    traffic_data = RealDataAPIs.get_traffic_conditions(current_location["latitude"], current_location["longitude"])

    base_data = {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': f"camera_{camera_id}_{timestamp}",
    }

    if traffic_data:
        base_data.update({
            'trafficLevel': traffic_data['trafficLevel'],
            'currentSpeed': traffic_data['currentSpeed'],
            'freeFlowSpeed': traffic_data['freeFlowSpeed'],
            'confidence': traffic_data['confidence'],
            'roadClosure': traffic_data['roadClosure']
        })
    else:
        # Fallback data
        base_data.update({
            'trafficLevel': random.choice(['LIGHT', 'MODERATE', 'HEAVY']),
            'currentSpeed': random.uniform(20, 80),
            'freeFlowSpeed': random.uniform(60, 100),
            'confidence': random.uniform(0.7, 1.0),
            'roadClosure': random.choice([True, False])
        })

    return base_data


def generate_weather_data(device_id: str, timestamp: str, location: str) -> Dict[str, Any]:
    """Generate real weather data"""
    weather_data = RealDataAPIs.get_real_weather(current_location["latitude"], current_location["longitude"])

    base_data = {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
    }

    if weather_data:
        base_data.update({
            'temperature': round(weather_data['temperature'], 1),
            'weatherCondition': weather_data['weatherCondition'],
            'precipitation': random.uniform(0, 10),  # Fallback for precipitation
            'windSpeed': round(weather_data['windSpeed'], 1),
            'humidity': weather_data['humidity'],
            'airQualityIndex': random.uniform(0, 150),  # Fallback for AQI
            'pressure': weather_data.get('pressure', 1013),
            'visibility': weather_data.get('visibility', 10000)
        })
    else:
        # Fallback data
        base_data.update({
            'temperature': round(random.uniform(-5, 30), 1),
            'weatherCondition': random.choice(['Clear', 'Clouds', 'Rain', 'Snow', 'Mist']),
            'precipitation': round(random.uniform(0, 25), 1),
            'windSpeed': round(random.uniform(0, 50), 1),
            'humidity': random.randint(30, 100),
            'airQualityIndex': round(random.uniform(0, 200), 1),
            'pressure': random.randint(980, 1030),
            'visibility': random.randint(1000, 20000)
        })

    return base_data


def generate_emergency_data(device_id: str, timestamp: str, location: str) -> Dict[str, Any]:
    """Generate emergency data with real incident information"""
    emergency_info = RealDataAPIs.get_emergency_incidents(current_location["latitude"], current_location["longitude"])

    # 5% chance of emergency incident
    has_incident = random.random() < 0.05

    base_data = {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'hasIncident': has_incident,
    }

    if has_incident and emergency_info:
        base_data.update({
            'incidentId': str(uuid.uuid4()),
            'type': emergency_info['incidentType'],
            'status': random.choice(['Active', 'Responding', 'Resolved']),
            'description': f"{emergency_info['incidentType']} incident reported near {location}",
            'nearbyServices': emergency_info['nearbyServices'],
            'serviceTypes': emergency_info['serviceTypes']
        })
    elif has_incident:
        # Fallback emergency data
        incident_type = random.choice(['Accident', 'Medical', 'Fire', 'Police'])
        base_data.update({
            'incidentId': str(uuid.uuid4()),
            'type': incident_type,
            'status': random.choice(['Active', 'Responding', 'Resolved']),
            'description': f"{incident_type} incident reported near {location}",
            'severity': random.choice(['Low', 'Medium', 'High']),
            'responseTime': random.randint(5, 30)
        })
    else:
        base_data.update({
            'type': 'None',
            'status': 'None',
            'description': 'No incidents reported'
        })

    return base_data


def generate_vehicle_data(device_id: str) -> Dict[str, Any]:
    """Generate vehicle data with real makes/models"""
    location = simulate_vehicle_movement()
    vehicle_info = RealDataAPIs.get_vehicle_makes_models()

    # Realistic speed based on location
    if current_location["latitude"] < 51.8:  # London area
        speed = random.uniform(5, 30)
    else:
        speed = random.uniform(40, 70)

    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': f"({location['latitude']:.6f}, {location['longitude']:.6f})",
        'speed': round(speed, 2),
        'direction': random.choice(
            ['North', 'North-East', 'East', 'South-East', 'South', 'South-West', 'West', 'North-West']),
        'make': vehicle_info['make'],
        'model': vehicle_info['model'],
        'year': vehicle_info['year'],
        'fuelType': vehicle_info['fuelType'],
        'fuelLevel': round(random.uniform(10, 100), 1),  # Percentage
        'engineStatus': random.choice(['Running', 'Idle', 'Off']),
        'odometer': random.randint(0, 200000)
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {type(obj)} not serializable')


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to: {msg.topic()}[{msg.partition()}]")


def producer_data_to_kafka(producer, topic: str, data: Dict[str, Any]):
    try:
        producer.produce(
            topic,
            key=str(data['id']),
            value=json.dumps(data, default=json_serializer).encode("utf-8"),
            on_delivery=delivery_report
        )
        producer.flush()
    except Exception as e:
        logger.error(f"Error producing message to {topic}: {e}")


def simulate_journey(producer, device_id: str):
    """Simulate a real vehicle journey with actual API data"""
    journey_start = datetime.datetime.now()
    logger.info(f"Starting real-time journey simulation from London to Birmingham")

    while True:
        try:
            # Generate realistic data
            vehicle_data = generate_vehicle_data(device_id)
            gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
            traffic_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],
                                                        vehicle_data['location'], 'UK-CAM-001')
            weather_data = generate_weather_data(device_id, vehicle_data['timestamp'],
                                                 vehicle_data['location'])
            emergency_data = generate_emergency_data(device_id, vehicle_data['timestamp'],
                                                     vehicle_data['location'])

            # Check if vehicle has reached Birmingham
            current_lat = current_location["latitude"]
            current_lon = current_location["longitude"]

            if (current_lat >= BIRMINGHAM_COORDINATES['latitude'] - 0.1 and
                    current_lon <= BIRMINGHAM_COORDINATES['longitude']):
                journey_duration = datetime.datetime.now() - journey_start
                logger.info(f"Vehicle arrived at Birmingham. Journey completed in {journey_duration}.")
                break

            # Produce all data to Kafka
            producer_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
            producer_data_to_kafka(producer, GPS_TOPIC, gps_data)
            producer_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
            producer_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
            producer_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

            # Realistic delay between data points
            time.sleep(random.uniform(2, 8))

        except Exception as e:
            logger.error(f"Error in journey simulation: {e}")
            time.sleep(5)  # Wait before retrying


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "message.timeout.ms": 5000,
        "enable.idempotence": True
    }
    producer = SerializingProducer(producer_config)

    try:
        logger.info(f"Starting real-time smart city simulation with Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info("Using real APIs for weather, traffic, and emergency data")
        simulate_journey(producer, 'SmartCity-Vehicle-001')
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    except Exception as e:
        logger.error(f'Unexpected error: {e}')