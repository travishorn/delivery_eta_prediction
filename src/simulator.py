import json
import random
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from geopy.distance import geodesic
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class SimulationClock:
    def __init__(self, time_acceleration_factor: float = 1.0):
        """Initialize simulation clock with optional time acceleration.

        Args:
            time_acceleration_factor: How much faster than real-time to run (e.g., 60.0 = 1 minute per second)
        """
        self.time_acceleration_factor = time_acceleration_factor
        self.start_real_time = time.time()
        self.start_sim_time = datetime.now()

    def now(self) -> datetime:
        """Get current simulation time."""
        elapsed_real_seconds = time.time() - self.start_real_time
        elapsed_sim_seconds = elapsed_real_seconds * self.time_acceleration_factor
        return self.start_sim_time + timedelta(seconds=elapsed_sim_seconds)

    def sleep(self, seconds: float):
        """Sleep for the specified number of simulation seconds."""
        real_seconds = seconds / self.time_acceleration_factor
        time.sleep(real_seconds)

class Driver:
    def __init__(self, driver_id: str, start_location: Tuple[float, float], 
                 destination_location: Tuple[float, float], initial_speed_mph: float = 40.0):
        self.driver_id = driver_id
        self.start_location = start_location
        self.destination_location = destination_location
        self.current_location = start_location
        self.current_speed_mph = initial_speed_mph
        self.status = 'EN_ROUTE'
        self.initial_planned_eta = None
        self.delay_end_time = None

    def calculate_distance_remaining(self) -> float:
        """Calculate remaining distance in miles."""
        return geodesic(self.current_location, self.destination_location).miles

    def is_completed(self) -> bool:
        """Check if delivery is completed (within 0.1 mi of destination)."""
        return self.status == 'COMPLETED'

class DeliverySimulator:
    def __init__(self, num_drivers: int = None, update_interval: int = None, 
                 time_acceleration_factor: float = None, kafka_bootstrap_servers: str = None, 
                 kafka_topic: str = None):
        """Initialize the delivery simulator.

        Args:
            num_drivers: Number of drivers to simulate. If None, will read from SIMULATION_NUM_DRIVERS env var
            update_interval: How often to update driver states (in simulation seconds). If None, will read from SIMULATION_UPDATE_INTERVAL env var
            time_acceleration_factor: How much faster than real-time to run. If None, will read from SIMULATION_TIME_ACCELERATION env var
            kafka_bootstrap_servers: Kafka bootstrap servers address. If None, will read from KAFKA_BOOTSTRAP_SERVERS env var
            kafka_topic: Kafka topic to publish messages to. If None, will read from KAFKA_TOPIC env var
        """
        self.drivers: List[Driver] = []
        
        # Get configuration from environment variables if not provided
        self.update_interval = update_interval or int(os.getenv('SIMULATION_UPDATE_INTERVAL', 10))
        self.time_acceleration_factor = time_acceleration_factor or float(os.getenv('SIMULATION_TIME_ACCELERATION', 1.0))
        num_drivers = num_drivers or int(os.getenv('SIMULATION_NUM_DRIVERS', 3))
        kafka_bootstrap_servers = kafka_bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        kafka_topic = kafka_topic or os.getenv('KAFKA_TOPIC', 'driver_updates')
        
        if kafka_bootstrap_servers is None:
            raise ValueError("Kafka bootstrap servers must be provided either directly or via KAFKA_BOOTSTRAP_SERVERS environment variable")
        
        self.weather_conditions = ['CLEAR', 'RAIN', 'SNOW']
        self.traffic_levels = ['LOW', 'MEDIUM', 'HIGH']
        self.clock = SimulationClock(self.time_acceleration_factor)
        
        # Initialize Kafka producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic

        # Initialize drivers with random start/end locations
        self._initialize_drivers(num_drivers)

    def _initialize_drivers(self, num_drivers: int):
        """Initialize drivers with random start/end locations within a reasonable area."""
        # Using Chicago area as an example boundary
        bounds = {
            'min_lat': 41.7,
            'max_lat': 42.0,
            'min_lon': -87.9,
            'max_lon': -87.5
        }

        for i in range(num_drivers):
            start_location = (
                random.uniform(bounds['min_lat'], bounds['max_lat']),
                random.uniform(bounds['min_lon'], bounds['max_lon'])
            )
            destination_location = (
                random.uniform(bounds['min_lat'], bounds['max_lat']),
                random.uniform(bounds['min_lon'], bounds['max_lon'])
            )

            driver = Driver(f"D{i+1}", start_location, destination_location)
            # Set initial ETA based on distance and initial speed
            distance = driver.calculate_distance_remaining()
            initial_time_hours = distance / driver.current_speed_mph
            driver.initial_planned_eta = self.clock.now() + timedelta(hours=initial_time_hours)
            self.drivers.append(driver)

    def _update_driver_status(self, driver: Driver):
        """Update driver status with random events."""
        if driver.delay_end_time and self.clock.now() >= driver.delay_end_time:
            driver.status = 'EN_ROUTE'
            driver.current_speed_mph = random.uniform(35.0, 45.0)
            driver.delay_end_time = None
            return

        if driver.status == 'EN_ROUTE' and random.random() < 0.1:  # 10% chance of event
            if random.random() < 0.5:  # 50% chance of traffic vs stop
                driver.status = 'DELAYED_TRAFFIC'
                driver.current_speed_mph *= 0.3  # Reduce speed significantly
            else:
                driver.status = 'AT_STOP'
                driver.current_speed_mph = 0

            # Set delay duration between 2-5 minutes
            delay_duration = random.uniform(2, 5)
            driver.delay_end_time = self.clock.now() + timedelta(minutes=delay_duration)

    def _update_driver_location(self, driver: Driver):
        """Update driver location based on current speed and heading."""
        if driver.status == 'AT_STOP' or driver.is_completed():
            return

        # Calculate movement vector
        distance_mi = (driver.current_speed_mph * self.update_interval) / 3600  # Convert to mi/update
        total_distance = geodesic(driver.current_location, driver.destination_location).miles

        # If we're very close to destination or would overshoot, complete the delivery
        if total_distance <= 0.05 or distance_mi >= total_distance:
            driver.current_location = driver.destination_location
            driver.status = 'COMPLETED'
            driver.current_speed_mph = 0
            return

        # Linear interpolation for normal movement
        fraction = distance_mi / total_distance
        new_lat = driver.current_location[0] + (driver.destination_location[0] - driver.current_location[0]) * fraction
        new_lon = driver.current_location[1] + (driver.destination_location[1] - driver.current_location[1]) * fraction
        driver.current_location = (new_lat, new_lon)

    def _generate_data_point(self, driver: Driver) -> Dict:
        """Generate a data point for the current driver state."""
        current_time = self.clock.now()
        return {
            "timestamp": current_time.isoformat(),
            "driver_id": driver.driver_id,
            "current_latitude": driver.current_location[0],
            "current_longitude": driver.current_location[1],
            "current_speed_mph": driver.current_speed_mph,
            "destination_latitude": driver.destination_location[0],
            "destination_longitude": driver.destination_location[1],
            "status": driver.status,
            "traffic_level": random.choice(self.traffic_levels),
            "weather": random.choice(self.weather_conditions),
            "initial_planned_eta": driver.initial_planned_eta.isoformat() if driver.initial_planned_eta else None
        }

    def run(self):
        """Main simulation loop."""
        print(f"Starting delivery simulation (Time acceleration: {self.clock.time_acceleration_factor}x)...")
        print("\nDriver Status Updates:")
        print("-" * 100)  # Separator line

        try:
            while True:
                active_drivers = [d for d in self.drivers if d.status != 'COMPLETED']
                if not active_drivers:
                    print("-" * 100)  # Separator line
                    print("All deliveries completed!")
                    break

                current_time = self.clock.now()
                for driver in active_drivers:
                    self._update_driver_status(driver)
                    self._update_driver_location(driver)
                    data_point = self._generate_data_point(driver)

                    # Send to Kafka
                    self.kafka_producer.send(self.kafka_topic, value=data_point)
                    self.kafka_producer.flush()

                    # Print status update with fixed-width formatting
                    status_str = f"{driver.status:<15}"  # Left-align status with 15 chars
                    speed_str = f"{driver.current_speed_mph:>6.1f}"  # Right-align speed with 6 chars
                    dist_str = f"{driver.calculate_distance_remaining():>6.2f}"  # Right-align distance with 6 chars

                    print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] "
                          f"Driver {driver.driver_id:<3} | "  # Left-align driver ID
                          f"Status: {status_str} | "
                          f"Speed: {speed_str} mph | "
                          f"Distance remaining: {dist_str} mi")

                self.clock.sleep(self.update_interval)

        except KeyboardInterrupt:
            print("\nSimulation stopped by user.")
        finally:
            # Ensure Kafka producer is properly closed
            self.kafka_producer.close()

if __name__ == "__main__":
    # Create simulator with configuration from environment variables
    simulator = DeliverySimulator()
    simulator.run()
