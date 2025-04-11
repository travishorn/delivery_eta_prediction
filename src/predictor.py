import json
import time
from datetime import datetime
import pandas as pd
import numpy as np
import joblib
from typing import Dict, Optional
from geopy.distance import geodesic
import os

class ETAPredictor:
    def __init__(self, model_path: str = 'eta_predictor_model.joblib', 
                 encoders_path: str = 'label_encoders.joblib'):
        """Initialize the ETA predictor with trained model and encoders."""
        print("Loading model and encoders...")
        self.model = joblib.load(model_path)
        self.label_encoders = joblib.load(encoders_path)
        self.last_processed_position = 0
        
    def prepare_features(self, data_point: Dict) -> pd.DataFrame:
        """Prepare features for a single data point."""
        # Convert timestamp and calculate time-based features
        timestamp = pd.to_datetime(data_point['timestamp'])
        initial_eta = pd.to_datetime(data_point['initial_planned_eta'])
        
        # Calculate distance remaining
        distance_remaining = geodesic(
            (data_point['current_latitude'], data_point['current_longitude']),
            (data_point['destination_latitude'], data_point['destination_longitude'])
        ).miles
        
        # Create feature dictionary
        features = {
            'distance_remaining_mi': distance_remaining,
            'current_speed_mph': data_point['current_speed_mph'],
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.dayofweek,
            'status_encoded': self.label_encoders['status'].transform([data_point['status']])[0],
            'traffic_level_encoded': self.label_encoders['traffic_level'].transform([data_point['traffic_level']])[0],
            'weather_encoded': self.label_encoders['weather'].transform([data_point['weather']])[0],
            'time_until_planned_eta': (initial_eta - timestamp).total_seconds() / 60  # minutes
        }
        
        return pd.DataFrame([features])
    
    def predict_eta(self, data_point: Dict) -> Dict:
        """Generate ETA prediction for a single data point."""
        # Prepare features
        X = self.prepare_features(data_point)
        
        # Make prediction (remaining minutes)
        predicted_minutes = float(self.model.predict(X)[0])
        
        # Calculate new ETA
        current_time = pd.to_datetime(data_point['timestamp'])
        new_eta = current_time + pd.Timedelta(minutes=predicted_minutes)
        
        # Calculate the difference from initial ETA
        initial_eta = pd.to_datetime(data_point['initial_planned_eta'])
        eta_difference = (new_eta - initial_eta).total_seconds() / 60  # minutes
        
        return {
            'driver_id': data_point['driver_id'],
            'current_time': current_time.isoformat(),
            'initial_eta': initial_eta.isoformat(),
            'predicted_eta': new_eta.isoformat(),
            'predicted_remaining_minutes': predicted_minutes,
            'eta_difference_minutes': eta_difference,
            'current_status': data_point['status'],
            'current_speed_mph': data_point['current_speed_mph'],
            'distance_remaining_mi': geodesic(
                (data_point['current_latitude'], data_point['current_longitude']),
                (data_point['destination_latitude'], data_point['destination_longitude'])
            ).miles
        }

def monitor_delivery_data(predictor: ETAPredictor, data_file: str = 'delivery_data.jsonl'):
    """Monitor the delivery data file for new records and generate predictions."""
    print(f"Monitoring {data_file} for new delivery updates...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Check if file exists
            if not os.path.exists(data_file):
                time.sleep(1)
                continue
            
            # Read new data points
            with open(data_file, 'r') as f:
                # Seek to last processed position
                f.seek(predictor.last_processed_position)
                
                # Process new lines
                for line in f:
                    data_point = json.loads(line)
                    prediction = predictor.predict_eta(data_point)
                    
                    # Format and print prediction
                    eta_diff = prediction['eta_difference_minutes']
                    eta_status = "ON TIME" if abs(eta_diff) < 5 else "DELAYED" if eta_diff > 0 else "EARLY"
                    
                    print(f"[{prediction['current_time']}] Driver {prediction['driver_id']} | "
                          f"Status: {prediction['current_status']:>13} | "
                          f"Distance: {prediction['distance_remaining_mi']:>6.2f} mi | "
                          f"Speed: {prediction['current_speed_mph']:>6.1f} mph")
                    print(f"    Initial ETA: {prediction['initial_eta']}")
                    print(f"    Updated ETA: {prediction['predicted_eta']} "
                          f"({abs(eta_diff):.1f} min {'later' if eta_diff > 0 else 'earlier'} | {eta_status})")
                    print("-" * 100)
                
                # Update last processed position
                predictor.last_processed_position = f.tell()
            
            # Sleep briefly before next check
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopped monitoring delivery data.")

def main():
    predictor = ETAPredictor()
    monitor_delivery_data(predictor)

if __name__ == "__main__":
    main() 