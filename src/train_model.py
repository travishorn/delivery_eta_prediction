import json
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from typing import Dict, List, Tuple
from geopy.distance import geodesic
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'driver_updates')

if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("Kafka bootstrap servers must be provided via KAFKA_BOOTSTRAP_SERVERS environment variable")

def load_and_preprocess_data() -> pd.DataFrame:
    """Load and preprocess historical data from Kafka into a pandas DataFrame."""
    print("Reading historical data from Kafka...")
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    
    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',  # Start from the beginning of the topic
            enable_auto_commit=False,  # Disable auto-commit to ensure we read all messages
            group_id=None,  # Don't use a consumer group to ensure we can read from beginning
            consumer_timeout_ms=10000,  # Timeout after 10 seconds if no messages
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Check topic partitions
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            raise ValueError(f"Topic {KAFKA_TOPIC} does not exist or is empty")
        print(f"Found {len(partitions)} partitions for topic {KAFKA_TOPIC}")
        
        # Get the end offset for the topic
        consumer.poll(timeout_ms=1000)  # This is needed to get the partitions assigned
        partitions = consumer.assignment()
        end_offsets = consumer.end_offsets(partitions)
        
        # Print partition information
        for partition in partitions:
            start_offset = consumer.beginning_offsets([partition])[partition]
            end_offset = end_offsets[partition]
            print(f"Partition {partition}: messages {start_offset} to {end_offset}")
            
        if all(start == end for start, end in zip(consumer.beginning_offsets(partitions).values(), end_offsets.values())):
            raise ValueError(f"Topic {KAFKA_TOPIC} is empty")
            
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        raise
    
    # Collect all messages
    data = []
    try:
        # Read all messages
        for message in consumer:
            try:
                # Print the first message to debug structure
                if len(data) == 0:
                    print("First message structure:", message.value)
                
                # Validate required fields
                required_fields = [
                    'timestamp', 'driver_id', 'current_latitude', 
                    'current_longitude', 'current_speed_mph',
                    'destination_latitude', 'destination_longitude',
                    'status', 'traffic_level', 'weather',
                    'initial_planned_eta'
                ]
                
                # Check if all required fields are present
                missing_fields = [field for field in required_fields if field not in message.value]
                if missing_fields:
                    print(f"Warning: Message missing required fields: {missing_fields}")
                    continue
                    
                data.append(message.value)
            except Exception as e:
                print(f"Error processing message: {e}")
                print(f"Message content: {message.value}")
                continue
                
    finally:
        consumer.close()
    
    if not data:
        raise ValueError("No valid messages were collected from Kafka. Please check the message structure and Kafka topic.")
    
    print(f"Collected {len(data)} historical messages")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Convert timestamp and initial_planned_eta to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['initial_planned_eta'] = pd.to_datetime(df['initial_planned_eta'])
    
    # Extract time-based features
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    
    # Calculate time-based metrics
    df['time_until_planned_eta'] = (df['initial_planned_eta'] - df['timestamp']).dt.total_seconds() / 60  # in minutes
    
    # Calculate distance remaining
    def calculate_distance(row: pd.Series) -> float:
        return geodesic(
            (row['current_latitude'], row['current_longitude']),
            (row['destination_latitude'], row['destination_longitude'])
        ).miles
    
    df['distance_remaining_mi'] = df.apply(calculate_distance, axis=1)
    
    # Encode categorical variables
    label_encoders = {}
    for col in ['status', 'traffic_level', 'weather']:
        label_encoders[col] = LabelEncoder()
        df[f'{col}_encoded'] = label_encoders[col].fit_transform(df[col])
    
    return df, label_encoders

def prepare_features_and_target(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """Prepare feature matrix X and target variable y."""
    features = [
        'distance_remaining_mi',
        'current_speed_mph',
        'hour_of_day',
        'day_of_week',
        'status_encoded',
        'traffic_level_encoded',
        'weather_encoded',
        'time_until_planned_eta'
    ]
    
    X = df[features]
    
    # Target variable: actual remaining time (minutes) to destination
    # Group by driver_id and calculate the time difference to the last record
    df['next_timestamp'] = df.groupby('driver_id')['timestamp'].shift(-1)
    df['actual_remaining_time'] = df.groupby('driver_id')['next_timestamp'].transform('last')
    df['actual_remaining_minutes'] = (df['actual_remaining_time'] - df['timestamp']).dt.total_seconds() / 60
    
    y = df['actual_remaining_minutes']
    
    # Remove any invalid targets (e.g., last points in each trip)
    mask = y.notna()
    
    return X[mask], y[mask]

def train_and_evaluate_model(X: pd.DataFrame, y: pd.Series) -> Tuple[RandomForestRegressor, Dict]:
    """Train the model and evaluate its performance."""
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize and train the model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    metrics = {
        'mean_absolute_error': mean_absolute_error(y_test, y_pred),
        'root_mean_squared_error': np.sqrt(mean_squared_error(y_test, y_pred)),
        'r2_score': r2_score(y_test, y_pred)
    }
    
    return model, metrics

def main():
    # Load and preprocess the data
    print("Loading and preprocessing historical data...")
    df, label_encoders = load_and_preprocess_data()
    
    # Prepare features and target
    print("Preparing features and target...")
    X, y = prepare_features_and_target(df)
    
    # Train and evaluate the model
    print("Training model...")
    model, metrics = train_and_evaluate_model(X, y)
    
    # Print metrics
    print("\nModel Performance:")
    print(f"Mean Absolute Error: {metrics['mean_absolute_error']:.2f} minutes")
    print(f"Root Mean Squared Error: {metrics['root_mean_squared_error']:.2f} minutes")
    print(f"R² Score: {metrics['r2_score']:.3f}")
    
    # Print feature importance
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nFeature Importance:")
    print(feature_importance)
    
    # Save the model and label encoders
    print("\nSaving model and encoders...")
    joblib.dump(model, 'eta_predictor_model.joblib')
    joblib.dump(label_encoders, 'label_encoders.joblib')
    print("Model and encoders saved successfully!")

if __name__ == "__main__":
    main() 