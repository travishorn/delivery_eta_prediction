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

def load_and_preprocess_data(file_path: str) -> pd.DataFrame:
    """Load and preprocess the JSONL data into a pandas DataFrame."""
    # Read JSONL file line by line
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    
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
    import argparse
    
    print("Loading and preprocessing data...")
    df, label_encoders = load_and_preprocess_data('delivery_data.jsonl')
    
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