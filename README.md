# Delivery ETA Prediction System

A real-time ML proof-of-concept (POC) project for predicting and adjusting
last-mile delivery ETAs based on dynamic conditions. The system continuously
monitors ongoing deliveries and updates ETAs based on current conditions,
traffic, and other factors.

## Project Components

1. **Real-Time Data Simulator** (`src/simulator.py`)

   - Simulates multiple delivery drivers moving towards their destinations
   - Generates realistic mock data including location updates, traffic
     conditions, and weather
   - Handles random events like traffic delays and stops
   - Supports time acceleration for quick data generation
   - Outputs data in JSONL format for real-time consumption

2. **ML Model Training** (`src/train_model.py`)

   - Processes historical delivery data to train an ETA prediction model
   - Features include:
     - Distance remaining
     - Current speed
     - Time of day and day of week
     - Traffic conditions
     - Weather conditions
     - Driver status
   - Uses Random Forest Regression for accurate predictions
   - Outputs trained model and encoders for real-time use

3. **Real-Time Prediction Service** (`src/predictor.py`)
   - Consumes live data from the simulator
   - Uses trained model to generate updated ETAs
   - Provides real-time status updates with:
     - Current location and speed
     - Initial vs. predicted ETA
     - Delivery status (On Time/Early/Delayed)

## Setup

1. Create a Python virtual environment (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the System

### Prerequisites

You must have a Kafka server already running with a topic called
`driver_updates`.

Set the `KAFKA_BOOTSTRAP_SERVERS` environment variable. You can do it in the
terminal or create a `.env` file with the following contents.

```
KAFKA_BOOTSTRAP_SERVERS=[ip_address_of_server]:9092
```

If your topic is called something other than `driver_updates`, you can set an
environment variable called `KAFKA_TOPIC`, as well.

### 1. Generate Training Data

First, run the simulator to generate training data:

```bash
python src/simulator.py
```

The simulator will:

- Create 3 drivers with random start/end locations
- Generate data and stream it to your Kafka server
- Display real-time status updates

You may consider setting `time_acceleration_factor=60.0` to run at 60x speed (1
minute of simulation time per real second) or even higher, for faster generation
of simulation data that can be used to train the model.

Let it run for a while to generate sufficient training data. It will
automatically stop when all 3 deliveries are complete. Run the simulation again
and again to generate more simulated data, which will further improve the
model's performance.

### 2. Train the ML Model

Train the prediction model on the generated data:

```bash
python src/train_model.py
```

This will:

- Load and process the training data from the Kafka server
- Train a Random Forest model
- Display performance metrics
- Save the model (`eta_predictor_model.joblib`) and encoders
  (`label_encoders.joblib`)

### 3. Run the Complete System

Open two terminal windows:

Terminal 1 - Start the simulator:

```bash
python src/simulator.py
```

You should probably set `time_acceleration_factor=1.0` to run at 1x speed, to
better simulate using the prediction model in real-time.

Terminal 2 - Start the prediction service:

```bash
python src/predictor.py
```

The prediction service will:

- Monitor the simulator's output in real-time
- Generate updated ETAs for each delivery
- Display status updates showing:
  ```
  [2025-04-11T14:30:15Z] Driver D1 | Status:     EN_ROUTE | Distance:   2.34 mi | Speed:  40.0 mph
      Initial ETA: 2025-04-11T15:00:00Z
      Updated ETA: 2025-04-11T15:45:00Z (45.0 min later | DELAYED)
  ```

## Data Format

The simulator generates data points in JSON format and streams them to Kafka:

```json
{
  "timestamp": "2025-04-11T14:30:15Z",
  "driver_id": "D123",
  "current_latitude": 38.75,
  "current_longitude": -90.35,
  "current_speed_mph": 45,
  "destination_latitude": 38.85,
  "destination_longitude": -90.45,
  "status": "EN_ROUTE",
  "traffic_level": "MEDIUM",
  "weather": "CLEAR",
  "initial_planned_eta": "2025-04-11T15:30:15Z"
}
```

## Project Structure

```
delivery_eta_prediction/
├── .gitignore
├── README.md
├── requirements.txt
├── src/
│   ├── simulator.py      # Real-time delivery simulator
│   ├── train_model.py    # ML model training script
│   └── predictor.py      # Real-time prediction service
├── eta_predictor_model.joblib    # Trained ML model
└── label_encoders.joblib         # Feature encoders
```

## License

The MIT License

Copyright 2025 Travis Horn

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
