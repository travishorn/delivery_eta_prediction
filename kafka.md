## Kafka

### One-time Setup

Download Kafka from https://www.apache.org/dyn/closer.cgi?path=/kafka/4.0.0/kafka_2.13-4.0.0.tgz

Unzip it:

```bash
tar -xzf kafka_2.13-4.0.0
```

Change into the Kafka directory:

```bash
cd kafka_2.13-4.0.0
```

Create a directory to store the logs:

```bash
mkdir kraft-combined-logs
```

Edit `config/server.properties`. Change the line...

```
log.dirs=/tmp/kraft-combined-logs
```

to...

```
log.dirs=/home/[your_username]/kafka_2.13-4.0.0/kraft-combined-logs
```

If you'll be streaming events to/from a remote machine, you'll also need to
chage the line...

```
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
```

to...

```
advertised.listeners=PLAINTEXT://[ip_address_of_this_machine]:9092,CONTROLLER://localhost:9093
```

Generate a new cluster ID:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

Format the log directories

```bash
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

Add as many topics as you need. You can always add more later, too.

### Using Kafka

Start the server:

```bash
bin/kafka-server-start.sh config/server.properties
```

Leave this terminal session open. You must do this every time you want to use
Kafka.

Create a topic:

```bash
bin/kafka-topics.sh --create --topic [name_of_topic] --bootstrap-server localhost:9092
```

You only need to do this once per topic you'll be streaming events from/to. You
can always add more later, too.

#### Manually Read Events in the Console

You can read events as they come in:

```bash
bin/kafka-console-consumer.sh --topic [name_of_topic] --bootstrap-server localhost:9092
```

No events will be streaming in unless you already have something producing
events towards your topic. Read the next section to learn how to manually
produce events in the console.

Press `Ctrl-C` at any time to stop consuming events.

### Manually Producing Events from the Console

Run the console producer:

```bash
bin/kafka-console-producer.sh --topic [name_of_topic] --bootstrap-server localhost:9092
```

Manually type in any event like "This is my first event" and press Enter.

If you have the consumer from the last section reading events, you can see them
appear in that terminal as they come in.

Press `Ctrl-C` to stop the producer at any time.
