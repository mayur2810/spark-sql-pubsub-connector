# Spark-SQL Pub/Sub Connector

A library for reading and writing data to/from Google Cloud Pub/Sub in [Apache Spark](https://spark.apache.org/)'s Structured Streaming.

This connector implements the Data Source V2 API and is designed for use in micro-batch streaming queries.

## Features

- **Pub/Sub Source**: Reads messages from a Pub/Sub subscription.
- **Pub/Sub Sink**: Writes messages to a Pub/Sub topic.
- **Schema Support**: Provides a fixed schema for Pub/Sub messages, including data, attributes, and metadata.
- **Dynamic Partitioning**: (Source) Automatically adjusts the number of partitions based on the backlog of messages in the subscription.
- **Ordering Keys**: (Sink) Supports specifying an ordering key for messages.
- **Emulator Support**: Can be configured to work with the Pub/Sub emulator for local development and testing.

## Authentication

This connector authenticates with Google Cloud Pub/Sub using [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials).

For the connector to authenticate, you need to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of a service account key file that has the necessary Pub/Sub permissions (e.g., Pub/Sub Editor, Pub/Sub Viewer).

When running a Spark application, ensure that this environment variable is available on all Spark executors.

## How to Build

You can build the connector from the source by running the following Maven command:

```bash
mvn clean package
```

This will produce a JAR file in the `target/` directory, for example `spark-sql-pubsub-connector-1.0.0.jar`.

## Usage

You can use the connector by including the JAR file in your Spark application's classpath.

### Reading from Pub/Sub (Source)

Here's an example of how to create a streaming DataFrame that reads from a Pub/Sub subscription:

```scala
val spark = SparkSession.builder()
  .appName("PubsubSourceExample")
  .master("local[*]")
  .getOrCreate()

val pubsubStream = spark.readStream
  .format("pubsub")
  .option("project_id", "your-gcp-project-id")
  .option("subscription", "your-pubsub-subscription")
  .load()

// The schema of pubsubStream is:
// root
//  |-- subscription: string (nullable = false)
//  |-- ack_id: string (nullable = false)
//  |-- message_id: string (nullable = false)
//  |-- ordering_key: string (nullable = false)
//  |-- data: binary (nullable = false)
//  |-- publish_timestamp: timestamp (nullable = false)
//  |-- attributes: map (nullable = true)
//  |    |-- key: string
//  |    |-- value: string (valueContainsNull = true)

val query = pubsubStream
  .selectExpr("CAST(data AS STRING)")
  .writeStream
  .format("console")
  .start()

query.awaitTermination()
```

### Writing to Pub/Sub (Sink)

Here's an example of how to write a streaming DataFrame to a Pub/Sub topic:

```scala
val spark = SparkSession.builder()
  .appName("PubsubSinkExample")
  .master("local[*]")
  .getOrCreate()

// Create a sample DataFrame to write to Pub/Sub
val data = Seq("message1", "message2", "message3").toDF("value")

val query = data
  .selectExpr("CAST(value AS BINARY) as data", "map() as attributes") // Schema must match
  .writeStream
  .format("pubsub")
  .option("project_id", "your-gcp-project-id")
  .option("topic", "your-pubsub-topic")
  .option("checkpointLocation", "/tmp/pubsub_sink_checkpoint") // Checkpoint is required for sinks
  .start()

query.awaitTermination()
```

## Configuration Options

### Source Options

| Option                        | Required | Default | Description                                                                                              |
| ----------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `project_id`                  | Yes      | -       | The Google Cloud Project ID.                                                                             |
| `subscription`                | Yes      | -       | The name of the Pub/Sub subscription to read from.                                                       |
| `num_partitions`              | No       | 4       | The number of partitions to use for reading from the subscription.                                       |
| `max_messages_per_partition`  | No       | 1000    | The maximum number of messages to pull per partition in a single batch.                                  |
| `dynamic_partitioning`        | No       | true    | If `true`, the connector will dynamically adjust the number of partitions based on subscription backlog. |
| `endpoint`                    | No       | -       | The endpoint to connect to, e.g., `localhost:8085` for the emulator.                                     |

### Sink Options

| Option             | Required | Default | Description                                                                    |
| ------------------ | -------- | ------- | ------------------------------------------------------------------------------ |
| `project_id`       | Yes      | -       | The Google Cloud Project ID.                                                   |
| `topic`            | Yes      | -       | The name of the Pub/Sub topic to write to.                                     |
| `ordering_key`     | No       | -       | The column name to use as the ordering key. The column must be of `StringType`.|
| `endpoint`         | No       | -       | The endpoint to connect to, e.g., `localhost:8085` for the emulator.           |
| `checkpointLocation`| Yes      | -       | The path to a directory for checkpointing the streaming query.                 |

## Design and Implementation Details

While Apache Spark Structured Streaming is based on offset-based processing, the Pub/Sub connector manages logical offsets internally as Pub/Sub is not an offset-based messaging system. It relies on Pub/Sub's message acknowledgment mechanism to resend messages that failed to be processed.

For reproducibility of partitions, the connector stores the partitions as cached RDDs in the Spark application, and any future re-references to the partitions in the lineage will use the cached RDDs.

For this reason, the connector does not allow the following operations on the Streaming DataFrame:
1.  The same Pub/Sub subscription cannot be used in multiple streams.
2.  Splitting a stream into multiple streams is not allowed. A workaround is to use `foreachBatch` to process the stream and write to multiple sinks.

## Testing

The project includes both unit and integration tests.

### Running Unit Tests

To run only the unit tests, use the following command:

```bash
mvn test
```

### Running Integration Tests

The integration tests use the Pub/Sub emulator running in a Docker container. Make sure you have Docker installed and running.

To run the integration tests:

```bash
mvn verify
```
This command will:
1. Start the Pub/Sub emulator in a Docker container.
2. Run the integration tests against the emulator.
3. Stop the emulator container.

If you want to run all tests (unit and integration), you can also use `mvn verify`.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).