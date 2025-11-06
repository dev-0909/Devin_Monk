"""
Kafka Streaming Job for Cab Ride Data Processing.
Reads from Kafka topic 'cab_stream', processes ride data, resolves dimensions,
and inserts into MonkDB fact and dimension tables.
"""

import sys
import types
import os
import yaml
from typing import Dict, Any, List
from dotenv import load_dotenv
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, when, unix_timestamp, lit,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# ✅ Patch distutils.version.LooseVersion if missing (Spark 3.5 fix)
try:
    from distutils.version import LooseVersion  # noqa: F401
except ImportError:
    import packaging.version
    sys.modules['distutils'] = types.ModuleType('distutils')
    sys.modules['distutils.version'] = types.ModuleType('distutils.version')
    sys.modules['distutils.version'].LooseVersion = packaging.version.Version

from spark.libs.utils import (
    generate_time_key, compute_duration, geohash,
    validate_coordinates
)
from spark.libs.monk_client import (
    get_monk_connection, upsert_dimension,
    get_dimension_key, bulk_upsert_facts
)

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class KafkaStreamingProcessor:
    """Kafka streaming processor for cab ride data."""

    def __init__(self, config_path: str = "config/streaming.yaml"):
        """Initialize the streaming processor with configuration."""
        self.config = self._load_config(config_path)
        self.spark = None
        self.schema = self._build_schema()

        logger.info("Kafka streaming processor initialized", config_path=config_path)

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Substitute environment variables
            config = self._substitute_env_vars(config)

            logger.info("Configuration loaded successfully", config_path=config_path)
            return config

        except Exception as e:
            logger.error("Failed to load configuration", config_path=config_path, error=str(e))
            raise

    def _substitute_env_vars(self, obj: Any) -> Any:
        """Recursively substitute environment variables in configuration."""
        if isinstance(obj, dict):
            return {k: self._substitute_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            # Extract env var with default: ${VAR_NAME:default_value}
            env_expr = obj[2:-1]  # Remove ${ and }
            if ':' in env_expr:
                var_name, default_value = env_expr.split(':', 1)
                return os.getenv(var_name, default_value)
            else:
                return os.getenv(env_expr, obj)
        else:
            return obj

    def _build_schema(self) -> StructType:
        """Build Spark schema for incoming Kafka messages."""
        return StructType([
            StructField("ride_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("pickup_ts", StringType(), False),
            StructField("dropoff_ts", StringType(), True),
            StructField("pickup_lat", DoubleType(), False),
            StructField("pickup_lon", DoubleType(), False),
            StructField("dropoff_lat", DoubleType(), True),
            StructField("dropoff_lon", DoubleType(), True),
            StructField("distance_km", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True)
        ])

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        try:
            spark_config = self.config['spark']

            builder = SparkSession.builder.appName(spark_config['app_name'])

            # Add Kafka package
            for package in spark_config.get('packages', []):
                builder = builder.config("spark.jars.packages", package)

            # Add additional Spark configurations
            for key, value in spark_config.get('config', {}).items():
                builder = builder.config(key, value)

            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

            logger.info("Spark session created successfully",
                       app_name=spark_config['app_name'])

            return self.spark

        except Exception as e:
            logger.error("Failed to create Spark session", error=str(e))
            raise

    def read_kafka_stream(self) -> DataFrame:
        """Read streaming data from Kafka."""
        try:
            kafka_config = self.config['kafka']

            kafka_df = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
                .option("subscribe", kafka_config['topics']['cab_stream'])
                .option("startingOffsets", kafka_config['consumer']['auto_offset_reset'])
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", kafka_config['consumer']['max_poll_records'])
                .load()
            )

            logger.info("Kafka stream configured",
                       topic=kafka_config['topics']['cab_stream'],
                       bootstrap_servers=kafka_config['bootstrap_servers'])

            return kafka_df

        except Exception as e:
            logger.error("Failed to configure Kafka stream", error=str(e))
            raise

    def parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON messages from Kafka and apply schema."""
        try:
            # Parse JSON from Kafka value
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), self.schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset")
            ).select("data.*", "kafka_timestamp", "partition", "offset")

            # Add derived fields
            enriched_df = self._add_derived_fields(parsed_df)

            # Apply data validation and cleansing
            cleaned_df = self._clean_and_validate(enriched_df)

            logger.info("Kafka messages parsed and enriched")
            return cleaned_df

        except Exception as e:
            logger.error("Failed to parse Kafka messages", error=str(e))
            raise

    def _add_derived_fields(self, df: DataFrame) -> DataFrame:
        """Add derived fields to the DataFrame."""
        try:
            # Convert timestamp strings to timestamp type
            df = df.withColumn("pickup_timestamp",
                              unix_timestamp(col("pickup_ts"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

            df = df.withColumn("dropoff_timestamp",
                              when(col("dropoff_ts").isNotNull(),
                                   unix_timestamp(col("dropoff_ts"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
                              .otherwise(None))

            # Add time keys using UDF
            from pyspark.sql.functions import udf

            time_key_udf = udf(lambda ts: generate_time_key(ts) if ts else None, IntegerType())

            df = df.withColumn("pickup_time_key", time_key_udf(col("pickup_timestamp")))
            df = df.withColumn("dropoff_time_key", time_key_udf(col("dropoff_timestamp")))

            # Add duration calculation
            duration_udf = udf(lambda pickup, dropoff:
                              compute_duration(pickup, dropoff) if pickup and dropoff else None,
                              IntegerType())

            df = df.withColumn("duration_min", duration_udf(col("pickup_timestamp"), col("dropoff_timestamp")))

            # Add geohash fields
            geohash_udf = udf(lambda lat, lon:
                             geohash(lat, lon, 7) if lat and lon and validate_coordinates(lat, lon) else None,
                             StringType())

            df = df.withColumn("pickup_geohash", geohash_udf(col("pickup_lat"), col("pickup_lon")))
            df = df.withColumn("dropoff_geohash", geohash_udf(col("dropoff_lat"), col("dropoff_lon")))

            # Add processing metadata
            df = df.withColumn("processing_timestamp", current_timestamp())
            df = df.withColumn("source", lit("kafka_stream"))

            return df

        except Exception as e:
            logger.error("Failed to add derived fields", error=str(e))
            raise

    def _clean_and_validate(self, df: DataFrame) -> DataFrame:
        """Apply data validation and cleansing rules."""
        try:
            validation_config = self.config['processing']['validation']

            # Filter out records with missing required fields
            for field in validation_config['required_fields']:
                df = df.filter(col(field).isNotNull() & (col(field) != ""))

            # Validate coordinate bounds
            bounds = validation_config['coordinate_bounds']
            df = df.filter(
                (col("pickup_lat") >= bounds['lat_min']) &
                (col("pickup_lat") <= bounds['lat_max']) &
                (col("pickup_lon") >= bounds['lon_min']) &
                (col("pickup_lon") <= bounds['lon_max'])
            )

            # Validate fare bounds
            fare_bounds = validation_config['fare_bounds']
            df = df.filter(
                (col("fare_amount").isNull()) |
                ((col("fare_amount") >= fare_bounds['min_fare']) &
                 (col("fare_amount") <= fare_bounds['max_fare']))
            )

            # Validate distance bounds
            distance_bounds = validation_config['distance_bounds']
            df = df.filter(
                (col("distance_km").isNull()) |
                ((col("distance_km") >= distance_bounds['min_distance']) &
                 (col("distance_km") <= distance_bounds['max_distance']))
            )

            logger.debug("Data validation and cleansing applied")
            return df

        except Exception as e:
            logger.error("Failed to clean and validate data", error=str(e))
            raise

    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        """Process a batch of streaming data."""
        try:
            if df.isEmpty():
                logger.info("Empty batch received", batch_id=batch_id)
                return

            # Convert to Pandas for MonkDB processing
            pdf = df.toPandas()
            records = pdf.to_dict('records')

            logger.info("Processing batch", batch_id=batch_id, record_count=len(records))

            with get_monk_connection() as conn:
                cursor = conn.cursor()

                # Process dimensions first
                self._process_dimensions(cursor, records)

                # Process facts
                self._process_facts(cursor, records)

            logger.info("Batch processed successfully",
                       batch_id=batch_id,
                       record_count=len(records))

        except Exception as e:
            logger.error("Failed to process batch",
                        batch_id=batch_id,
                        error=str(e))
            raise

    def _process_dimensions(self, cursor, records: List[Dict[str, Any]]) -> None:
        """Process and upsert dimension records."""
        try:
            monkdb_config = self.config['monkdb']

            # Process driver dimension
            drivers = {}
            for record in records:
                driver_id = record.get('driver_id')
                if driver_id and driver_id not in drivers:
                    drivers[driver_id] = {
                        'driver_id': driver_id,
                        'driver_name': f"Driver_{driver_id}",  # Placeholder
                        'license_number': None,
                        'phone_number': None,
                        'email': None,
                        'status': 'active'
                    }

            for driver_data in drivers.values():
                upsert_dimension(cursor, monkdb_config['tables']['dim_driver'],
                               'driver_id', driver_data, scd_type=2)

            # Process vehicle dimension
            vehicles = {}
            for record in records:
                vehicle_id = record.get('vehicle_id')
                if vehicle_id and vehicle_id not in vehicles:
                    vehicles[vehicle_id] = {
                        'vehicle_id': vehicle_id,
                        'make': None,
                        'model': None,
                        'year': None,
                        'license_plate': None,
                        'color': None,
                        'status': 'active'
                    }

            for vehicle_data in vehicles.values():
                upsert_dimension(cursor, monkdb_config['tables']['dim_vehicle'],
                               'vehicle_id', vehicle_data, scd_type=2)

            # Process geolocation dimension
            geolocations = {}
            for record in records:
                # Pickup location
                pickup_geohash = record.get('pickup_geohash')
                if pickup_geohash and pickup_geohash not in geolocations:
                    geolocations[pickup_geohash] = {
                        'geohash': pickup_geohash,
                        'latitude': record.get('pickup_lat'),
                        'longitude': record.get('pickup_lon'),
                        'city': None,
                        'state': None,
                        'country': None,
                        'location_type': 'pickup'
                    }

                # Dropoff location
                dropoff_geohash = record.get('dropoff_geohash')
                if dropoff_geohash and dropoff_geohash not in geolocations:
                    geolocations[dropoff_geohash] = {
                        'geohash': dropoff_geohash,
                        'latitude': record.get('dropoff_lat'),
                        'longitude': record.get('dropoff_lon'),
                        'city': None,
                        'state': None,
                        'country': None,
                        'location_type': 'dropoff'
                    }

            for geo_data in geolocations.values():
                upsert_dimension(cursor, monkdb_config['tables']['dim_geolocation'],
                               'geohash', geo_data, scd_type=1)

            logger.info("Dimensions processed",
                       drivers=len(drivers),
                       vehicles=len(vehicles),
                       geolocations=len(geolocations))

        except Exception as e:
            logger.error("Failed to process dimensions", error=str(e))
            raise

    def _process_facts(self, cursor, records: List[Dict[str, Any]]) -> None:
        """Process and upsert fact records."""
        try:
            monkdb_config = self.config['monkdb']

            # Prepare fact records with dimension keys
            fact_records = []
            for record in records:
                # Get dimension keys
                driver_key = get_dimension_key(cursor, monkdb_config['tables']['dim_driver'],
                                             'driver_id', record.get('driver_id'))
                vehicle_key = get_dimension_key(cursor, monkdb_config['tables']['dim_vehicle'],
                                              'vehicle_id', record.get('vehicle_id'))
                pickup_geo_key = get_dimension_key(cursor, monkdb_config['tables']['dim_geolocation'],
                                                 'geohash', record.get('pickup_geohash'))
                dropoff_geo_key = get_dimension_key(cursor, monkdb_config['tables']['dim_geolocation'],
                                                  'geohash', record.get('dropoff_geohash'))

                fact_record = {
                    'ride_id': record.get('ride_id'),
                    'driver_key': driver_key,
                    'vehicle_key': vehicle_key,
                    'pickup_geolocation_key': pickup_geo_key,
                    'dropoff_geolocation_key': dropoff_geo_key,
                    'pickup_time_key': record.get('pickup_time_key'),
                    'dropoff_time_key': record.get('dropoff_time_key'),
                    'pickup_timestamp': record.get('pickup_timestamp'),
                    'dropoff_timestamp': record.get('dropoff_timestamp'),
                    'pickup_lat': record.get('pickup_lat'),
                    'pickup_lon': record.get('pickup_lon'),
                    'dropoff_lat': record.get('dropoff_lat'),
                    'dropoff_lon': record.get('dropoff_lon'),
                    'distance_km': record.get('distance_km'),
                    'duration_min': record.get('duration_min'),
                    'fare_amount': record.get('fare_amount'),
                    'processing_timestamp': record.get('processing_timestamp'),
                    'source': record.get('source')
                }

                fact_records.append(fact_record)

            # Bulk upsert fact records
            results = bulk_upsert_facts(cursor, monkdb_config['tables']['fact_rides'],
                                      fact_records, unique_key='ride_id',
                                      batch_size=monkdb_config['batch_processing']['batch_size'])

            logger.info("Facts processed", results=results)

        except Exception as e:
            logger.error("Failed to process facts", error=str(e))
            raise

    def start_streaming(self) -> None:
        """Start the Kafka streaming job."""
        try:
            logger.info("Starting Kafka streaming job")

            # Create Spark session
            self.create_spark_session()

            # Read from Kafka
            kafka_df = self.read_kafka_stream()

            # Parse and enrich data
            processed_df = self.parse_kafka_messages(kafka_df)

            # Start streaming query
            streaming_config = self.config['spark']['streaming']

            query = (
                processed_df.writeStream
                .foreachBatch(self.process_batch)
                .outputMode("append")
                .trigger(processingTime=streaming_config['trigger_interval'])
                .option("checkpointLocation",
                       self.config['spark']['config']['spark.sql.streaming.checkpointLocation'])
                .start()
            )

            logger.info("Streaming job started successfully")

            # Wait for termination
            query.awaitTermination()

        except Exception as e:
            logger.error("Streaming job failed", error=str(e))
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point for the streaming job."""
    try:
        # Initialize processor
        processor = KafkaStreamingProcessor()

        # Start streaming
        processor.start_streaming()

    except KeyboardInterrupt:
        logger.info("Streaming job interrupted by user")
    except Exception as e:
        logger.error("Streaming job failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
