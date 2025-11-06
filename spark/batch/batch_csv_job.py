"""
Batch CSV Processing Job for Cab Ride Data.
Reads CSV files from /data/batch/rides_incoming/, deduplicates on ride_id,
and upserts dimensions and facts using MonkDB utilities.
"""

import sys
import types
import os
import yaml
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, trim, lower, unix_timestamp, lit, current_timestamp,
    row_number, desc, asc
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType,
    IntegerType, TimestampType
)
from pyspark.sql.window import Window

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# ✅ Patch distutils.version.LooseVersion if missing (Spark 3.5 fix)
try:
    from distutils.version import LooseVersion  # noqa: F401
except ImportError:
    import packaging.version
    sys.modules['distutils'] = types.ModuleType('distutils')
    sys.modules['distutils.version'] = types.ModuleType('distutils.version')
    setattr(sys.modules['distutils.version'], 'LooseVersion', packaging.version.Version)

from spark.libs.utils import (  # noqa: E402
    generate_time_key, compute_duration, geohash,
    validate_coordinates, normalize_ride_id
)
from spark.libs.monk_client import (  # noqa: E402
    get_monk_connection, bulk_insert, upsert_dimension,
    get_dimension_key, bulk_upsert_facts
)

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


class BatchCSVProcessor:
    """Batch CSV processor for cab ride data."""

    def __init__(self, config_path: str = "config/batch.yaml"):
        """Initialize the batch processor with configuration."""
        self.config = self._load_config(config_path)
        self.spark: Optional[SparkSession] = None
        self.batch_id = self._generate_batch_id()
        self.processing_stats = {
            'files_processed': 0,
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'duplicate_records': 0,
            'inserted_records': 0,
            'updated_records': 0,
            'start_time': datetime.now(timezone.utc),
            'end_time': None
        }

        logger.info("Batch CSV processor initialized",
                   batch_id=self.batch_id,
                   config_path=config_path)

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

    def _generate_batch_id(self) -> str:
        """Generate unique batch ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"batch_{timestamp}_{unique_id}"

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        try:
            spark_config = self.config['spark']

            builder = SparkSession.builder.appName(spark_config['app_name'])

            # Add Spark configurations
            for key, value in spark_config.get('config', {}).items():
                builder = builder.config(key, value)

            # Memory configurations
            memory_config = spark_config.get('memory', {})
            for key, value in memory_config.items():
                if key == 'executor_memory':
                    builder = builder.config("spark.executor.memory", value)
                elif key == 'driver_memory':
                    builder = builder.config("spark.driver.memory", value)
                elif key == 'executor_cores':
                    builder = builder.config("spark.executor.cores", value)
                elif key == 'max_result_size':
                    builder = builder.config("spark.driver.maxResultSize", value)

            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

            logger.info("Spark session created successfully",
                       app_name=spark_config['app_name'],
                       batch_id=self.batch_id)

            return self.spark

        except Exception as e:
            logger.error("Failed to create Spark session", error=str(e))
            raise

    def discover_input_files(self) -> List[str]:
        """Discover CSV files in the input directory."""
        try:
            input_config = self.config['data_sources']['input']
            base_path = Path(input_config['base_path'])
            file_pattern = input_config['file_pattern']

            if not base_path.exists():
                logger.warning("Input directory does not exist", path=str(base_path))
                return []

            # Find CSV files matching pattern
            csv_files = list(base_path.glob(file_pattern))
            csv_file_paths = [str(f) for f in csv_files]

            logger.info("Discovered input files",
                       file_count=len(csv_file_paths),
                       base_path=str(base_path),
                       batch_id=self.batch_id)

            return csv_file_paths

        except Exception as e:
            logger.error("Failed to discover input files", error=str(e))
            raise

    def read_csv_files(self, file_paths: List[str]) -> DataFrame:
        """Read CSV files into a Spark DataFrame."""
        try:
            if self.spark is None:
                raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")
                
            if not file_paths:
                logger.info("No CSV files to process")
                return self.spark.createDataFrame([], StructType([]))

            input_config = self.config['data_sources']['input']

            # Read CSV files with configuration
            df = (
                self.spark.read
                .option("header", input_config['header'])
                .option("delimiter", input_config['delimiter'])
                .option("quote", input_config['quote'])
                .option("escape", input_config['escape'])
                .option("nullValue", input_config['null_value'])
                .option("dateFormat", input_config['date_format'])
                .option("timestampFormat", input_config['timestamp_format'])
                .option("multiline", input_config['multiline'])
                .option("encoding", input_config['encoding'])
                .option("inferSchema", "false")  # Use explicit schema
                .csv(file_paths)
            )

            # Add file metadata
            df = df.withColumn("batch_id", lit(self.batch_id))
            df = df.withColumn("processing_timestamp", current_timestamp())

            record_count = df.count()
            self.processing_stats['total_records'] = record_count
            self.processing_stats['files_processed'] = len(file_paths)

            logger.info("CSV files loaded successfully",
                       file_count=len(file_paths),
                       record_count=record_count,
                       batch_id=self.batch_id)

            return df

        except Exception as e:
            logger.error("Failed to read CSV files", error=str(e))
            raise

    def clean_and_transform(self, df: DataFrame) -> DataFrame:
        """Clean and transform the raw CSV data."""
        try:
            logger.info("Starting data cleaning and transformation", batch_id=self.batch_id)

            # Apply cleansing rules
            df = self._apply_cleansing_rules(df)

            # Standardize data types
            df = self._standardize_data_types(df)

            # Add derived fields
            df = self._add_derived_fields(df)

            # Apply validation rules
            df = self._apply_validation_rules(df)

            # Deduplicate records
            df = self._deduplicate_records(df)

            valid_count = df.count()
            self.processing_stats['valid_records'] = valid_count
            total_records = self.processing_stats['total_records']
            if isinstance(total_records, int):
                self.processing_stats['invalid_records'] = total_records - int(valid_count)
            else:
                self.processing_stats['invalid_records'] = 0

            logger.info("Data cleaning and transformation completed",
                       valid_records=valid_count,
                       invalid_records=self.processing_stats['invalid_records'],
                       batch_id=self.batch_id)

            return df

        except Exception as e:
            logger.error("Failed to clean and transform data", error=str(e))
            raise

    def _apply_cleansing_rules(self, df: DataFrame) -> DataFrame:
        """Apply data cleansing rules."""
        try:
            cleansing_config = self.config['processing']['cleansing']

            # Trim strings
            if cleansing_config.get('trim_strings', True):
                string_columns = [field.name for field in df.schema.fields
                                if field.dataType == StringType()]
                for col_name in string_columns:
                    df = df.withColumn(col_name, trim(col(col_name)))

            # Normalize case
            case_normalization = cleansing_config.get('normalize_case')
            if case_normalization == 'lower':
                string_columns = [field.name for field in df.schema.fields
                                if field.dataType == StringType()]
                for col_name in string_columns:
                    df = df.withColumn(col_name, lower(col(col_name)))

            # Standardize nulls
            if cleansing_config.get('standardize_nulls', True):
                null_representations = cleansing_config.get('null_representations', [])
                for col_name in df.columns:
                    for null_repr in null_representations:
                        df = df.withColumn(col_name,
                                         when(col(col_name) == null_repr, None)
                                         .otherwise(col(col_name)))

            logger.debug("Data cleansing rules applied")
            return df

        except Exception as e:
            logger.error("Failed to apply cleansing rules", error=str(e))
            raise

    def _standardize_data_types(self, df: DataFrame) -> DataFrame:
        """Standardize data types based on schema configuration."""
        try:
            # Map CSV columns to expected data types
            type_mappings = {
                'ride_id': StringType(),
                'driver_id': StringType(),
                'vehicle_id': StringType(),
                'pickup_timestamp': StringType(),  # Will be converted later
                'dropoff_timestamp': StringType(),
                'pickup_latitude': DoubleType(),
                'pickup_longitude': DoubleType(),
                'dropoff_latitude': DoubleType(),
                'dropoff_longitude': DoubleType(),
                'distance_km': DoubleType(),
                'fare_amount': DoubleType(),
                'payment_type': StringType(),
                'tip_amount': DoubleType(),
                'total_amount': DoubleType()
            }

            # Apply type conversions with error handling
            for col_name, target_type in type_mappings.items():
                if col_name in df.columns:
                    if isinstance(target_type, DoubleType):
                        # Safe conversion to double
                        df = df.withColumn(col_name,
                                         when(col(col_name).rlike(r'^-?\d+\.?\d*$'),
                                              col(col_name).cast(target_type))
                                         .otherwise(None))
                    else:
                        df = df.withColumn(col_name, col(col_name).cast(target_type))

            logger.debug("Data types standardized")
            return df

        except Exception as e:
            logger.error("Failed to standardize data types", error=str(e))
            raise

    def _add_derived_fields(self, df: DataFrame) -> DataFrame:
        """Add derived fields to the DataFrame."""
        try:
            # Convert timestamp strings to timestamp type
            df = df.withColumn("pickup_ts",
                              unix_timestamp(col("pickup_timestamp"), "yyyy-MM-dd HH:mm:ss")
                              .cast(TimestampType()))

            df = df.withColumn("dropoff_ts",
                              when(col("dropoff_timestamp").isNotNull(),
                                   unix_timestamp(col("dropoff_timestamp"), "yyyy-MM-dd HH:mm:ss")
                                   .cast(TimestampType()))
                              .otherwise(None))

            # Add time keys using UDF
            from pyspark.sql.functions import udf

            time_key_udf = udf(lambda ts: generate_time_key(ts) if ts else None, IntegerType())

            df = df.withColumn("pickup_time_key", time_key_udf(col("pickup_ts")))
            df = df.withColumn("dropoff_time_key", time_key_udf(col("dropoff_ts")))

            # Add duration calculation
            duration_udf = udf(lambda pickup, dropoff:
                              compute_duration(pickup, dropoff) if pickup and dropoff else None,
                              IntegerType())

            df = df.withColumn("duration_min", duration_udf(col("pickup_ts"), col("dropoff_ts")))

            # Add geohash fields
            geohash_udf = udf(lambda lat, lon:
                             geohash(lat, lon, 7) if lat and lon and validate_coordinates(lat, lon) else None,
                             StringType())

            df = df.withColumn("pickup_geohash",
                              geohash_udf(col("pickup_latitude"), col("pickup_longitude")))
            df = df.withColumn("dropoff_geohash",
                              geohash_udf(col("dropoff_latitude"), col("dropoff_longitude")))

            # Normalize ride ID
            normalize_ride_id_udf = udf(lambda rid: normalize_ride_id(rid) if rid else None, StringType())
            df = df.withColumn("ride_id_normalized", normalize_ride_id_udf(col("ride_id")))

            logger.debug("Derived fields added")
            return df

        except Exception as e:
            logger.error("Failed to add derived fields", error=str(e))
            raise

    def _apply_validation_rules(self, df: DataFrame) -> DataFrame:
        """Apply data validation rules."""
        try:
            validation_config = self.config['processing']['validation']

            # Filter out records with missing required fields
            for field in validation_config['required_fields']:
                df = df.filter(col(field).isNotNull() & (col(field) != ""))

            # Apply data quality checks
            quality_checks = validation_config['data_quality_checks']

            # Coordinate bounds
            coord_bounds = quality_checks['coordinate_bounds']
            df = df.filter(
                (col("pickup_latitude") >= coord_bounds['lat_min']) &
                (col("pickup_latitude") <= coord_bounds['lat_max']) &
                (col("pickup_longitude") >= coord_bounds['lon_min']) &
                (col("pickup_longitude") <= coord_bounds['lon_max'])
            )

            # Fare bounds
            fare_bounds = quality_checks['fare_bounds']
            df = df.filter(
                (col("fare_amount").isNull()) |
                ((col("fare_amount") >= fare_bounds['min_fare']) &
                 (col("fare_amount") <= fare_bounds['max_fare']))
            )

            # Distance bounds
            distance_bounds = quality_checks['distance_bounds']
            df = df.filter(
                (col("distance_km").isNull()) |
                ((col("distance_km") >= distance_bounds['min_distance']) &
                 (col("distance_km") <= distance_bounds['max_distance']))
            )

            # Timestamp bounds
            timestamp_bounds = quality_checks['timestamp_bounds']
            df = df.filter(
                (col("pickup_ts").isNull()) |
                ((col("pickup_ts") >= lit(f"{timestamp_bounds['min_year']}-01-01")) &
                 (col("pickup_ts") <= lit(f"{timestamp_bounds['max_year']}-12-31")))
            )

            logger.debug("Validation rules applied")
            return df

        except Exception as e:
            logger.error("Failed to apply validation rules", error=str(e))
            raise

    def _deduplicate_records(self, df: DataFrame) -> DataFrame:
        """Deduplicate records based on configuration."""
        try:
            dedup_config = self.config['processing']['deduplication']

            if not dedup_config.get('enabled', True):
                return df

            key_columns = dedup_config['key_columns']
            strategy = dedup_config.get('strategy', 'keep_last')

            # Count duplicates before deduplication
            total_before = df.count()

            if strategy == 'keep_last':
                # Keep the last occurrence based on processing timestamp
                window_spec = Window.partitionBy(*key_columns).orderBy(desc("processing_timestamp"))
                df = df.withColumn("row_num", row_number().over(window_spec))
                df = df.filter(col("row_num") == 1).drop("row_num")
            elif strategy == 'keep_first':
                # Keep the first occurrence
                window_spec = Window.partitionBy(*key_columns).orderBy(asc("processing_timestamp"))
                df = df.withColumn("row_num", row_number().over(window_spec))
                df = df.filter(col("row_num") == 1).drop("row_num")
            # 'keep_all' strategy - no deduplication

            total_after = df.count()
            duplicates_removed = total_before - total_after
            self.processing_stats['duplicate_records'] = duplicates_removed

            logger.info("Deduplication completed",
                       strategy=strategy,
                       duplicates_removed=duplicates_removed,
                       batch_id=self.batch_id)

            return df

        except Exception as e:
            logger.error("Failed to deduplicate records", error=str(e))
            raise

    def process_to_monkdb(self, df: DataFrame) -> None:
        """Process the cleaned data to MonkDB."""
        try:
            logger.info("Starting MonkDB processing", batch_id=self.batch_id)

            # Convert to Pandas for MonkDB processing
            pdf = df.toPandas()  # type: ignore[attr-defined]
            records = pdf.to_dict('records')  # type: ignore[attr-defined]

            if not records:
                logger.info("No records to process", batch_id=self.batch_id)
                return

            with get_monk_connection() as conn:
                cursor = conn.cursor()

                # Process dimensions first
                self._process_dimensions(cursor, records)

                # Process facts
                fact_results = self._process_facts(cursor, records)

                # Update processing stats
                self.processing_stats['inserted_records'] = fact_results.get('inserted', 0)
                self.processing_stats['updated_records'] = fact_results.get('updated', 0)

                # Record batch metadata
                self._record_batch_metadata(cursor)

            logger.info("MonkDB processing completed",
                       batch_id=self.batch_id,
                       results=fact_results)

        except Exception as e:
            logger.error("Failed to process data to MonkDB",
                        batch_id=self.batch_id,
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
                        'driver_name': f"Driver_{driver_id}",
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
                        'latitude': record.get('pickup_latitude'),
                        'longitude': record.get('pickup_longitude'),
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
                        'latitude': record.get('dropoff_latitude'),
                        'longitude': record.get('dropoff_longitude'),
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
                       geolocations=len(geolocations),
                       batch_id=self.batch_id)

        except Exception as e:
            logger.error("Failed to process dimensions", error=str(e))
            raise

    def _process_facts(self, cursor, records: List[Dict[str, Any]]) -> Dict[str, int]:
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
                    'ride_id': record.get('ride_id_normalized', record.get('ride_id')),
                    'driver_key': driver_key,
                    'vehicle_key': vehicle_key,
                    'pickup_geolocation_key': pickup_geo_key,
                    'dropoff_geolocation_key': dropoff_geo_key,
                    'pickup_time_key': record.get('pickup_time_key'),
                    'dropoff_time_key': record.get('dropoff_time_key'),
                    'pickup_timestamp': record.get('pickup_ts'),
                    'dropoff_timestamp': record.get('dropoff_ts'),
                    'pickup_lat': record.get('pickup_latitude'),
                    'pickup_lon': record.get('pickup_longitude'),
                    'dropoff_lat': record.get('dropoff_latitude'),
                    'dropoff_lon': record.get('dropoff_longitude'),
                    'distance_km': record.get('distance_km'),
                    'duration_min': record.get('duration_min'),
                    'fare_amount': record.get('fare_amount'),
                    'tip_amount': record.get('tip_amount'),
                    'total_amount': record.get('total_amount'),
                    'payment_type': record.get('payment_type'),
                    'batch_id': record.get('batch_id'),
                    'processing_timestamp': record.get('processing_timestamp'),
                    'source': 'csv_batch'
                }

                fact_records.append(fact_record)

            # Bulk upsert fact records
            results = bulk_upsert_facts(cursor, monkdb_config['tables']['fact_rides'],
                                      fact_records, unique_key='ride_id',
                                      batch_size=monkdb_config['batch_processing']['batch_size'])

            logger.info("Facts processed", results=results, batch_id=self.batch_id)
            return results

        except Exception as e:
            logger.error("Failed to process facts", error=str(e))
            raise

    def _record_batch_metadata(self, cursor) -> None:
        """Record batch processing metadata."""
        try:
            if not self.config['batch_metadata']['tracking']['enabled']:
                return

            self.processing_stats['end_time'] = datetime.now(timezone.utc)
            end_time = self.processing_stats['end_time']
            start_time = self.processing_stats['start_time']
            if isinstance(end_time, datetime) and isinstance(start_time, datetime):
                processing_duration = (end_time - start_time).total_seconds()
            else:
                processing_duration = 0.0

            metadata_record = {
                'batch_id': self.batch_id,
                'processing_start_time': self.processing_stats['start_time'],
                'processing_end_time': self.processing_stats['end_time'],
                'processing_duration_seconds': processing_duration,
                'files_processed': self.processing_stats['files_processed'],
                'total_records': self.processing_stats['total_records'],
                'valid_records': self.processing_stats['valid_records'],
                'invalid_records': self.processing_stats['invalid_records'],
                'duplicate_records': self.processing_stats['duplicate_records'],
                'inserted_records': self.processing_stats['inserted_records'],
                'updated_records': self.processing_stats['updated_records'],
                'status': 'success'
            }

            # Insert batch metadata
            monkdb_config = self.config['monkdb']
            table_name = monkdb_config['tables']['batch_metadata']
            columns = list(metadata_record.keys())
            values = [tuple(metadata_record.values())]

            bulk_insert(cursor, table_name, columns, values, batch_size=1)

            logger.info("Batch metadata recorded",
                       batch_id=self.batch_id,
                       processing_duration=processing_duration)

        except Exception as e:
            logger.error("Failed to record batch metadata", error=str(e))
            # Don't raise - metadata recording failure shouldn't fail the job

    def archive_processed_files(self, file_paths: List[str]) -> None:
        """Archive processed files if archiving is enabled."""
        try:
            archive_config = self.config['data_sources']['archive']

            if not archive_config.get('enabled', True):
                logger.info("File archiving disabled")
                return

            archive_path = Path(archive_config['path'])
            archive_path.mkdir(parents=True, exist_ok=True)

            # Create batch-specific archive directory
            batch_archive_path = archive_path / self.batch_id
            batch_archive_path.mkdir(exist_ok=True)

            archived_count = 0
            for file_path in file_paths:
                try:
                    source_file = Path(file_path)
                    if source_file.exists():
                        target_file = batch_archive_path / source_file.name
                        source_file.rename(target_file)
                        archived_count += 1
                        logger.debug("File archived",
                                   source=str(source_file),
                                   target=str(target_file))
                except Exception as e:
                    logger.warning("Failed to archive file",
                                 file_path=file_path,
                                 error=str(e))

            logger.info("File archiving completed",
                       archived_count=archived_count,
                       total_files=len(file_paths),
                       batch_id=self.batch_id)

        except Exception as e:
            logger.error("Failed to archive files", error=str(e))
            # Don't raise - archiving failure shouldn't fail the job

    def run_batch_job(self) -> Dict[str, Any]:
        """Run the complete batch processing job."""
        try:
            logger.info("Starting batch CSV processing job", batch_id=self.batch_id)

            # Create Spark session
            self.create_spark_session()

            # Discover input files
            file_paths = self.discover_input_files()

            if not file_paths:
                logger.info("No files to process", batch_id=self.batch_id)
                return self.processing_stats

            # Read CSV files
            raw_df = self.read_csv_files(file_paths)

            # Clean and transform data
            processed_df = self.clean_and_transform(raw_df)

            # Process to MonkDB
            self.process_to_monkdb(processed_df)

            # Archive processed files
            self.archive_processed_files(file_paths)

            # Finalize stats
            self.processing_stats['end_time'] = datetime.now(timezone.utc)

            logger.info("Batch processing job completed successfully",
                       batch_id=self.batch_id,
                       stats=self.processing_stats)

            return self.processing_stats

        except Exception as e:
            self.processing_stats['end_time'] = datetime.now(timezone.utc)
            logger.error("Batch processing job failed",
                        batch_id=self.batch_id,
                        error=str(e))
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped", batch_id=self.batch_id)


def main():
    """Main entry point for the batch processing job."""
    try:
        # Initialize processor
        processor = BatchCSVProcessor()

        # Run batch job
        stats = processor.run_batch_job()

        # Print summary
        print("\n=== Batch Processing Summary ===")
        print(f"Batch ID: {processor.batch_id}")
        print(f"Files Processed: {stats['files_processed']}")
        print(f"Total Records: {stats['total_records']}")
        print(f"Valid Records: {stats['valid_records']}")
        print(f"Invalid Records: {stats['invalid_records']}")
        print(f"Duplicate Records: {stats['duplicate_records']}")
        print(f"Inserted Records: {stats['inserted_records']}")
        print(f"Updated Records: {stats['updated_records']}")

        if stats['end_time'] and stats['start_time']:
            duration = (stats['end_time'] - stats['start_time']).total_seconds()
            print(f"Processing Duration: {duration:.2f} seconds")

        print("=== Batch Processing Completed ===\n")

    except KeyboardInterrupt:
        logger.info("Batch processing job interrupted by user")
    except Exception as e:
        logger.error("Batch processing job failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
