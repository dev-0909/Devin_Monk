"""
Utility functions for PySpark ETL operations.
Provides time key generation, duration computation, and geospatial utilities.
"""

import hashlib
from datetime import datetime
from typing import Optional, Union
import structlog

logger = structlog.get_logger(__name__)


def generate_time_key(timestamp: Union[str, datetime]) -> int:
    """
    Generate time key in YYYYMMDDHH format from timestamp.
    
    Args:
        timestamp: ISO timestamp string or datetime object
        
    Returns:
        Integer time key in YYYYMMDDHH format
        
    Examples:
        >>> generate_time_key("2024-01-15T14:30:00Z")
        2024011514
        >>> generate_time_key(datetime(2024, 1, 15, 14, 30))
        2024011514
    """
    try:
        if isinstance(timestamp, str):
            # Parse ISO timestamp
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")

        time_key = int(dt.strftime("%Y%m%d%H"))
        logger.debug("Generated time key", timestamp=timestamp, time_key=time_key)
        return time_key

    except Exception as e:
        logger.error("Failed to generate time key", timestamp=timestamp, error=str(e))
        raise ValueError(f"Invalid timestamp format: {timestamp}") from e


def compute_duration(pickup_ts: Union[str, datetime], dropoff_ts: Union[str, datetime]) -> Optional[int]:
    """
    Compute duration in minutes between pickup and dropoff timestamps.
    
    Args:
        pickup_ts: Pickup timestamp (ISO string or datetime)
        dropoff_ts: Dropoff timestamp (ISO string or datetime)
        
    Returns:
        Duration in minutes, or None if invalid timestamps
        
    Examples:
        >>> compute_duration("2024-01-15T14:00:00Z", "2024-01-15T14:30:00Z")
        30
        >>> compute_duration("2024-01-15T14:30:00Z", "2024-01-15T14:00:00Z")
        None
    """
    try:
        # Parse timestamps
        if isinstance(pickup_ts, str):
            pickup_dt = datetime.fromisoformat(pickup_ts.replace('Z', '+00:00'))
        else:
            pickup_dt = pickup_ts

        if isinstance(dropoff_ts, str):
            dropoff_dt = datetime.fromisoformat(dropoff_ts.replace('Z', '+00:00'))
        else:
            dropoff_dt = dropoff_ts

        # Validate chronological order
        if dropoff_dt <= pickup_dt:
            logger.warning(
                "Invalid ride duration: dropoff before pickup",
                pickup_ts=pickup_ts,
                dropoff_ts=dropoff_ts
            )
            return None

        # Calculate duration in minutes
        duration_seconds = (dropoff_dt - pickup_dt).total_seconds()
        duration_minutes = int(duration_seconds / 60)

        logger.debug(
            "Computed ride duration",
            pickup_ts=pickup_ts,
            dropoff_ts=dropoff_ts,
            duration_min=duration_minutes
        )

        return duration_minutes

    except Exception as e:
        logger.error(
            "Failed to compute duration",
            pickup_ts=pickup_ts,
            dropoff_ts=dropoff_ts,
            error=str(e)
        )
        return None


def geohash(lat: float, lon: float, precision: int = 7) -> str:
    """
    Generate geohash for latitude/longitude coordinates.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate  
        precision: Geohash precision level (default: 7)
        
    Returns:
        Geohash string
        
    Examples:
        >>> geohash(40.7128, -74.0060, precision=7)
        'dr5regw'
        >>> geohash(37.7749, -122.4194, precision=5)
        '9q8yy'
    """
    try:
        # Validate coordinates
        if not (-90 <= lat <= 90):
            raise ValueError(f"Invalid latitude: {lat}")
        if not (-180 <= lon <= 180):
            raise ValueError(f"Invalid longitude: {lon}")
        if not (1 <= precision <= 12):
            raise ValueError(f"Invalid precision: {precision}")

        # Simple geohash implementation using coordinate encoding
        # This is a simplified version - in production, use a proper geohash library
        lat_range = [-90.0, 90.0]
        lon_range = [-180.0, 180.0]

        bits = []
        even = True  # Start with longitude

        for _ in range(precision * 5):  # 5 bits per character
            if even:  # Longitude
                mid = (lon_range[0] + lon_range[1]) / 2
                if lon >= mid:
                    bits.append(1)
                    lon_range[0] = mid
                else:
                    bits.append(0)
                    lon_range[1] = mid
            else:  # Latitude
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    bits.append(1)
                    lat_range[0] = mid
                else:
                    bits.append(0)
                    lat_range[1] = mid
            even = not even

        # Convert bits to base32
        base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
        geohash_str = ""

        for i in range(0, len(bits), 5):
            chunk = bits[i:i+5]
            while len(chunk) < 5:
                chunk.append(0)

            value = 0
            for j, bit in enumerate(chunk):
                value += bit * (2 ** (4 - j))

            geohash_str += base32[value]

        logger.debug(
            "Generated geohash",
            lat=lat,
            lon=lon,
            precision=precision,
            geohash=geohash_str
        )

        return geohash_str

    except Exception as e:
        logger.error(
            "Failed to generate geohash",
            lat=lat,
            lon=lon,
            precision=precision,
            error=str(e)
        )
        # Fallback: use coordinate hash
        coord_str = f"{lat:.6f},{lon:.6f}"
        return hashlib.md5(coord_str.encode()).hexdigest()[:precision]


def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate latitude and longitude coordinates.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    try:
        return (-90 <= lat <= 90) and (-180 <= lon <= 180)
    except (TypeError, ValueError):
        return False


def normalize_ride_id(ride_id: str) -> str:
    """
    Normalize ride ID for consistent processing.
    
    Args:
        ride_id: Raw ride identifier
        
    Returns:
        Normalized ride ID
    """
    if not ride_id:
        raise ValueError("Ride ID cannot be empty")

    # Remove whitespace and convert to lowercase
    normalized = str(ride_id).strip().lower()

    if not normalized:
        raise ValueError("Ride ID cannot be empty after normalization")

    return normalized


def safe_float_conversion(value: Union[str, float, int, None]) -> Optional[float]:
    """
    Safely convert value to float with error handling.
    
    Args:
        value: Value to convert
        
    Returns:
        Float value or None if conversion fails
    """
    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning("Failed to convert value to float", value=value)
        return None


def safe_int_conversion(value: Union[str, int, float, None]) -> Optional[int]:
    """
    Safely convert value to integer with error handling.
    
    Args:
        value: Value to convert
        
    Returns:
        Integer value or None if conversion fails
    """
    if value is None:
        return None

    try:
        return int(float(value))  # Handle string floats like "123.0"
    except (ValueError, TypeError):
        logger.warning("Failed to convert value to int", value=value)
        return None
