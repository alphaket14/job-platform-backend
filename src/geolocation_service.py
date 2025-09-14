# src/geolocation_service.py

import logging
import json
from typing import Dict, List, Optional

from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
from google.cloud import storage

from .config import Config

logger = logging.getLogger(__name__)


storage_client = storage.Client()
GEO_CACHE_FILENAME = "geolocation_cache.json"
GEO_CACHE = {}
_geo_cache_loaded = False

try:
    bucket = storage_client.bucket(Config.GCS_CACHE_BUCKET)
except Exception as e:
    logger.error(f"Could not initialize GCS bucket for geolocation cache: {e}")
    bucket = None

geolocator = GoogleV3(api_key=Config.GOOGLE_API_KEY, timeout=10)
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=0.1,
    return_value_on_exception=None
)


def _load_geo_cache():
    """Loads the geolocation cache from GCS into memory."""
    global GEO_CACHE, _geo_cache_loaded
    if not bucket:
        logger.warning("GCS bucket not configured; geolocation caching is disabled.")
        _geo_cache_loaded = True
        return
    try:
        blob = bucket.blob(GEO_CACHE_FILENAME)
        if blob.exists():
            logger.info(f"Loading geolocation cache from GCS: gs://{Config.GCS_CACHE_BUCKET}/{GEO_CACHE_FILENAME}")
            json_data = blob.download_as_text()
            GEO_CACHE = json.loads(json_data)
        else:
            logger.info("Geolocation cache not found in GCS. A new one will be created.")
            GEO_CACHE = {}
    except Exception as e:
        logger.error(f"Failed to load geolocation cache from GCS. Caching will be limited for this run. Error: {e}")
        GEO_CACHE = {}
    _geo_cache_loaded = True

def save_geo_cache():
    """Saves the in-memory geolocation cache back to GCS."""
    if not bucket or not _geo_cache_loaded:
        # Don't save if the cache was never loaded, to avoid overwriting with an empty dict
        return

    try:
        logger.info(f"Saving updated geolocation cache to GCS...")
        blob = bucket.blob(GEO_CACHE_FILENAME)
        json_data = json.dumps(GEO_CACHE, indent=2)
        blob.upload_from_string(json_data, content_type='application/json')
    except Exception as e:
        logger.error(f"Failed to save geolocation cache to GCS: {e}")



def generate_location_string(locations: List[Dict]) -> str:
    """
    Creates a single, comma-separated string from a list of location objects.
    e.g., "Boston MA US, Seattle WA US"
    """
    if not locations:
        return ""
    location_parts = []
    for loc in locations:
        parts = [loc.get('city'), loc.get('state'), loc.get('country')]
        location_parts.append(" ".join(p for p in parts if p))
    return ", ".join(location_parts)


def get_geo_coordinates(locations: List[Dict]) -> Optional[Dict[str, float]]:
    """
    Gets a single representative lat/long coordinate object using a persistent GCS cache.
    """
    global _geo_cache_loaded
    if not _geo_cache_loaded:
        _load_geo_cache()

    if not locations:
        return None

    query_string = ""
    if len(locations) == 1:
        loc = locations[0]
        parts = [loc.get('city'), loc.get('state'), loc.get('country')]
        query_string = ", ".join(p for p in parts if p)
    else:
        states = {loc.get('state') for loc in locations if loc.get('state')}
        if len(states) == 1:
            state = states.pop()
            country = locations[0].get('country', 'US')
            query_string = f"{state}, {country}"
        else:
            query_string = locations[0].get('country', 'US')

    if not query_string:
        return None

    if query_string in GEO_CACHE:
        return GEO_CACHE[query_string]

    try:
        logger.info(f"Geocoding location query (GCS cache miss): '{query_string}'")
        location = geocode(query_string)
        if location:
            result = {"lat": location.latitude, "lng": location.longitude}
            GEO_CACHE[query_string] = result  # Add to in-memory cache
            return result
        else:
            logger.warning(f"Could not find coordinates for query: '{query_string}'")
            GEO_CACHE[query_string] = None  # Cache the failure
            return None
    except Exception as e:
        logger.error(f"Error during geocoding for '{query_string}': {e}")
        GEO_CACHE[query_string] = None  # Cache the failure
        return None