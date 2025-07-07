import os
import time
import requests
import json
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sync.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(supabase_url, supabase_key)

# HubSpot API configuration
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_API_KEY')
if not HUBSPOT_ACCESS_TOKEN:
    raise ValueError("HUBSPOT_API_KEY environment variable is not set")

BASE_URL = 'https://api.hubapi.com/crm/v3'

def get_all_properties(object_type: str) -> List[str]:
    """Fetch all property names for a given object type from HubSpot"""
    url = f"{BASE_URL}/properties/{object_type}"
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        properties = response.json().get('results', [])
        return [prop['name'] for prop in properties]
    except Exception as e:
        print(f"Error fetching properties for {object_type}: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return []

def get_last_sync_time(object_type: str) -> Tuple[datetime, str]:
    """Get the last successful sync time from Supabase"""
    try:
        result = supabase.table('sync_state') \
            .select('*') \
            .eq('object_type', object_type) \
            .order('last_sync_time', desc=True) \
            .limit(1) \
            .execute()
        
        if result.data and len(result.data) > 0:
            last_sync = result.data[0]
            return datetime.fromisoformat(last_sync['last_sync_time']), last_sync.get('sync_cursor', '')
    except Exception as e:
        logger.warning(f"Error getting last sync time: {e}")
    
    # Default to 24 hours ago if no previous sync
    return datetime.now(timezone.utc) - timedelta(days=1), ''

def update_sync_state(object_type: str, sync_time: datetime, cursor: str = '') -> None:
    """Update the last sync time in Supabase"""
    try:
        sync_data = {
            'object_type': object_type,
            'last_sync_time': sync_time.isoformat(),
            'sync_cursor': cursor,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        supabase.table('sync_state').upsert(sync_data).execute()
    except Exception as e:
        logger.error(f"Error updating sync state: {e}")

def get_hubspot_objects(object_type: str, properties: Optional[List[str]] = None, limit: int = 100, 
                       last_sync_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Fetch objects from HubSpot with pagination and incremental support"""
    all_objects = []
    after = None
    has_more = True
    
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    logger.info(f"Fetching {object_type} from HubSpot...")
    
    while has_more:
        params = {
            'limit': min(limit, 100),
            'properties': ','.join(properties) if properties else None,
            'after': after
        }
        
        # Add incremental sync filter if last_sync_time is provided
        if last_sync_time:
            params['sorts'] = json.dumps([
                {
                    "propertyName": "hs_lastmodifieddate",
                    "direction": "ASCENDING"
                }
            ])
            params['filterGroups'] = json.dumps([
                {
                    "filters": [
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": int(last_sync_time.timestamp() * 1000)  # Convert to milliseconds
                        }
                    ]
                }
            ])
        
        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}
        
        try:
            response = requests.get(
                f'{BASE_URL}/objects/{object_type}',
                headers=headers,
                params=params
            )
            
            if response.status_code == 401:
                print("Error: Invalid or expired access token. Please check your HUBSPOT_API_KEY.")
                break
            elif response.status_code != 200:
                logger.error(f"Error fetching {object_type}: {response.status_code} - {response.text}")
                if response.status_code == 429:  # Rate limit hit
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logger.info(f"Rate limit hit. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                break
                
            data = response.json()
            batch = data.get('results', [])
            all_objects.extend(batch)
            print(f"Fetched {len(batch)} {object_type} (total: {len(all_objects)})")
            
            # Check if there are more pages
            if 'paging' in data and 'next' in data['paging']:
                after = data['paging']['next'].get('after')
                if not after:
                    break
            else:
                break
                
        except Exception as e:
            print(f"Error fetching {object_type}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            break
            
    return all_objects

def check_supabase_table_exists(table_name: str) -> bool:
    """Check if a table exists in Supabase"""
    try:
        supabase.table(table_name).select("count", count='exact').limit(1).execute()
        return True
    except Exception:
        return False

def create_supabase_table(table_name: str, properties: List[str]) -> bool:
    """Generate SQL for creating a new table in Supabase with the given properties"""
    # Basic columns that exist for all tables
    columns = [
        "id SERIAL PRIMARY KEY",
        "hubspot_id TEXT NOT NULL UNIQUE",
        "created_at TIMESTAMPTZ",
        "updated_at TIMESTAMPTZ",
        "archived BOOLEAN DEFAULT FALSE",
        "raw_properties JSONB"
    ]
    
    # Add property columns
    for prop in properties:
        # Sanitize column names (replace spaces and special characters with _)
        col_name = ''.join(c if c.isalnum() else '_' for c in prop).lower()
        columns.append(f'"{col_name}" TEXT')
    
    # Create the SQL statement
    create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);"
    
    print(f"\nPlease execute this SQL in your Supabase SQL Editor to create the {table_name} table:")
    print(create_sql)
    print("\nAfter creating the table, run this script again to import the data.")
    
    # Also save the SQL to a file for easy access
    sql_filename = f"create_{table_name}.sql"
    with open(sql_filename, 'w') as f:
        f.write(create_sql)
    print(f"\nSQL has been saved to {sql_filename}")
    
    return False  # Return False to indicate manual intervention is needed

def create_sync_state_table():
    """Create the sync_state table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_state (
        id SERIAL PRIMARY KEY,
        object_type TEXT NOT NULL UNIQUE,
        last_sync_time TIMESTAMPTZ NOT NULL,
        sync_cursor TEXT,
        updated_at TIMESTAMPTZ NOT NULL
    );
    """
    try:
        supabase.rpc('pg_execute', {'query': create_table_sql}).execute()
    except Exception as e:
        logger.error(f"Error creating sync_state table: {e}")

def export_object_type(object_type: str, specific_properties: Optional[List[str]] = None, batch_size: int = 100):
    """Export a specific object type from HubSpot to Supabase"""
    table_name = f"hubspot_{object_type}"
    
    # Create sync_state table if it doesn't exist
    create_sync_state_table()
    
    # Get last sync time
    last_sync_time, sync_cursor = get_last_sync_time(object_type)
    logger.info(f"Last sync for {object_type} was at {last_sync_time}")
    
    # Get all properties if specific_properties is None
    if specific_properties is None:
        print(f"Fetching all available properties for {object_type}...")
        properties = get_all_properties(object_type)
        if not properties:
            print(f"No properties found for {object_type}")
            return
        print(f"Found {len(properties)} properties for {object_type}")
    else:
        properties = specific_properties
    
    # Check if table exists
    if not check_supabase_table_exists(table_name):
        if not create_supabase_table(table_name, properties):
            return
    else:
        print(f"Table {table_name} already exists. Using existing table.")
    
    # Get objects from HubSpot (only those modified since last sync)
    logger.info(f"Fetching {object_type} data from HubSpot...")
    objects = get_hubspot_objects(
        object_type, 
        properties, 
        last_sync_time=last_sync_time,
        limit=batch_size
    )
    
    if not objects:
        print(f"No {object_type} found to export")
        return
    
    # Prepare data for Supabase
    records = []
    for obj in objects:
        try:
            record = {
                'hubspot_id': obj['id'],
                'created_at': obj.get('createdAt'),
                'updated_at': obj.get('updatedAt'),
                'archived': obj.get('archived', False),
                'raw_properties': obj.get('properties', {})
            }
            
            # Add properties as separate columns
            props = obj.get('properties', {})
            for prop in properties:
                # Sanitize column names to match what we created
                col_name = ''.join(c if c.isalnum() else '_' for c in prop).lower()
                prop_value = props.get(prop)
                if prop_value is not None:
                    record[col_name] = str(prop_value)
            
            records.append(record)
        except Exception as e:
            print(f"Error processing {object_type} {obj.get('id')}: {e}")
    
    if not records:
        print(f"No valid records to insert for {object_type}")
        return
    
    # Insert in batches
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        # Get all unique keys across all records in the batch
        all_keys = set()
        for record in batch:
            all_keys.update(record.keys())
        
        # Ensure all records have all keys, filling missing ones with None
        standardized_batch = []
        for record in batch:
            standardized_record = {key: record.get(key) for key in all_keys}
            standardized_batch.append(standardized_record)
        
        try:
            # Insert the standardized batch
            result = supabase.table(table_name).upsert(standardized_batch).execute()
            logger.info(f"Inserted/updated batch {i//batch_size + 1} of {total_batches} with {len(batch)} records")
            
        except Exception as e:
            print(f"Error inserting batch {i//batch_size + 1} of {total_batches}: {e}")
            # Try inserting records one by one to find the problematic record
            print("Attempting to insert records one by one...")
            success_count = 0
            for idx, record in enumerate(standardized_batch, 1):
                try:
                    supabase.table(table_name).upsert([record]).execute()
                    success_count += 1
                except Exception as single_error:
                    print(f"Error inserting record {idx} in batch: {single_error}")
                    print("Problematic record ID:", record.get('hubspot_id', 'unknown'))
            print(f"Successfully inserted {success_count} out of {len(standardized_batch)} records in this batch")

def main():
    # Define which objects to export with their specific properties
    # Set to None to fetch all properties
    OBJECTS_TO_EXPORT = {
        'contacts': [
            'email', 'firstname', 'lastname', 'phone', 'company', 'hs_object_id',
            'createdate', 'lastmodifieddate', 'hs_lead_status', 'lifecyclestage'
        ],
        'companies': [
            'name', 'domain', 'phone', 'hs_object_id', 'createdate', 
            'lastmodifieddate', 'industry', 'city', 'state', 'country', 'website'
        ],
        'deals': [
            'dealname', 'dealstage', 'pipeline', 'amount', 'closedate',
            'createdate', 'hs_object_id', 'lastmodifieddate', 'hubspot_owner_id'
        ]
    }
    
    logger.info("=" * 50)
    logger.info("Starting HubSpot to Supabase sync...")
    logger.info(f"Using Supabase URL: {supabase_url}")
    logger.info(f"Current time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 50)
    
    for object_type, properties in OBJECTS_TO_EXPORT.items():
        print(f"\n{'='*50}")
        print(f"Processing {object_type}...")
        try:
            export_object_type(object_type, properties)
            logger.info(f"✓ Successfully processed {object_type}")
            # Update sync time after successful sync
            update_sync_state(object_type, datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"✗ Error processing {object_type}: {e}", exc_info=True)
    
    logger.info("\nSync completed!")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()