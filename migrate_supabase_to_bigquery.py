#!/usr/bin/env python3
"""
One-time migration script to move existing HubSpot data from Supabase to BigQuery
Run this before starting the daily ETL to BigQuery
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from google.cloud import bigquery
from datetime import datetime
import json

class SupabaseToBigQueryMigration:
    def __init__(self):
        # Supabase connection
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        
        # BigQuery connection
        self.project_id = os.getenv('GCP_PROJECT_ID', 'brevo-471121')
        self.dataset_id = os.getenv('BQ_DATASET', 'hubspot')
        self.table_id = os.getenv('BQ_TABLE', 'hubspot_contacts')
        
        if not self.supabase_url:
            raise ValueError("SUPABASE_URL environment variable is required")
        
        self.bq_client = bigquery.Client(project=self.project_id)
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Create Supabase connection string
        # Extract database details from Supabase URL
        self.supabase_conn_string = self._build_supabase_connection()
    
    def _build_supabase_connection(self):
        """Build PostgreSQL connection string from Supabase URL"""
        # Supabase URL format: https://your-project.supabase.co
        # We need to construct the PostgreSQL connection
        
        # You'll need to get these from your Supabase dashboard -> Settings -> Database
        supabase_host = self.supabase_url.replace('https://', '').replace('http://', '')
        supabase_project_id = supabase_host.split('.')[0]
        
        # Standard Supabase PostgreSQL connection format
        conn_string = f"postgresql://postgres.{supabase_project_id}:{self.supabase_key}@aws-0-us-west-1.pooler.supabase.com:6543/postgres"

        
        print("Note: You may need to update the connection string based on your Supabase region")
        print("Check your Supabase Dashboard -> Settings -> Database for the exact connection details")
        
        return conn_string
    
    def export_from_supabase(self):
        """Export all data from Supabase hubspot_contacts table"""
        print("Connecting to Supabase...")
        
        try:
            # Create SQLAlchemy engine
            engine = create_engine(self.supabase_conn_string)
            
            # Query to get all data
            query = """
            SELECT 
                id,
                hubspot_id,
                created_at,
                updated_at,
                archived,
                raw_properties,
                address, annualrevenue, associatedcompanyid, associatedcompanylastupdated,
                city, closedate, comment, company, company_size, country, createdate,
                currentlyinworkflow, date_of_birth, days_to_close, degree, email,
                engagements_last_meeting_booked, engagements_last_meeting_booked_campaign,
                engagements_last_meeting_booked_medium, engagements_last_meeting_booked_source,
                fax, field_of_study, first_conversion_date, first_conversion_event_name,
                first_deal_created_date, firstname, followercount, gender, graduation_date,
                hs_additional_emails, hs_all_accessible_team_ids, hs_all_assigned_business_unit_ids,
                hs_all_contact_vids, hs_all_owner_ids, hs_all_team_ids, hs_analytics_average_page_views,
                hs_analytics_first_referrer, hs_analytics_first_timestamp, hs_analytics_first_touch_converting_campaign,
                hs_analytics_first_url, hs_analytics_first_visit_timestamp, hs_analytics_last_referrer,
                hs_analytics_last_timestamp, hs_analytics_last_touch_converting_campaign, hs_analytics_last_url,
                hs_analytics_last_visit_timestamp, hs_analytics_num_event_completions, hs_analytics_num_page_views,
                hs_analytics_num_visits, hs_analytics_revenue, hs_analytics_source, hs_analytics_source_data_1,
                hs_analytics_source_data_2, hs_associated_target_accounts, hs_avatar_filemanager_key,
                hs_buying_role, hs_calculated_form_submissions, hs_calculated_merged_vids,
                hs_calculated_mobile_number, hs_calculated_phone_number, hs_calculated_phone_number_area_code,
                hs_calculated_phone_number_country_code, hs_calculated_phone_number_region_code,
                hs_clicked_linkedin_ad, hs_contact_enrichment_opt_out, hs_contact_enrichment_opt_out_timestamp,
                hs_content_membership_email, hs_content_membership_email_confirmed,
                hs_content_membership_follow_up_enqueued_at, hs_content_membership_notes,
                hs_content_membership_registered_at, hs_content_membership_registration_domain_sent_to,
                hs_content_membership_registration_email_sent_at, hs_content_membership_status,
                hs_conversations_visitor_email, hs_count_is_unworked, hs_count_is_worked,
                hs_country_region_code, hs_created_by_conversations, hs_created_by_user_id,
                hs_createdate, hs_cross_sell_opportunity, hs_currently_enrolled_in_prospecting_agent,
                hs_data_privacy_ads_consent, hs_date_entered_customer, hs_date_entered_evangelist,
                hs_date_entered_lead, hs_date_entered_marketingqualifiedlead, hs_date_entered_opportunity,
                hs_date_entered_other, hs_date_entered_salesqualifiedlead, hs_date_entered_subscriber,
                hs_date_exited_customer, hs_date_exited_evangelist
            FROM hubspot_contacts
            ORDER BY updated_at DESC;
            """
            
            print("Executing query...")
            df = pd.read_sql(query, engine)
            
            print(f"Exported {len(df)} records from Supabase")
            
            if len(df) == 0:
                print("No data found in Supabase table")
                return None
            
            # Show some basic stats
            print(f"Date range: {df['created_at'].min()} to {df['updated_at'].max()}")
            print(f"Unique contacts: {df['hubspot_id'].nunique()}")
            
            return df
            
        except Exception as e:
            print(f"Error connecting to Supabase: {e}")
            print("\nTroubleshooting tips:")
            print("1. Check your SUPABASE_URL and SUPABASE_KEY secrets")
            print("2. Verify the connection string format for your Supabase region")
            print("3. Make sure your Supabase project allows external connections")
            raise
    
    def transform_for_bigquery(self, df):
        """Transform the data for BigQuery compatibility"""
        print("Transforming data for BigQuery...")
        
        # Remove the auto-increment ID column since BigQuery doesn't need it
        if 'id' in df.columns:
            df = df.drop('id', axis=1)
        
        # Convert JSON columns
        if 'raw_properties' in df.columns:
            # Ensure raw_properties is properly formatted JSON
            df['raw_properties'] = df['raw_properties'].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
        
        # Add migration timestamp
        df['migrated_at'] = datetime.utcnow()
        df['data_source'] = 'supabase_migration'
        
        return df
    
    def load_to_bigquery(self, df):
        """Load the transformed data to BigQuery"""
        print(f"Loading {len(df)} records to BigQuery...")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace all data
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        try:
            job = self.bq_client.load_table_from_dataframe(
                df, self.table_ref, job_config=job_config
            )
            
            job.result()  # Wait for job to complete
            
            print(f"Successfully migrated {len(df)} records to BigQuery")
            
            # Verify the data
            table = self.bq_client.get_table(self.table_ref)
            print(f"BigQuery table now has {table.num_rows} total rows")
            
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            raise
    
    def verify_migration(self):
        """Verify the migration by comparing record counts and sample data"""
        print("\nVerifying migration...")
        
        # Query BigQuery to check the migrated data
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT hubspot_id) as unique_contacts,
            MIN(created_at) as earliest_record,
            MAX(updated_at) as latest_record,
            COUNT(CASE WHEN data_source = 'supabase_migration' THEN 1 END) as migrated_records
        FROM `{self.table_ref}`
        """
        
        try:
            result = self.bq_client.query(query).to_dataframe()
            print("Migration verification:")
            print(result.to_string(index=False))
            
            return True
            
        except Exception as e:
            print(f"Error verifying migration: {e}")
            return False
    
    def migrate(self):
        """Main migration function"""
        print("Starting Supabase to BigQuery migration...")
        
        # Step 1: Export from Supabase
        df = self.export_from_supabase()
        
        if df is None or len(df) == 0:
            print("No data to migrate. Exiting.")
            return
        
        # Step 2: Transform for BigQuery
        df_transformed = self.transform_for_bigquery(df)
        
        # Step 3: Load to BigQuery
        self.load_to_bigquery(df_transformed)
        
        # Step 4: Verify migration
        if self.verify_migration():
            print("\n✅ Migration completed successfully!")
            print("You can now start the daily BigQuery ETL process.")
        else:
            print("\n❌ Migration verification failed. Please check the data.")

def main():
    """Main function for CLI usage"""
    try:
        migrator = SupabaseToBigQueryMigration()
        migrator.migrate()
    except Exception as e:
        print(f"Migration failed: {e}")
        raise

if __name__ == "__main__":
    main()
