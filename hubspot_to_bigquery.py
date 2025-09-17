#!/usr/bin/env python3
"""
HubSpot Contacts to BigQuery Sync Script
Replaces the Supabase sync with BigQuery
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import requests
import time

class HubSpotBigQuerySync:
    def __init__(self):
        self.hubspot_api_key = os.getenv('HUBSPOT_API_KEY')
        self.project_id = os.getenv('GCP_PROJECT_ID', 'brevo-471121')
        self.dataset_id = os.getenv('BQ_DATASET', 'hubspot')
        self.table_id = os.getenv('BQ_TABLE', 'hubspot_contacts')
        
        if not self.hubspot_api_key:
            raise ValueError("HUBSPOT_API_KEY environment variable is required")
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
            
        self.client = bigquery.Client(project=self.project_id)
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
    def get_all_hubspot_contacts(self) -> List[Dict[str, Any]]:
        """Fetch all contacts from HubSpot API with pagination"""
        contacts = []
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        
        # Define all the properties we want to fetch based on your schema
        properties = [
            "address", "annualrevenue", "associatedcompanyid", "associatedcompanylastupdated",
            "city", "closedate", "comment", "company", "company_size", "country", "createdate",
            "currentlyinworkflow", "date_of_birth", "days_to_close", "degree", "email",
            "engagements_last_meeting_booked", "engagements_last_meeting_booked_campaign",
            "engagements_last_meeting_booked_medium", "engagements_last_meeting_booked_source",
            "fax", "field_of_study", "first_conversion_date", "first_conversion_event_name",
            "first_deal_created_date", "firstname", "followercount", "gender", "graduation_date",
            "hs_additional_emails", "hs_all_accessible_team_ids", "hs_all_assigned_business_unit_ids",
            "hs_all_contact_vids", "hs_all_owner_ids", "hs_all_team_ids", "hs_analytics_average_page_views",
            "hs_analytics_first_referrer", "hs_analytics_first_timestamp", "hs_analytics_first_touch_converting_campaign",
            "hs_analytics_first_url", "hs_analytics_first_visit_timestamp", "hs_analytics_last_referrer",
            "hs_analytics_last_timestamp", "hs_analytics_last_touch_converting_campaign", "hs_analytics_last_url",
            "hs_analytics_last_visit_timestamp", "hs_analytics_num_event_completions", "hs_analytics_num_page_views",
            "hs_analytics_num_visits", "hs_analytics_revenue", "hs_analytics_source", "hs_analytics_source_data_1",
            "hs_analytics_source_data_2", "hs_associated_target_accounts", "hs_avatar_filemanager_key",
            "hs_buying_role", "hs_calculated_form_submissions", "hs_calculated_merged_vids",
            "hs_calculated_mobile_number", "hs_calculated_phone_number", "hs_calculated_phone_number_area_code",
            "hs_calculated_phone_number_country_code", "hs_calculated_phone_number_region_code",
            "hs_clicked_linkedin_ad", "hs_contact_enrichment_opt_out", "hs_contact_enrichment_opt_out_timestamp",
            "hs_content_membership_email", "hs_content_membership_email_confirmed",
            "hs_content_membership_follow_up_enqueued_at", "hs_content_membership_notes",
            "hs_content_membership_registered_at", "hs_content_membership_registration_domain_sent_to",
            "hs_content_membership_registration_email_sent_at", "hs_content_membership_status",
            "hs_conversations_visitor_email", "hs_count_is_unworked", "hs_count_is_worked",
            "hs_country_region_code", "hs_created_by_conversations", "hs_created_by_user_id",
            "hs_createdate", "hs_cross_sell_opportunity", "hs_currently_enrolled_in_prospecting_agent",
            "hs_data_privacy_ads_consent", "hs_date_entered_customer", "hs_date_entered_evangelist",
            "hs_date_entered_lead", "hs_date_entered_marketingqualifiedlead", "hs_date_entered_opportunity",
            "hs_date_entered_other", "hs_date_entered_salesqualifiedlead", "hs_date_entered_subscriber",
            "hs_date_exited_customer", "hs_date_exited_evangelist"
        ]
        
        params = {
            "limit": 100,
            "properties": ",".join(properties),
            "archived": "false"
        }
        
        headers = {
            "Authorization": f"Bearer {self.hubspot_api_key}",
            "Content-Type": "application/json"
        }
        
        after = None
        total_contacts = 0
        
        while True:
            if after:
                params["after"] = after
                
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                batch_contacts = data.get("results", [])
                contacts.extend(batch_contacts)
                total_contacts += len(batch_contacts)
                
                print(f"Fetched {len(batch_contacts)} contacts (Total: {total_contacts})")
                
                # Check if there are more pages
                paging = data.get("paging", {})
                if "next" not in paging:
                    break
                    
                after = paging["next"]["after"]
                
                # Rate limiting - HubSpot allows 100 requests per 10 seconds
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching contacts: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response: {e.response.text}")
                raise
                
        print(f"Total contacts fetched: {total_contacts}")
        return contacts
    
    def transform_contacts_for_bigquery(self, contacts: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transform HubSpot contacts data for BigQuery"""
        transformed_data = []
        
        for contact in contacts:
            properties = contact.get("properties", {})
            
            # Create a row with all the fields from your schema
            row = {
                "hubspot_id": contact.get("id"),
                "created_at": self._parse_timestamp(contact.get("createdAt")),
                "updated_at": self._parse_timestamp(contact.get("updatedAt")),
                "archived": contact.get("archived", False),
                "raw_properties": json.dumps(properties),  # Store full properties as JSON
            }
            
            # Add all the individual properties
            for prop_name in properties:
                if prop_name in [
                    "address", "annualrevenue", "associatedcompanyid", "associatedcompanylastupdated",
                    "city", "closedate", "comment", "company", "company_size", "country", "createdate",
                    "currentlyinworkflow", "date_of_birth", "days_to_close", "degree", "email",
                    "engagements_last_meeting_booked", "engagements_last_meeting_booked_campaign",
                    "engagements_last_meeting_booked_medium", "engagements_last_meeting_booked_source",
                    "fax", "field_of_study", "first_conversion_date", "first_conversion_event_name",
                    "first_deal_created_date", "firstname", "followercount", "gender", "graduation_date"
                ] or prop_name.startswith("hs_"):
                    row[prop_name] = properties.get(prop_name)
            
            transformed_data.append(row)
        
        df = pd.DataFrame(transformed_data)
        
        # Add processing timestamp
        df["synced_at"] = datetime.now(timezone.utc)
        
        return df
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse HubSpot timestamp string to datetime"""
        if not timestamp_str:
            return None
        try:
            # HubSpot returns ISO format timestamps
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None
    
    def load_to_bigquery(self, df: pd.DataFrame, write_disposition: str = "WRITE_TRUNCATE"):
        """Load DataFrame to BigQuery"""
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,  # WRITE_TRUNCATE or WRITE_APPEND
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="updated_at"
            ),
            clustering_fields=["hubspot_id", "email"]
        )
        
        print(f"Loading {len(df)} contacts to BigQuery table: {self.table_ref}")
        
        job = self.client.load_table_from_dataframe(
            df, self.table_ref, job_config=job_config
        )
        
        try:
            job.result()  # Wait for the job to complete
            print(f"Successfully loaded {len(df)} contacts to BigQuery")
            
            # Print some stats
            table = self.client.get_table(self.table_ref)
            print(f"Table now has {table.num_rows} total rows")
            
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            raise
    
    def sync(self, incremental: bool = False):
        """Main sync function"""
        print("Starting HubSpot to BigQuery sync...")
        
        # Fetch contacts from HubSpot
        contacts = self.get_all_hubspot_contacts()
        
        if not contacts:
            print("No contacts found in HubSpot")
            return
        
        # Transform for BigQuery
        df = self.transform_contacts_for_bigquery(contacts)
        
        # Determine write disposition
        write_disposition = "WRITE_APPEND" if incremental else "WRITE_TRUNCATE"
        
        # Load to BigQuery
        self.load_to_bigquery(df, write_disposition)
        
        print("Sync completed successfully!")

def main():
    """Main function for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync HubSpot contacts to BigQuery")
    parser.add_argument("--incremental", action="store_true", 
                       help="Use incremental load (append) instead of full refresh")
    
    args = parser.parse_args()
    
    try:
        syncer = HubSpotBigQuerySync()
        syncer.sync(incremental=args.incremental)
    except Exception as e:
        print(f"Sync failed: {e}")
        raise

if __name__ == "__main__":
    main()
