# HubSpot to Supabase Sync

This project syncs data from HubSpot to Supabase on a daily basis using GitHub Actions.

## Features

- Incremental sync (only fetches changed records)
- Handles rate limiting and retries
- Logs all operations
- Runs automatically via GitHub Actions
- Supports contacts, companies, and deals

## Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/hubspot-supabase-sync.git
   cd hubspot-supabase-sync
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file with the following variables:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   HUBSPOT_API_KEY=your_hubspot_api_key
   ```

4. **Run locally**
   ```bash
   python export_hubspot_to_supabase.py
   ```

## GitHub Actions Setup

1. Push this repository to GitHub
2. Go to your repository's Settings > Secrets and variables > Actions
3. Add the following repository secrets:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase anon/public key
   - `HUBSPOT_API_KEY`: Your HubSpot API key

## Database Schema

The sync creates the following tables:
- `hubspot_contacts`
- `hubspot_companies`
- `hubspot_deals`
- `sync_state` (tracks last sync time)

## Manual Trigger

You can manually trigger the sync by going to the Actions tab in your GitHub repository and clicking "Run workflow" on the "HubSpot to Supabase Sync" workflow.

## Logs

Logs are available in the GitHub Actions interface and are also saved as artifacts for 7 days.

## License

MIT
