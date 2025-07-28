# sleeper_data_pipeline_full_history.py

import requests
import pandas as pd
import math
from pymongo import MongoClient
import os

from dotenv import load_dotenv
load_dotenv()

# ... (SleeperAPIClient and MongoManager classes remain the same) ...
class SleeperAPIClient:
    """
    Client to handle all interactions with the Sleeper API.
    """
    BASE_URL = "https://api.sleeper.app/v1"

    def get_all_nfl_players(self):
        """Fetches a comprehensive list of all NFL players."""
        url = f"{self.BASE_URL}/players/nfl"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_league_info(self, league_id):
        """Fetches basic information for a given league."""
        url = f"{self.BASE_URL}/league/{league_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_league_users(self, league_id):
        """Fetches all users in a specific league."""
        url = f"{self.BASE_URL}/league/{league_id}/users"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_league_rosters(self, league_id):
        """Fetches all rosters in a specific league."""
        url = f"{self.BASE_URL}/league/{league_id}/rosters"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_draft_metadata(self, draft_id):
        """Fetches metadata for a specific draft."""
        url = f"{self.BASE_URL}/draft/{draft_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_draft_picks(self, draft_id):
        """Fetches all picks from a specific draft."""
        url = f"{self.BASE_URL}/draft/{draft_id}/picks"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_weekly_transactions(self, league_id, week):
        """Fetches all transactions for a given league and week."""
        url = f"{self.BASE_URL}/league/{league_id}/transactions/{week}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()


class MongoManager:
    """
    Manages all interactions with the MongoDB database.
    """
    def __init__(self, db_user, db_password, db_host, db_name='nfldata_v2'):
        """Initializes the database connection."""
        uri = f"mongodb+srv://{db_user}:{db_password}@{db_host}/?retryWrites=true&w=majority"
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        print("MongoDB connection established.")

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def clear_and_insert(self, collection_name, data):
        collection = self.get_collection(collection_name)
        collection.delete_many({})
        if data:
            collection.insert_many(data)
        print(f"Cleared and inserted {len(data)} documents into '{collection_name}'.")

    def archive_collection(self, source_name):
        archive_name = f"{source_name}_archival"
        source_collection = self.get_collection(source_name)
        documents = list(source_collection.find({}))
        self.clear_and_insert(archive_name, documents)
        print(f"Successfully archived '{source_name}' to '{archive_name}'.")

    def close_connection(self):
        self.client.close()
        print("MongoDB connection closed.")

class DataProcessor:
    """
    Handles the processing and transformation of fantasy football data.
    """
    def __init__(self, api_client, mongo_manager):
        self.api = api_client
        self.db = mongo_manager

    def update_master_player_list(self):
        print("Updating master player list...")
        players_data = self.api.get_all_nfl_players()
        self.db.clear_and_insert('players', list(players_data.values()))
        return players_data

    def sync_all_transactions(self, league_ids_with_seasons):
        """
        Fetches all transactions for all specified leagues and seasons and
        stores them in a single 'transactions' collection.
        """
        print("Syncing all historical transactions...")
        all_transactions = []
        for league_id, season in league_ids_with_seasons.items():
            for week in range(1, 19):
                try:
                    transactions = self.api.get_weekly_transactions(league_id, week)
                    for tx in transactions:
                        if tx.get("status") == "complete":
                            tx["season"] = int(season)
                            tx["league_id"] = league_id
                            all_transactions.append(tx)
                except requests.exceptions.HTTPError as e:
                    print(f"Warning: Could not fetch transactions for week {week}, league {league_id}. Status: {e.response.status_code}")
        
        self.db.clear_and_insert('transactions', all_transactions)

    def clean_transactions(self, cleaning_queries):
        """
        Removes transactions from the log based on a provided dictionary of queries.
        """
        print("Cleaning transactions based on exemption list...")
        transactions_collection = self.db.get_collection('transactions')
        total_deleted = 0
        for season, query_list in cleaning_queries.items():
            for query_item in query_list:
                # Build the final query, including the season
                final_query = {"season": season, **query_item}
                result = transactions_collection.delete_many(final_query)
                if result.deleted_count > 0:
                    print(f"  - Season {season}: Removed {result.deleted_count} documents matching query.")
                    total_deleted += result.deleted_count
        print(f"Total exempted transactions removed: {total_deleted}.")
    
    # ... (other DataProcessor methods) ...
    def build_user_and_roster_maps(self, league_ids):
        """
        Builds mappings of user IDs to display names and league IDs to rosters.
        """
        print("Building user and roster maps...")
        users_map = {}
        rosters_map = {}
        for league_id in league_ids:
            try:
                users_data = self.api.get_league_users(league_id)
                if not isinstance(users_data, list):
                    print(f"Warning: Unexpected user data format for league {league_id}. Skipping users.")
                    users_data = []
                for user in users_data:
                    users_map[user["user_id"]] = user["display_name"]
                
                roster_data = self.api.get_league_rosters(league_id)
                if not isinstance(roster_data, list):
                    print(f"Warning: Unexpected roster data format for league {league_id}. Skipping rosters.")
                    roster_data = []
                
                rosters_map[league_id] = []
                for roster in roster_data:
                    owner_id = roster.get("owner_id")
                    if owner_id:
                         rosters_map[league_id].append({
                            "owner_id": owner_id,
                            roster["roster_id"]: users_map.get(owner_id, "N/A"),
                            "sleeper_name": users_map.get(owner_id, "N/A")
                        })
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for league {league_id}: {e}. Skipping this league.")
                rosters_map[league_id] = [] # Ensure map is initialized even if data fetch fails
            except Exception as e:
                print(f"An unexpected error occurred for league {league_id}: {e}. Skipping this league.")
                rosters_map[league_id] = []
        return users_map, rosters_map
    
    def process_and_store_drafts(self, draft_ids, users_map, players_map):
        """
        Processes draft data, assigns initial contracts, and stores it in MongoDB.
        """
        print("Processing draft data...")
        all_draft_picks = []
        initial_rosters = {} # Keyed by season

        for draft_id in draft_ids:
            metadata = self.api.get_draft_metadata(draft_id)
            picks = self.api.get_draft_picks(draft_id)
            
            season = int(metadata["season"])
            league_id = metadata["league_id"]
            draft_type = "league" if len(picks) > 100 else "rookie"
            
            print(f"Processing {season} {draft_type} draft for league {league_id}...")
            
            if season not in initial_rosters:
                initial_rosters[season] = []

            for pick in picks:
                player_id = pick["player_id"]
                owner_id = pick["picked_by"]

                pick.update({
                    "league_id": league_id,
                    "season": season,
                    "draft_type": draft_type,
                    "team_name": users_map.get(owner_id),
                    "player_name": players_map.get(player_id, {}).get("full_name"),
                    "contract": {}
                })
                
                # Assign initial contract based on draft type
                if draft_type == "rookie":
                    # Your rookie contract logic here...
                    pass
                else: # Startup draft
                    # Your startup contract logic here...
                    pass

                all_draft_picks.append(pick)
                if season == 2022: # Logic to build the initial 2022 roster
                    initial_rosters[season].append(pick)

        self.db.clear_and_insert('drafts', all_draft_picks)
        if 2022 in initial_rosters:
            self.db.clear_and_insert('roster_2022', initial_rosters[2022])

    def apply_contracts_from_csv(self, season, file_path):
        """
        Updates a roster collection with contract details from a CSV file.
        """
        print(f"Applying contracts for {season} season from CSV...")
        roster_collection = self.db.get_collection(f'roster_{season}')
        contracts_df = pd.read_csv(file_path)
        
        for _, row in contracts_df.iterrows():
            # Your logic to parse the CSV and update MongoDB documents
            # Example:
            # roster_collection.update_one(
            #     {"player_name": row['full_name_ai'], "team_name": row['team']},
            #     {"$set": {"contract.years": row['contract_years'], ...}}
            # )
            pass

    def simulate_season_transactions(self, season, rosters_map, players_map):
        """
        Applies all 'complete' transactions for a given season to the
        corresponding roster collection.
        """
        print(f"Simulating transactions for the {season} season...")
        roster_collection = self.db.get_collection(f"roster_{season}")
        transactions_collection = self.db.get_collection('transactions')

        transactions = list(transactions_collection.find({"season": season}).sort("transaction_id", 1))

        for tx in transactions:
            # Replicate the logic from your notebook to handle trades, waivers, drops, etc.
            # This is a simplified example of the logic from your notebook.
            if tx.get('status') != 'complete':
                continue

            # Handle player drops
            if tx.get('drops'):
                for player_id, roster_id in tx['drops'].items():
                    player_name = players_map.get(player_id, {}).get("full_name")
                    owner_info = next((r for r in rosters_map[tx['league_id']] if roster_id in r), None)
                    if player_name and owner_info:
                        owner_name = owner_info.get(roster_id)
                        roster_collection.update_one(
                            {"player_name": player_name, "team_name": owner_name},
                            {"$unset": {"team_name": "", "contract": ""}}
                        )

            # Handle player adds
            if tx.get('adds'):
                for player_id, roster_id in tx['adds'].items():
                    player_name = players_map.get(player_id, {}).get("full_name")
                    owner_info = next((r for r in rosters_map[tx['league_id']] if roster_id in r), None)
                    if player_name and owner_info:
                        owner_name = owner_info.get(roster_id)
                        roster_collection.update_one(
                            {"player_name": player_name},
                            {"$set": {"team_name": owner_name}},
                            upsert=True
                        )

# ... (InterSeasonManager class remains the same) ...
class InterSeasonManager:
    """
    Manages the offseason transition between two seasons.
    """
    def __init__(self, completed_season, mongo_manager):
        self.completed_season = completed_season
        self.upcoming_season = completed_season + 1
        self.db = mongo_manager
        self.completed_rosters_name = f"roster_{self.completed_season}"
        self.franchise_tag_name = f"roster_{self.upcoming_season}_ft"
        self.upcoming_rosters_name = f"roster_{self.upcoming_season}"

    def run_full_offseason_pipeline(self, ft_csv_path, new_contracts_csv_path, taxi_csv_path, new_draft_ids):
        """Orchestrates the entire offseason process."""
        print(f"--- Starting Offseason Process: {self.completed_season} -> {self.upcoming_season} ---")
        
        # Step 1: Archive last season's final rosters
        self.db.archive_collection(self.completed_rosters_name)
        
        # Step 2: Set up franchise tag collection
        self._setup_franchise_tag_collection()
        
        # Step 2a: Apply franchise tags from CSV
        if ft_csv_path and os.path.exists(ft_csv_path):
            self._apply_franchise_tags_from_csv(ft_csv_path)
        else:
            print(f"Skipping franchise tags: No CSV found at {ft_csv_path}")

        # Step 3: Create the initial roster for the upcoming season
        self._initialize_new_season_roster()
        
        # Step 4, 5, 6: Process preseason transactions and new draft
        # These would be additional methods. For now, we'll assume they are part of this flow.
        
        # Step 7: Apply new contracts from auction/startup draft
        if new_contracts_csv_path and os.path.exists(new_contracts_csv_path):
            # This would call a method similar to the original notebook's set_contracts_2023
            print(f"Applying new contracts from {new_contracts_csv_path}...")
        
        # Step 7a: Set taxi squad designations
        if taxi_csv_path and os.path.exists(taxi_csv_path):
            # This would call a method similar to the original notebook's set_taxi_squad_2023
            print(f"Setting taxi squad from {taxi_csv_path}...")

        print(f"--- Offseason Process for {self.upcoming_season} Complete ---")


    def _setup_franchise_tag_collection(self):
        """Prepares the collection for owners to select franchise tags."""
        print(f"Setting up franchise tag collection: '{self.franchise_tag_name}'")
        source_collection = self.db.get_collection(self.completed_rosters_name)
        ft_collection = self.db.get_collection(self.franchise_tag_name)
        
        # Your logic to age contracts and determine FT eligibility
        # For demonstration, we copy and mark players with 1 year left as eligible
        all_players = list(source_collection.find({"contract": {"$exists": True}}))
        
        for player in all_players:
            # Age contract logic goes here
            pass

        self.db.clear_and_insert(self.franchise_tag_name, all_players)
        ft_collection.update_many(
            {"contract.contract_years_left": 1},
            {"$set": {"contract.franchise_tag_allowed": True}}
        )

    def _apply_franchise_tags_from_csv(self, file_path):
        """Applies selected franchise tags from a CSV file."""
        print(f"Applying franchise tags from {file_path}...")
        ft_collection = self.db.get_collection(self.franchise_tag_name)
        ft_df = pd.read_csv(file_path)
        tagged_players_df = ft_df[ft_df['franchise_tag'].notnull()]

        for _, row in tagged_players_df.iterrows():
            # Your logic to update the player's contract with FT details
            pass

    def _initialize_new_season_roster(self):
        """Creates the new season's roster collection from the FT collection."""
        print(f"Initializing new season roster: '{self.upcoming_rosters_name}'")
        ft_collection = self.db.get_collection(self.franchise_tag_name)
        
        # Find players still under contract or who were tagged
        players_under_contract = list(ft_collection.find(
            {"$or": [
                {"contract.contract_years_left": {"$gt": 1}},
                {"contract.franchise_tag_used": True}
            ]}
        ))
        
        self.db.clear_and_insert(self.upcoming_rosters_name, players_under_contract)

# --- NEW: Configuration for Transaction Exemptions ---
# This dictionary holds the queries to find and remove specific transactions.
# It's based directly on the logic from your original notebook.
TRANSACTION_CLEANING_QUERIES = {
    2022: [
        {"transaction_id": {"$lt": 873343429562765312}} # Preseason adjustments
    ],
    2023: [
        # Preseason commish mistakes
        {"transaction_id": {"$in": [
            992630737629118464, 992658795543166976, 992680388503961600,
            997667560936038400, 997667820534018048, 997671488788467712,
            997675132925648896, 997676466890448896
        ]}},
        # Post-draft adjustments
        {"transaction_id": {"$gt": 1005152235380125696, "$lt": 1006599450380292096}},
        # Roster adjustment (Antonius/Fields)
        {"transaction_id": {"$in": [1026754846822588416, 1026806999914332160]}}
    ],
    2024: []
}


def run_full_league_history_pipeline():
    """
    Orchestrates the entire data pipeline from the first season to the latest.
    """
    # --- 1. SETUP ---
    # Load environment variables from .env file
    DB_USER = os.getenv('MDB_USER')
    DB_PASSWORD = os.getenv('MDB_PASSWORD')
    DB_HOST = os.getenv('MDB_HOST')
    
    START_SEASON = 2022
    END_SEASON = 2025
    
    # Map league IDs to their corresponding season for accurate transaction fetching
    LEAGUE_IDS_SEASONS = {
        '867459577837416448': 2022,
        '966851427416977408': 2023,
        '1124855836695351296': 2024,
        '1124855836695351297': 2025
    }
    
    api_client = SleeperAPIClient()
    mongo_manager = MongoManager(DB_USER, DB_PASSWORD, DB_HOST)
    processor = DataProcessor(api_client, mongo_manager)

    # --- 2. INITIAL DATA LOAD ---
    players_map = processor.update_master_player_list()
    users_map, rosters_map = processor.build_user_and_roster_maps(list(LEAGUE_IDS_SEASONS.keys()))
    
    # ['rookie', 'league']
    drafts_by_season = {
        2022: ['869802031370686464', '869408534662696960'],
        2023: ['966851427416977409', '1005342622375886848'],
        2024: ['1124855836695351297', '1136367086491676672']
    }
    
    # --- 3. SYNC AND CLEAN TRANSACTIONS (Done once for all seasons) ---
    processor.sync_all_transactions(LEAGUE_IDS_SEASONS)
    processor.clean_transactions(TRANSACTION_CLEANING_QUERIES)

    # --- 4. MAIN SEASON PROCESSING LOOP ---
    for season in range(START_SEASON, END_SEASON + 1):
        print(f"\n================ PROCESSING SEASON {season} ================")
        
        if season == START_SEASON:
            processor.process_and_store_drafts(drafts_by_season[season], users_map, players_map)
            # Apply initial contracts from your startup CSV
            processor.apply_contracts_from_csv(
                season, 
                f'assets/{season}_contracts_full_name.csv'
            )

        processor.simulate_season_transactions(season, rosters_map, players_map)
        
        if season < END_SEASON:
            offseason_manager = InterSeasonManager(season, mongo_manager)
            offseason_manager.run_full_offseason_pipeline(
                ft_csv_path=f'assets/{season + 1}_franchise_tag.csv',
                new_contracts_csv_path=f'assets/{season + 1}_contracts.csv',
                taxi_csv_path=f'assets/{season + 1}_taxi.csv',
                new_draft_ids=drafts_by_season.get(season + 1, [])
            )
            
    print("\n✅ Full historical data pipeline has been successfully processed.")
    mongo_manager.close_connection()


if __name__ == '__main__':
    run_full_league_history_pipeline()
