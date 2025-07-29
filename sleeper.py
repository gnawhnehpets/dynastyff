import requests
import pandas as pd
import math
from pymongo import MongoClient
import os
import json # Added for saving JSON data

from dotenv import load_dotenv
load_dotenv()

# Helper function to save top N results
def save_top_n_results(data, subdir, filename, n=5):
    if isinstance(data, dict):
        # If it's a dict (like players), convert values to a list for slicing
        data_to_save = list(data.values())
    elif isinstance(data, list):
        data_to_save = data
    else:
        # If data is not a list or dict, just save it as is (e.g., metadata)
        data_to_save = data

    # Ensure data_to_save is iterable before slicing
    if isinstance(data_to_save, (list, dict)):
        top_n = data_to_save[:n] if isinstance(data_to_save, list) else {k: data_to_save[k] for i, k in enumerate(data_to_save) if i < n}
    else:
        top_n = data_to_save # For non-list/dict data, save as is

    filepath = f"{subdir}/{filename}"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(top_n, f, indent=4)
    print(f"Saved top {n} results to {filepath}")


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
        data = response.json()
        save_top_n_results(data, "output", "top_players.json")
        return data

    def get_league_info(self, league_id, season):
        """Fetches basic information for a given league."""
        url = f"{self.BASE_URL}/league/{league_id}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        save_top_n_results(data, "output", f"top_league_info_{season}.json")
        return data

    def get_league_users(self, league_id, season):
        """Fetches all users in a specific league."""
        url = f"{self.BASE_URL}/league/{league_id}/users"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        save_top_n_results(data, "output", f"top_league_users_{season}.json")
        return data

    def get_league_rosters(self, league_id, season):
        """Fetches all rosters in a specific league."""
        url = f"{self.BASE_URL}/league/{league_id}/rosters"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        save_top_n_results(data, "output", f"top_league_rosters_{season}.json")
        return data

    def get_draft_metadata(self, draft_id, season):
        """Fetches metadata for a specific draft."""
        url = f"{self.BASE_URL}/draft/{draft_id}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        save_top_n_results(data, "output", f"top_draft_metadata_{season}.json")
        return data

    def get_draft_picks(self, draft_id, season):
        """Fetches all picks from a specific draft."""
        url = f"{self.BASE_URL}/draft/{draft_id}/picks"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        save_top_n_results(data, "output", f"top_draft_picks_{season}.json")
        return data

    def get_weekly_transactions(self, league_id, week, season):
        """Fetches all transactions for a given league and week."""
        url = f"{self.BASE_URL}/league/{league_id}/transactions/{week}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if week == 17:
            save_top_n_results(data, "output", f"top_weekly_transactions_{season}_week_{week}.json")
        return data


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

    def reset_collections(self, collection_names):
        """
        Resets (clears) a list of specified MongoDB collections.
        """
        print(f"Resetting collections: {', '.join(collection_names)}...")
        for name in collection_names:
            collection = self.get_collection(name)
            collection.delete_many({})
            print(f"  - Cleared collection '{name}'.")

class DataProcessor:
    """
    Handles the processing and transformation of fantasy football data.
    """
    def __init__(self, api_client, mongo_manager):
        self.api = api_client
        self.db = mongo_manager

    def process_and_store_drafts(self, drafts_by_season, users_map, players_map):
        """
        Processes draft data, assigns initial rookie and startup auction contracts, 
        and stores it in MongoDB.
        """
        print("Processing draft data with contract logic...")
        all_draft_picks = []
        initial_rosters = {}  # Keyed by season

        # Determine the earliest season to correctly identify the startup roster
        min_season = min(drafts_by_season.keys()) if drafts_by_season else None

        for season, draft_ids in drafts_by_season.items():
            for draft_id in draft_ids:
                metadata = self.api.get_draft_metadata(draft_id, season)
                picks = self.api.get_draft_picks(draft_id, season)
                
                league_id = metadata["league_id"]
                draft_type = "league" if len(picks) > 100 else "rookie"
                
                print(f"  - Processing {season} {draft_type} draft for league {league_id}...")
                
                if season not in initial_rosters:
                    initial_rosters[season] = []

                for pick in picks:
                    player_id = pick["player_id"]
                    owner_id = pick["picked_by"]
                    pick_no = pick["pick_no"]

                    pick.update({
                        "league_id": league_id,
                        "season": season,
                        "draft_type": draft_type,
                        "team_name": users_map.get(owner_id),
                        "player_name": players_map.get(player_id, {}).get("full_name"),
                        "contract": {}
                    })
                    
                    # --- IMPLEMENTED CONTRACT LOGIC ---
                    if draft_type == "rookie":
                        cost = 0
                        years_left = 3
                        if pick_no <= 5: cost = 15
                        elif pick_no <= 10: cost = 10
                        elif pick_no <= 20: cost = 5
                        elif pick_no <= 30: cost = 3
                        else: cost = 1
                        
                        pick["needs_contract_status"] = False
                        pick["contract"].update({
                            "drafted_in": season,
                            "y0_cost": cost,
                            "y1_cost": int(math.ceil(cost * 1.4)),
                            "y2_cost": int(math.ceil(int(math.ceil(cost * 1.4)) * 1.4)),
                            "contract_years_left": 3,
                            "free_agent_before_season": season + years_left
                        })
                    else:  # 'league' or startup draft
                        if pick['metadata'].get('years_exp') != "0": # Exclude rookies from startup draft data
                            pick["needs_contract_status"] = True
                            cost = int(pick["metadata"]["amount"])
                            pick["contract"].update({
                                "y0_cost": cost,
                                "contract_years_left": 1,
                                "free_agent_before_season": season + 1
                            })
                    
                    all_draft_picks.append(pick)
                    
                    # Build the initial roster only for the league's first season
                    if season == min_season:
                        # Exclude rookies from the startup roster as they are added via their own draft
                        if draft_type == "league" and pick['metadata'].get('years_exp') != "0":
                            initial_rosters[season].append(pick)
                        elif draft_type == "rookie":
                            initial_rosters[season].append(pick)

        self.db.clear_and_insert('drafts', all_draft_picks)
        if min_season and min_season in initial_rosters:
            self.db.clear_and_insert(f'roster_{min_season}', initial_rosters[min_season])
    
    # ... (all other DataProcessor methods remain the same as the previous response) ...
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
                    transactions = self.api.get_weekly_transactions(league_id, week, season)
                    for tx in transactions:
                        if tx.get("status") == "complete":
                            tx["season"] = int(season)
                            tx["league_id"] = league_id
                            tx["transaction_id"] = int(tx["transaction_id"])
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

    def build_user_and_roster_maps(self, league_ids):
        """
        Builds mappings of user IDs to display names, league IDs to rosters,
        and league IDs to their season.
        """
        print("Building user, roster, and season maps...")
        users_map = {}
        rosters_map = {}
        league_season_map = {}
        for league_id in league_ids:
            try:
                # Get the season from the league info endpoint, which is the source of truth
                league_info = self.api.get_league_info(league_id, 0) # season param is for logging, can be 0
                season = int(league_info["season"])
                league_season_map[league_id] = season

                users_data = self.api.get_league_users(league_id, season)
                if not isinstance(users_data, list):
                    print(f"Warning: Unexpected user data format for league {league_id}. Skipping users.")
                    users_data = []
                for user in users_data:
                    users_map[user["user_id"]] = user["display_name"]
                
                roster_data = self.api.get_league_rosters(league_id, season)
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
                rosters_map[league_id] = []
            except Exception as e:
                print(f"An unexpected error occurred for league {league_id}: {e}. Skipping this league.")
                rosters_map[league_id] = []
        return users_map, rosters_map, league_season_map

    def apply_contracts_from_csv(self, season, file_path):
        """
        Updates a roster collection with detailed contract specifics for drafted
        veterans from a CSV file.
        """
        if not os.path.exists(file_path):
            print(f"Warning: Contracts file not found at '{file_path}'. Skipping contract application for {season}.")
            return

        print(f"Applying contracts for {season} season from CSV: {file_path}")
        roster_collection = self.db.get_collection(f'roster_{season}')
        
        try:
            contracts_df = pd.read_csv(file_path)
            # Filter for rows where a contract is explicitly defined
            leaguedraft_df = contracts_df[contracts_df['contract_years'].notnull()]
        except (FileNotFoundError, KeyError) as e:
            print(f"Error reading or processing contracts CSV: {e}. Please check the file format.")
            return

        updates_made = 0
        for _, row in leaguedraft_df.iterrows():
            player_name = row['full_name_ai']
            team_name = row['team']
            contract_years = int(row['contract_years'])
            y0_cost = int(row['y0'])

            update_payload = {
                "contract.y0_cost": y0_cost,
                "contract.contract_years_left": contract_years,
                "needs_contract_status": False
            }

            if contract_years == 2:
                y1_cost = math.ceil(y0_cost * 1.2)
                update_payload.update({
                    "contract.y1_cost": y1_cost,
                    "contract.free_agent_before_season": season + 2
                })
            elif contract_years == 3:
                y1_cost = math.ceil(y0_cost * 1.2)
                y2_cost = math.ceil(y1_cost * 1.2)
                update_payload.update({
                    "contract.y1_cost": y1_cost,
                    "contract.y2_cost": y2_cost,
                    "contract.free_agent_before_season": season + 3
                })
            else: # Default to 1-year contract
                update_payload["contract.free_agent_before_season"] = season + 1

            result = roster_collection.update_one(
                {"player_name": player_name, "team_name": team_name},
                {"$set": update_payload}
            )
            if result.modified_count > 0:
                updates_made += 1
        
        print(f"Applied contract details for {updates_made} players for the {season} season.")

    def simulate_season_transactions(self, season, rosters_map, players_map):
        """
        Applies all 'complete' transactions for a given season to the
        corresponding roster collection and correctly tracks dropped players,
        excluding trades.
        """
        print(f"Simulating transactions for the {season} season...")
        roster_collection = self.db.get_collection(f"roster_{season}")
        transactions_collection = self.db.get_collection('transactions')
        drafts_collection = self.db.get_collection('drafts')

        transactions = list(transactions_collection.find({"season": season}).sort("transaction_id", 1))

        rookies_dropped = []
        multiyear_dropped = []

        for tx in transactions:
            if tx.get('status') != 'complete':
                continue
            
            transaction_type = tx.get('type')

            # Handle player drops
            if tx.get('drops'):
                for player_id, roster_id in tx['drops'].items():
                    player_name = players_map.get(player_id, {}).get("full_name")
                    # Find the league_id associated with the transaction
                    tx_league_id = tx.get('league_id')
                    owner_info = next((r for r in rosters_map.get(tx_league_id, []) if roster_id in r), None)

                    if player_name and owner_info:
                        owner_name = owner_info.get(roster_id)
                        
                        # --- MODIFIED LOGIC: Check transaction type before logging a drop ---
                        # A player is only truly "dropped" if it's not a trade.
                        if transaction_type != 'trade':
                            player_data = roster_collection.find_one(
                                {"player_name": player_name, "team_name": owner_name}
                            )

                            if player_data:
                                dropped_info = {
                                    "player_name": player_name, "team_name": owner_name,
                                    "season": season, "transaction_id": tx.get("transaction_id"),
                                    "week": tx.get("leg")
                                }
                                
                                if player_data.get("draft_type") == "rookie":
                                    rookies_dropped.append(dropped_info)
                                
                                contract = player_data.get("contract", {})
                                if contract.get("contract_years_left", 0) > 1:
                                    multiyear_dropped.append({**dropped_info, **contract})

                        # The database update happens regardless of transaction type
                        roster_collection.update_one(
                            {"player_name": player_name, "team_name": owner_name},
                            {"$unset": {"team_name": "", "contract": ""}}
                        )

            # Handle player adds
            if tx.get('adds'):
                for player_id, roster_id in tx['adds'].items():
                    player_name = players_map.get(player_id, {}).get("full_name")
                    tx_league_id = tx.get('league_id')
                    owner_info = next((r for r in rosters_map.get(tx_league_id, []) if roster_id in r), None)

                    if player_name and owner_info:
                        owner_name = owner_info.get(roster_id)
                        
                        draft_info = drafts_collection.find_one({"player_id": player_id, "season": season})
                        player_draft_type = draft_info.get("draft_type") if draft_info else None

                        update_fields = {"team_name": owner_name}
                        if player_draft_type:
                            update_fields["draft_type"] = player_draft_type

                        roster_collection.update_one(
                            {"player_name": player_name},
                            {"$set": update_fields},
                            upsert=True
                        )
        
        # Save dropped player data to CSV
        if rookies_dropped:
            pd.DataFrame(rookies_dropped).to_csv(f'assets/{season}_rookies_dropped_during_season.csv', index=False)
            print(f"Saved {len(rookies_dropped)} rookies dropped in {season} to assets/{season}_rookies_dropped_during_season.csv")
        else:
            print(f"No rookies dropped (non-trade) in {season}.")

        if multiyear_dropped:
            pd.DataFrame(multiyear_dropped).to_csv(f'assets/{season}_multiyear_dropped_during_season.csv', index=False)
            print(f"Saved {len(multiyear_dropped)} multi-year contract players dropped in {season} to assets/{season}_multiyear_dropped_during_season.csv")
        else:
            print(f"No multi-year contract players dropped (non-trade) in {season}.")


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
        
        # Logic to age contracts and determine FT eligibility, based on notebook
        source_players = list(source_collection.find({"contract": {"$exists": True}}))
        aged_players = []

        for player in source_players:
            contract = player.get("contract", {})
            
            # General contract aging for all players
            contract["contract_years_left"] -= 1
            contract["free_agent_before_season"] -= 1

            # Taxi squad logic: contract tolls for a year, effectively reversing the aging
            if contract.get("taxi_designation"):
                if "y2_cost" in contract: contract["y3_cost"] = contract["y2_cost"]
                if "y1_cost" in contract: contract["y2_cost"] = contract["y1_cost"]
                if "y0_cost" in contract: contract["y1_cost"] = contract["y0_cost"]
                # Reverse the aging for years left and FA season
                contract["contract_years_left"] += 1
                contract["free_agent_before_season"] += 1
                # Taxi designation is for one season, so we remove it
                contract["taxi_designation"] = False
            
            player["contract"] = contract
            aged_players.append(player)

        self.db.clear_and_insert(self.franchise_tag_name, aged_players)
        
        # Set franchise tag eligibility
        ft_collection.update_many(
            {"contract.contract_years_left": 0},
            {"$set": {"contract.franchise_tag_allowed": True}}
        )
        print(f"Aged contracts and set up {len(aged_players)} players in '{self.franchise_tag_name}'.")

    def _apply_franchise_tags_from_csv(self, file_path):
        """Applies selected franchise tags from a CSV file."""
        print(f"Applying franchise tags from {file_path}...")
        ft_collection = self.db.get_collection(self.franchise_tag_name)
        try:
            ft_df = pd.read_csv(file_path)
            tagged_players_df = ft_df[ft_df['franchise_tag'].notnull()]
        except (FileNotFoundError, KeyError) as e:
            print(f"Error reading or processing franchise tag CSV: {e}. Skipping.")
            return

        updates_made = 0
        for _, row in tagged_players_df.iterrows():
            player_name = row['full_name_ai']
            team_name = row['team']
            
            player_doc = ft_collection.find_one({"player_name": player_name, "team_name": team_name})
            
            if player_doc and player_doc.get("contract", {}).get("franchise_tag_allowed"):
                original_cost = player_doc.get("contract", {}).get("y0_cost", 0)
                franchise_cost = int(math.ceil(original_cost * 1.2))
                
                result = ft_collection.update_one(
                    {"_id": player_doc["_id"]},
                    {"$set": {
                        "contract.y1_cost": franchise_cost,
                        "contract.franchise_tag_used": True,
                        "contract.franchise_tag_allowed": False,
                        "contract.contract_years_left": 1, # A franchise tag is a 1-year deal
                        "contract.free_agent_before_season": self.upcoming_season + 1
                    }}
                )
                if result.modified_count > 0:
                    updates_made += 1
                    print(f"  - Applied franchise tag to {player_name} for {team_name} at cost ${franchise_cost}.")
            else:
                print(f"  - Warning: Could not apply tag to {player_name} for {team_name}. Player not found or not eligible.")
        
        print(f"Applied {updates_made} franchise tags.")

    def _initialize_new_season_roster(self):
        """
        Creates the new season's roster by carrying over players with remaining
        contracts from the franchise tag collection and aging their contracts.
        """
        print(f"Initializing new season roster: '{self.upcoming_rosters_name}'")
        ft_collection = self.db.get_collection(self.franchise_tag_name)
        
        # Find players who will be on the roster for the new season.
        # This includes anyone with years left on their contract after the initial aging.
        players_to_carry_over_cursor = ft_collection.find({
            "contract.contract_years_left": {"$gt": 0}
        })
        
        new_roster_players = []
        for player in players_to_carry_over_cursor:
            contract = player.get("contract", {})
            
            # Age the contract values for the new season
            # y1 becomes y0, y2 becomes y1, etc.
            contract["y0_cost"] = contract.get("y1_cost")
            contract["y1_cost"] = contract.get("y2_cost")
            contract["y2_cost"] = contract.get("y3_cost")

            # Remove keys that are now null
            if contract["y1_cost"] is None: del contract["y1_cost"]
            if contract["y2_cost"] is None: del contract["y2_cost"]
            if "y3_cost" in contract: del contract["y3_cost"]

            player["contract"] = contract
            player["season"] = self.upcoming_season
            player["needs_contract_status"] = False
            
            new_roster_players.append(player)
            
        self.db.clear_and_insert(self.upcoming_rosters_name, new_roster_players)
        print(f"Initialized '{self.upcoming_rosters_name}' with {len(new_roster_players)} players carried over.")

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
            997672269440774144, 997673326384390144, 997673167717994496,
            997674584004464640, 997675604331868160, 997675716969844736,
            997675834104242176, 997675132925648896, 997676466890448896
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
    END_SEASON = 2024
    
    # Hardcoded list of all league IDs across all seasons
    ALL_LEAGUE_IDS = [
        '867459577837416448',
        '966851427416977408',
        '1124855836695351296',
        '1111111111111111111' # Example placeholder
    ]
    
    api_client = SleeperAPIClient()
    mongo_manager = MongoManager(DB_USER, DB_PASSWORD, DB_HOST)
    processor = DataProcessor(api_client, mongo_manager)

    # --- 2. INITIAL DATA LOAD AND MAP BUILDING ---
    players_map = processor.update_master_player_list()
    users_map, rosters_map, league_season_map = processor.build_user_and_roster_maps(ALL_LEAGUE_IDS)
    
    # Define all collections that should be reset at the start
    collections_to_reset = ['players', 'transactions', 'drafts']
    # Determine the maximum season from the dynamically fetched map
    if league_season_map:
        max_season_in_config = max(league_season_map.values())
        for season_val in range(START_SEASON, max_season_in_config + 2):
            collections_to_reset.append(f'roster_{season_val}')
            collections_to_reset.append(f'roster_{season_val}_archival')
            collections_to_reset.append(f'roster_{season_val}_ft')
    
    mongo_manager.reset_collections(collections_to_reset)

    # ['rookie', 'league']
    drafts_by_season = {
        2022: ['869802031370686464', '869408534662696960'],
        2023: ['966851427416977409', '1005342622375886848'],
        2024: ['1124855836695351297', '1136367086491676672']
    }
    
    # --- 3. SYNC AND CLEAN TRANSACTIONS (Done once for all seasons) ---
    processor.sync_all_transactions(league_season_map) # Use the dynamic map
    processor.clean_transactions(TRANSACTION_CLEANING_QUERIES)

    # --- 4. MAIN SEASON PROCESSING LOOP ---
    for season in range(START_SEASON, END_SEASON + 1):
        print(f"\n================ PROCESSING SEASON {season} ================")
        
        if season == START_SEASON:
            processor.process_and_store_drafts(drafts_by_season, users_map, players_map) # Pass the dictionary directly
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
