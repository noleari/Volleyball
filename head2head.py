import os, fnmatch
import pandas as pd
from copy import deepcopy


def get_head2head(dropna = False, fillna = False):
    """grabs schedule and head to head data from the "all_schedules" folder in the same directory. Returns two items; 
    The first is a DataFrame containing all the valid games from our dataset with the format date, t1, t1_stats, ..., t2, t2_stats, ..., result. Result will be 0 if t1 won and 1 if t2 won.
    The second is a dictionary containing all team schedules with the format filename: DataFrame. Filename will have the format '{team_name}-schedule-{year}.csv'.

    Get the necessary data with a line similar to 'games_master, all_schedules = get_head2head()'

    Args:
        dropna (bool, optional): drop NA values in main dataframe. Defaults to False.
        fillna (bool, optional): fill NA values with 0 in main dataframe. Defaults to False.

    Raises:
        Exception: when dropna and fillna are both true

    Returns:
        tuple(pandas.DataFrame, dict): tuple of main datframe and dict containing all schedules as dataframes
    """
    
    
    global master_cols
    columns = deepcopy(master_cols)
    
    if dropna and fillna:
        raise Exception("dropna and fillna cannot both be true")
    
    all_schedules = {}
    for root, dirs, filenames in os.walk("all_schedules"):
        for filename in fnmatch.filter(filenames, "*.csv"):
            try:
                all_schedules[filename] = pd.read_csv(os.path.join(root, filename), delimiter=",")
            except pd.errors.EmptyDataError as e:
                print(filename, "is empty")
                
    # Collect one-sided game stats into matching games - "{date}~{team_name1}~{team_name2}"
    games = {}

    for key, df in all_schedules.items():
        team_name1 = key.split("-schedule")[0].strip()
        for i, row in df.iterrows():
            team_name2 = row["opponent"].strip()
            date = row["date"]
            res = f"{date}~{team_name1}~{team_name2}" if team_name1 > team_name2 else f"{date}~{team_name2}~{team_name1}"
            
            if res not in games:
                games[res] = []
            games[res].append((team_name1, row))
            
            
    # Create dataframe from matched up games

    for name, item in games.items():
        if len(item) != 2: continue # Don't know why there are games that don't have two teams stats
        date, t1, t2 = name.split("~")
        row1 = item[0][1] if item[0][0] == t1 else item[1][1]
        row2 = item[0][1] if item[0][0] == t2 else item[1][1]
        winner = 0 if int(row1["result"].split("-")[0].strip()) == 3 else 1
        
        if row1['opponent/venue'] == row2['opponent/venue'] and row1['rolling_digs/set_total'] == row2['rolling_digs/set_total']:
            continue
        
        if int(row1["result"].split("-")[0].strip()) == 3 and int(row2["result"].split("-")[0].strip()) == 3:
            continue

        
        # add row to dataframe - yes it is ugly
        columns["result"].append(winner)
        columns["date"].append(date)
        columns["t1"].append(t1)
        columns["t1_rolling_kills/set_total"].append(row1["rolling_kills/set_total"])
        columns["t1_rolling_kills/set_3"].append(row1["rolling_kills/set_3"])
        columns["t1_rolling_errors/set_total"].append(row1["rolling_errors/set_total"])
        columns["t1_rolling_errors/set_3"].append(row1["rolling_errors/set_3"])
        columns["t1_rolling_total_attacks/set_total"].append(row1["rolling_total_attacks/set_total"])
        columns["t1_rolling_total_attacks/set_3"].append(row1["rolling_total_attacks/set_3"])
        columns["t1_rolling_hit_pct_total"].append(row1["rolling_hit_pct_total"])
        columns["t1_rolling_hit_pct_3"].append(row1["rolling_hit_pct_3"])
        columns["t1_rolling_assists/set_total"].append(row1["rolling_assists/set_total"])
        columns["t1_rolling_assists/set_3"].append(row1["rolling_assists/set_3"])
        columns["t1_rolling_aces/set_total"].append(row1["rolling_aces/set_total"])
        columns["t1_rolling_aces/set_3"].append(row1["rolling_aces/set_3"])
        columns["t1_rolling_serr/set_total"].append(row1["rolling_serr/set_total"])
        columns["t1_rolling_serr/set_3"].append(row1["rolling_serr/set_3"])
        columns["t1_rolling_digs/set_total"].append(row1["rolling_digs/set_total"])
        columns["t1_rolling_digs/set_3"].append(row1["rolling_digs/set_3"])
        columns["t1_rolling_b_solo/set_total"].append(row1["rolling_b_solo/set_total"])
        columns["t1_rolling_b_solo/set_3"].append(row1["rolling_b_solo/set_3"])
        columns["t1_rolling_b_assist/set_total"].append(row1["rolling_b_assist/set_total"])
        columns["t1_rolling_b_assist/set_3"].append(row1["rolling_b_assist/set_3"])
        columns["t1_rolling_b_error/set_total"].append(row1["rolling_b_error/set_total"])
        columns["t1_rolling_b_error/set_3"].append(row1["rolling_b_error/set_3"])
        columns["t1_rolling_pts/set_total"].append(row1["rolling_pts/set_total"])
        columns["t1_rolling_pts/set_3"].append(row1["rolling_pts/set_3"])
        columns["t2"].append(t2)
        columns["t2_rolling_kills/set_total"].append(row2["rolling_kills/set_total"])
        columns["t2_rolling_kills/set_3"].append(row2["rolling_kills/set_3"])
        columns["t2_rolling_errors/set_total"].append(row2["rolling_errors/set_total"])
        columns["t2_rolling_errors/set_3"].append(row2["rolling_errors/set_3"])
        columns["t2_rolling_total_attacks/set_total"].append(row2["rolling_total_attacks/set_total"])
        columns["t2_rolling_total_attacks/set_3"].append(row2["rolling_total_attacks/set_3"])
        columns["t2_rolling_hit_pct_total"].append(row2["rolling_hit_pct_total"])
        columns["t2_rolling_hit_pct_3"].append(row2["rolling_hit_pct_3"])
        columns["t2_rolling_assists/set_total"].append(row2["rolling_assists/set_total"])
        columns["t2_rolling_assists/set_3"].append(row2["rolling_assists/set_3"])
        columns["t2_rolling_aces/set_total"].append(row2["rolling_aces/set_total"])
        columns["t2_rolling_aces/set_3"].append(row2["rolling_aces/set_3"])
        columns["t2_rolling_serr/set_total"].append(row2["rolling_serr/set_total"])
        columns["t2_rolling_serr/set_3"].append(row2["rolling_serr/set_3"])
        columns["t2_rolling_digs/set_total"].append(row2["rolling_digs/set_total"])
        columns["t2_rolling_digs/set_3"].append(row2["rolling_digs/set_3"])
        columns["t2_rolling_b_solo/set_total"].append(row2["rolling_b_solo/set_total"])
        columns["t2_rolling_b_solo/set_3"].append(row2["rolling_b_solo/set_3"])
        columns["t2_rolling_b_assist/set_total"].append(row2["rolling_b_assist/set_total"])
        columns["t2_rolling_b_assist/set_3"].append(row2["rolling_b_assist/set_3"])
        columns["t2_rolling_b_error/set_total"].append(row2["rolling_b_error/set_total"])
        columns["t2_rolling_b_error/set_3"].append(row2["rolling_b_error/set_3"])
        columns["t2_rolling_pts/set_total"].append(row2["rolling_pts/set_total"])
        columns["t2_rolling_pts/set_3"].append(row2["rolling_pts/set_3"])
    
    master_df = pd.DataFrame(columns)
    master_df["date"] = pd.to_datetime(master_df["date"])
    master_df["t1_code"] = master_df["t1"].astype("category").cat.codes
    master_df["t2_code"] = master_df["t2"].astype("category").cat.codes
    
    if fillna:
        master_df = master_df.fillna(0)
        
    if dropna:
        master_df = master_df.dropna()
        
    return master_df, all_schedules
            
    







# Columns of end feature set
# t1 will always end up being team first alphabetically
# rolling_*_total is total cumulative average
# rolling_*_3 is 3 game cumulative - first three games of season for every team are NA
# team names are either not included or converted to categorical codes
# result is 0 for t1, 1 for t2

master_cols = {
    "date": [],
    "t1": [],
    "t1_rolling_kills/set_total": [],
    "t1_rolling_kills/set_3": [],
    "t1_rolling_errors/set_total": [],
    "t1_rolling_errors/set_3": [],
    "t1_rolling_total_attacks/set_total": [],
    "t1_rolling_total_attacks/set_3": [],
    "t1_rolling_hit_pct_total": [],
    "t1_rolling_hit_pct_3": [],
    "t1_rolling_assists/set_total": [],
    "t1_rolling_assists/set_3": [],
    "t1_rolling_aces/set_total": [],
    "t1_rolling_aces/set_3": [],
    "t1_rolling_serr/set_total": [],
    "t1_rolling_serr/set_3": [],
    "t1_rolling_digs/set_total": [],
    "t1_rolling_digs/set_3": [],
    "t1_rolling_b_solo/set_total": [],
    "t1_rolling_b_solo/set_3": [],
    "t1_rolling_b_assist/set_total": [],
    "t1_rolling_b_assist/set_3": [],
    "t1_rolling_b_error/set_total": [],
    "t1_rolling_b_error/set_3": [],
    "t1_rolling_pts/set_total": [],
    "t1_rolling_pts/set_3": [],
    "t2": [],
    "t2_rolling_kills/set_total": [],
    "t2_rolling_kills/set_3": [],
    "t2_rolling_errors/set_total": [],
    "t2_rolling_errors/set_3": [],
    "t2_rolling_total_attacks/set_total": [],
    "t2_rolling_total_attacks/set_3": [],
    "t2_rolling_hit_pct_total": [],
    "t2_rolling_hit_pct_3": [],
    "t2_rolling_assists/set_total": [],
    "t2_rolling_assists/set_3": [],
    "t2_rolling_aces/set_total": [],
    "t2_rolling_aces/set_3": [],
    "t2_rolling_serr/set_total": [],
    "t2_rolling_serr/set_3": [],
    "t2_rolling_digs/set_total": [],
    "t2_rolling_digs/set_3": [],
    "t2_rolling_b_solo/set_total": [],
    "t2_rolling_b_solo/set_3": [],
    "t2_rolling_b_assist/set_total": [],
    "t2_rolling_b_assist/set_3": [],
    "t2_rolling_b_error/set_total": [],
    "t2_rolling_b_error/set_3": [],
    "t2_rolling_pts/set_total": [],
    "t2_rolling_pts/set_3": [],
    "result": [] #0 for t1, 1 for t2
}