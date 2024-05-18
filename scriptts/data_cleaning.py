import pandas as pd

def load_data():
    match_df = pd.read_csv(r"C:\Users\DELL\Desktop\Amex_Superbowl\t20_prediction_project\data\664389efa0868_match_level_scorecard.csv")

    batsman_df = pd.read_csv(r"C:\Users\DELL\Desktop\Amex_Superbowl\t20_prediction_project\data\663e2b548c98c_batsman_level_scorecard.csv")
    bowler_df = pd.read_csv(r"C:\Users\DELL\Desktop\Amex_Superbowl\t20_prediction_project\data\663e2b2c60743_bowler_level_scorecard.csv")
    training_df = pd.read_csv(r"C:\Users\DELL\Desktop\Amex_Superbowl\t20_prediction_project\data\663e2b6d54457_train_data_with_samplefeatures.csv")
    return match_df, batsman_df, bowler_df, training_df

def clean_match_data(match_df):
    # Check for missing values
    print(match_df.isnull().sum())

    # Drop rows with missing critical information or impute them
    match_df = match_df.dropna(subset=['match id', 'team1_id', 'team2_id', 'inning1_runs', 'inning1_wickets', 'inning1_balls', 'inning2_runs', 'inning2_wickets', 'inning2_balls', 'winner_id'])
    
    #feature: run_rate_inning1
    #match_df['run_rate_inning1'] = match_df['inning1_runs'] / match_df['inning1_wickets']
    return match_df

def clean_batsman_data(batsman_df):
    batsman_df = batsman_df.dropna(subset=['match_id', 'batsman_id', 'inning', 'runs', 'balls_faced'])
    return batsman_df
    
def aggregate_batsman_data(batsman_df):
    batsman_agg = batsman_df.groupby(['match id', 'inning']).agg({
        'runs': 'sum',
        'balls_faced': 'sum'
    }).reset_index()
    #batsman_agg['strike_rate'] = (batsman_agg['runs'] / batsman_agg['balls_faced']) * 100
    return batsman_agg

def clean_bowler_data(bowler_df):
    bowler_df = bowler_df.dropna(subset=['match_id', 'bowler_id', 'inning', 'runs', 'wicket_count'])
    
    return bowler_df

def aggregate_bowler_data(bowler_df):
    bowler_agg = bowler_df.groupby(['match id', 'inning']).agg({
        'runs': 'sum',
        'wicket_count': 'sum'
    }).reset_index()
    #bowler_agg['economy_rate'] = bowler_agg['runs'] / bowler_agg['wicket_count']
    return bowler_agg

def merge_data(training_df, match_df, batsman_agg, bowler_agg):
    combined_df = match_df.merge(batsman_agg, on=['match id'], how='left')
    combined_df = combined_df.merge(bowler_agg, on=['match id'], how='left')
    final_df = training_df.merge(combined_df, on='match id', how='left')
    return final_df

def main():
    match_df, batsman_df, bowler_df, training_df = load_data()
    match_df = clean_match_data(match_df)
    batsman_agg = aggregate_batsman_data(batsman_df)
    bowler_agg = aggregate_bowler_data(bowler_df)
    final_df = merge_data(training_df, match_df, batsman_agg, bowler_agg)
    
    # Save the cleaned data
    final_df.to_csv(r"C:\Users\DELL\Desktop\Amex_Superbowl\t20_prediction_project\data\processed", index=False)

if __name__ == '__main__':
    main()
