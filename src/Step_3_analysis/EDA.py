import numpy as np
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar

def main():
    """
    Analyzes Steam game review data using Dask for distributed processing.
    Performs three key EDA tasks: review volume analysis, sentiment analysis,
    and playtime distribution analysis.
    """
    # Enable progress bar for better visibility of Dask operations
    ProgressBar().register()
    
    # Load all parquet files in one call using Dask
    print("Reading parquet files...")
    df = dd.read_parquet("cleaned_data_polars/*.parquet")
    
    # Data preparation
    # ---------------
    # Handle missing values in key columns
    df['author_playtime_forever'] = df['author_playtime_forever'].fillna(0)
    # Convert boolean voted_up to integer for aggregation (True=1, False=0)
    df['voted_up_int'] = df['voted_up'].astype('bool').astype('int')
    # Convert playtime from minutes to hours
    df['playtime_hours'] = df['author_playtime_forever'] / 60.0
    
    # Task 1: Volume per game
    # ----------------------
    print("\n=== VOLUME PER GAME ===")
    # Count reviews per game
    game_review_counts = df.groupby('steam_appid').size().reset_index(name='review_count')
    # Compute and sort by review count (descending)
    top_games_by_volume = game_review_counts.compute().sort_values('review_count', ascending=False).head(20)
    print("Top 20 games by review count:")
    print(top_games_by_volume)
    
    # Task 2: Sentiment proxy – votes-up ratio
    # --------------------------------------
    print("\n=== SENTIMENT PROXY - VOTES-UP RATIO ===")
    # Calculate sum of positive reviews and total count per game
    sentiment_agg = df.groupby('steam_appid').agg({
        'voted_up_int': ['sum', 'count']
    }).compute()
    
    # Process results in pandas
    sentiment_agg.columns = ['votes_up_sum', 'review_count']
    sentiment_agg['positive_ratio'] = sentiment_agg['votes_up_sum'] / sentiment_agg['review_count']
    
    # Filter games with at least 100 reviews
    top_sentiment = (
        sentiment_agg[sentiment_agg['review_count'] >= 100]
        .sort_values('positive_ratio', ascending=False)
        .head(20)
        .reset_index()
    )
    
    print("Top 20 games by positive ratio (minimum 100 reviews):")
    print(top_sentiment[['steam_appid', 'review_count', 'positive_ratio']])
    
    # Task 3: Play-time distributions
    # -----------------------------
    print("\n=== PLAY-TIME DISTRIBUTIONS ===")
    
    # Dask requires separate computations for different percentiles
    playtime_stats_tasks = {
        'mean_hours': df.groupby('steam_appid')['playtime_hours'].mean(),
        'median_hours': df.groupby('steam_appid')['playtime_hours'].quantile(0.5),
        'percentile_95_hours': df.groupby('steam_appid')['playtime_hours'].quantile(0.95)
    }
    
    # Execute all Dask tasks and collect results
    playtime_stats_results = {k: v.compute() for k, v in playtime_stats_tasks.items()}
    
    # Combine into a single DataFrame
    playtime_stats_df = pd.DataFrame(playtime_stats_results)
    playtime_stats_df = playtime_stats_df.reset_index()
    
    print("Playtime statistics per game (showing first 20):")
    print(playtime_stats_df.head(20))
    
    # Generate global histogram of playtimes
    # ------------------------------------
    print("Generating playtime histogram...")
    
    # For large datasets, sample to avoid memory issues
    estimated_size = df.shape[0].compute()
    
    if estimated_size > 1_000_000:
        # Use a sampling fraction that gives us at most 1M records
        sample_frac = min(1_000_000 / estimated_size, 1.0)
        print(f"Sampling {sample_frac:.2%} of data for histogram ({estimated_size:,} records)")
        playtime_data = df['playtime_hours'].sample(frac=sample_frac).compute()
    else:
        # For smaller datasets, use all data
        playtime_data = df['playtime_hours'].compute()
    
    # Filter outliers for better visualization (keep playtimes under 1000 hours)
    filtered_playtime = playtime_data[playtime_data < 1000]
    
    # Create histogram with 1-hour bins
    plt.figure(figsize=(12, 8))
    plt.hist(filtered_playtime, bins=np.arange(0, 1000, 1), alpha=0.75)
    plt.title('Distribution of Playtime Hours (excluding outliers > 1000 hours)')
    plt.xlabel('Playtime (hours)')
    plt.ylabel('Number of Reviews')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Add vertical lines for key statistics
    global_mean = filtered_playtime.mean()
    global_median = filtered_playtime.median()
    
    plt.axvline(global_mean, color='r', linestyle='--', label=f'Mean: {global_mean:.2f} hours')
    plt.axvline(global_median, color='g', linestyle='--', label=f'Median: {global_median:.2f} hours')
    
    plt.legend()
    plt.tight_layout()
    
    # Save the histogram
    plt.savefig('playtime_hist.png', dpi=300)
    print("Histogram saved as 'playtime_hist.png'")

if __name__ == "__main__":
    main()