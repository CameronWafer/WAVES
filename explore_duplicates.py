"""
Simple Duplicate Exploration
Copy this into a separate cell for quick exploration of duplicates
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("QUICK DUPLICATE EXPLORATION")
print("=" * 80)

if 'behav_copy' not in locals():
    print("✗ behav_copy not found. Run main processing first.")
elif 'duplicates_df' not in locals():
    print("✗ duplicates_df not found. Run the duplicate analysis cell first.")
else:
    # Quick stats
    print(f"\nDuplicates dataframe shape: {duplicates_df.shape}")
    print(f"Columns: {list(duplicates_df.columns)}")
    
    # Show more samples
    print("\n" + "=" * 80)
    print("Sample 100-150 of duplicates:")
    print("=" * 80)
    print(duplicates_df.iloc[100:150])
    
    # Group by key columns to see patterns
    print("\n" + "=" * 80)
    print("Grouping by (id, obs, rel_time) to see duplicate patterns:")
    print("=" * 80)
    if all(c in duplicates_df.columns for c in ['id', 'obs', 'rel_time']):
        grouped = duplicates_df.groupby(['id', 'obs', 'rel_time']).size().reset_index(name='count')
        grouped = grouped.sort_values('count', ascending=False)
        print(f"\nTop 30 most duplicated (id, obs, rel_time) combinations:")
        print(grouped.head(30))
        
        # Show actual rows for top duplicates
        print("\n" + "=" * 80)
        print("Actual rows for top 5 duplicate patterns:")
        print("=" * 80)
        for idx, row in grouped.head(5).iterrows():
            print(f"\nPattern {idx+1}: id={row['id']}, obs={row['obs']}, rel_time={row['rel_time']}, appears {row['count']} times")
            pattern_rows = duplicates_df[
                (duplicates_df['id'] == row['id']) & 
                (duplicates_df['obs'] == row['obs']) & 
                (duplicates_df['rel_time'] == row['rel_time'])
            ]
            print(pattern_rows.head(3))
            if len(pattern_rows) > 3:
                print(f"... and {len(pattern_rows) - 3} more identical rows")
    
    # Check if duplicates have different values in other columns
    print("\n" + "=" * 80)
    print("Checking if duplicates differ in activity_type, posture_waves, intensity:")
    print("=" * 80)
    
    if all(c in duplicates_df.columns for c in ['id', 'obs', 'rel_time', 'activity_type', 'posture_waves', 'intensity']):
        # Group by key columns and check variance in other columns
        variance_check = duplicates_df.groupby(['id', 'obs', 'rel_time']).agg({
            'activity_type': ['nunique', lambda x: list(x.unique())[:3]],
            'posture_waves': ['nunique', lambda x: list(x.unique())[:3]],
            'intensity': ['nunique', lambda x: list(x.unique())[:3]],
        }).reset_index()
        
        variance_check.columns = ['id', 'obs', 'rel_time', 'activity_type_nunique', 'activity_type_values',
                                 'posture_waves_nunique', 'posture_waves_values',
                                 'intensity_nunique', 'intensity_values']
        
        # Find duplicates that differ in these columns
        differing = variance_check[
            (variance_check['activity_type_nunique'] > 1) |
            (variance_check['posture_waves_nunique'] > 1) |
            (variance_check['intensity_nunique'] > 1)
        ]
        
        print(f"\nDuplicates that differ in activity_type, posture_waves, or intensity: {len(differing):,}")
        if len(differing) > 0:
            print("\nSample of differing duplicates:")
            print(differing.head(20))
        else:
            print("✓ All duplicates are identical across all columns")
    
    # Check start_time_new in duplicates
    if 'start_time_new' in duplicates_df.columns:
        print("\n" + "=" * 80)
        print("start_time_new in duplicates:")
        print("=" * 80)
        stn_nulls = duplicates_df['start_time_new'].isna().sum()
        print(f"Null start_time_new in duplicates: {stn_nulls:,} / {len(duplicates_df):,} ({stn_nulls/len(duplicates_df)*100:.2f}%)")
        
        if stn_nulls < len(duplicates_df):
            print("\nSample duplicates with start_time_new values:")
            print(duplicates_df[duplicates_df['start_time_new'].notna()].head(20))

