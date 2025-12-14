"""
Quick diagnostic to check where date column is lost
Copy this into a notebook cell to run
"""

print("=" * 80)
print("CHECKING WHERE 'date' COLUMN IS LOST")
print("=" * 80)

# Check if date exists in log_df
if 'log_df' in locals():
    print(f"\n1. log_df columns: {list(log_df.columns)}")
    print(f"   'date' in log_df: {'date' in log_df.columns}")
    if 'date' in log_df.columns:
        print(f"   Sample date values: {log_df['date'].head(5).tolist()}")
else:
    print("\n✗ log_df not found")

# Check behav_am_df3 (after merge)
if 'behav_am_df3' in locals():
    print(f"\n2. behav_am_df3 columns: {list(behav_am_df3.columns)}")
    print(f"   'date' in behav_am_df3: {'date' in behav_am_df3.columns}")
else:
    print("   (behav_am_df3 may not exist if you haven't run that far)")

# Check behav_am_df4
if 'behav_am_df4' in locals():
    print(f"\n3. behav_am_df4 columns: {list(behav_am_df4.columns)}")
    print(f"   'date' in behav_am_df4: {'date' in behav_am_df4.columns}")
else:
    print("   (behav_am_df4 may not exist)")

# Check behav_am_df5
if 'behav_am_df5' in locals():
    print(f"\n4. behav_am_df5 columns: {list(behav_am_df5.columns)}")
    print(f"   'date' in behav_am_df5: {'date' in behav_am_df5.columns}")
else:
    print("   (behav_am_df5 may not exist)")

# Check behav_am_df6
if 'behav_am_df6' in locals():
    print(f"\n5. behav_am_df6 columns: {list(behav_am_df6.columns)}")
    print(f"   'date' in behav_am_df6: {'date' in behav_am_df6.columns}")
else:
    print("   (behav_am_df6 may not exist)")

# Check behav_am_df_7
if 'behav_am_df_7' in locals():
    print(f"\n6. behav_am_df_7 columns: {list(behav_am_df_7.columns)}")
    print(f"   'date' in behav_am_df_7: {'date' in behav_am_df_7.columns}")
    if 'date' not in behav_am_df_7.columns:
        print("   ✗ PROBLEM: 'date' is missing from behav_am_df_7!")
        print("\n   Checking what columns are available that might contain date info:")
        date_like_cols = [c for c in behav_am_df_7.columns if 'date' in c.lower() or 'time' in c.lower()]
        print(f"   Date/time related columns: {date_like_cols}")
else:
    print("\n✗ behav_am_df_7 not found")

print("\n" + "=" * 80)

