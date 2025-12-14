"""
Simple test script to verify the merge logic works correctly.
Copy this into a new notebook cell and run it to test the merge.
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("SIMPLE MERGE TEST")
print("=" * 80)

# ============================================================================
# Step 1: Create test data that mimics your actual data structure
# ============================================================================
print("\n1. CREATING TEST DATA")
print("-" * 80)

# Simulate behav_am_df with id and do extracted
behav_test = pd.DataFrame({
    'Observation': ['AM01DO1_J_FINAL_R', 'AM01DO2_M_FINAL_R', 'AM02DO1_J_FINAL_R', 'AM02DO2_J_FINAL_R'],
    'Behavior': ['les- socializing', 'ha- housework', 'wrk- general', 'ex- participating'],
    'Time_Relative_hms': ['00:00:00', '00:05:30', '00:10:15', '00:15:45'],
    'id': [1, 1, 2, 2],
    'do': [1, 2, 1, 2]  # This is what we extract from Observation
})

print("behav_test (simulated behavior data):")
print(behav_test)
print(f"\nbehav_test columns: {list(behav_test.columns)}")
print(f"behav_test shape: {behav_test.shape}")

# Simulate log_df with (id, do) combinations
log_test = pd.DataFrame({
    'id': [1, 1, 2, 2],
    'do': [1, 2, 1, 2],
    'session': [1, 2, 1, 2],
    'start_time': ['6:43:57 PM', '4:43:57 PM', '1:17:10 PM', '8:00:27 AM']
})

print("\nlog_test (simulated log data):")
print(log_test)
print(f"\nlog_test columns: {list(log_test.columns)}")
print(f"log_test shape: {log_test.shape}")

# ============================================================================
# Step 2: Test merge with only 'id' (the OLD way - WRONG)
# ============================================================================
print("\n" + "=" * 80)
print("2. TESTING MERGE WITH ONLY 'id' (OLD/WRONG WAY)")
print("-" * 80)

merge_wrong = behav_test.merge(
    log_test[['id', 'session', 'start_time']].rename(columns={'session': 'do'}),
    on='id',
    how='left'
)

print("\nResult of merge on 'id' only:")
print(merge_wrong[['Observation', 'id', 'do_x', 'do_y', 'start_time']])
print("\n⚠️ PROBLEM: Notice how 'do_x' and 'do_y' are created because merge couldn't match properly!")
print("   This causes wrong start_time values to be assigned.")

# ============================================================================
# Step 3: Test merge with both 'id' and 'do' (the CORRECT way)
# ============================================================================
print("\n" + "=" * 80)
print("3. TESTING MERGE WITH 'id' AND 'do' (CORRECT WAY)")
print("-" * 80)

merge_correct = behav_test.merge(
    log_test[['id', 'session', 'start_time']].rename(columns={'session': 'do'}),
    on=['id', 'do'],
    how='left'
)

print("\nResult of merge on ['id', 'do']:")
print(merge_correct[['Observation', 'id', 'do', 'start_time']])
print("\n✓ SUCCESS: Each row now has the correct start_time matching its (id, do) combination!")

# ============================================================================
# Step 4: Verify the merge worked correctly
# ============================================================================
print("\n" + "=" * 80)
print("4. VERIFICATION")
print("-" * 80)

# Check for nulls
null_count = merge_correct['start_time'].isna().sum()
print(f"Number of null start_time values: {null_count}")

if null_count == 0:
    print("✓ PASS: All rows have start_time values")
else:
    print(f"✗ FAIL: {null_count} rows missing start_time")

# Check that each (id, do) got the right start_time
print("\nVerifying correct matches:")
for idx, row in merge_correct.iterrows():
    expected = log_test[(log_test['id'] == row['id']) & (log_test['do'] == row['do'])]['start_time'].values
    if len(expected) > 0:
        match = row['start_time'] == expected[0]
        status = "✓" if match else "✗"
        print(f"{status} Row {idx}: id={row['id']}, do={row['do']}, start_time={row['start_time']} (expected: {expected[0]})")

# ============================================================================
# Step 5: Test start_time_new computation (simulating your pipeline)
# ============================================================================
print("\n" + "=" * 80)
print("5. TESTING start_time_new COMPUTATION")
print("-" * 80)

# Simulate the start_time_dt computation
merge_correct['start_time_str'] = merge_correct['start_time'].astype(str).str.strip()
dt1 = pd.to_datetime(merge_correct['start_time_str'], format='%I:%M:%S %p', errors='coerce')
dt2 = pd.to_datetime(merge_correct['start_time_str'], format='%I:%M %p', errors='coerce')
merge_correct['start_time_dt'] = dt1.fillna(dt2)

print("\nParsed start_time_dt:")
print(merge_correct[['Observation', 'start_time', 'start_time_dt']])

# Check for nulls in start_time_dt
dt_nulls = merge_correct['start_time_dt'].isna().sum()
print(f"\nNumber of null start_time_dt values: {dt_nulls}")

if dt_nulls == 0:
    print("✓ PASS: All start_time values parsed successfully")
else:
    print(f"✗ FAIL: {dt_nulls} start_time values failed to parse")
    print("\nRows with parsing issues:")
    print(merge_correct[merge_correct['start_time_dt'].isna()][['Observation', 'start_time', 'start_time_dt']])

# Simulate time_relative_td (using a simple example)
merge_correct['Time_Relative_hms'] = merge_correct['Time_Relative_hms']
r = merge_correct['Time_Relative_hms'].astype(str).str.strip()
td = pd.to_timedelta(r, errors='coerce')
merge_correct['time_relative_td'] = td

print("\nParsed time_relative_td:")
print(merge_correct[['Observation', 'Time_Relative_hms', 'time_relative_td']])

# Compute start_time_new
merge_correct['start_time_new'] = merge_correct['start_time_dt'] + merge_correct['time_relative_td']

print("\nFinal start_time_new:")
print(merge_correct[['Observation', 'start_time', 'Time_Relative_hms', 'start_time_new']])

# Check for nulls in start_time_new
stn_nulls = merge_correct['start_time_new'].isna().sum()
print(f"\nNumber of null start_time_new values: {stn_nulls}")

if stn_nulls == 0:
    print("✓ PASS: All start_time_new values computed successfully")
else:
    print(f"✗ FAIL: {stn_nulls} start_time_new values are null")
    print("\nRows with null start_time_new:")
    null_rows = merge_correct[merge_correct['start_time_new'].isna()]
    print(null_rows[['Observation', 'start_time', 'start_time_dt', 'time_relative_td', 'start_time_new']])

# ============================================================================
# Step 6: Compare with your actual data
# ============================================================================
print("\n" + "=" * 80)
print("6. DIAGNOSTIC: Check your actual data")
print("-" * 80)

print("\nTo diagnose your actual issue, check these in your notebook:")
print("1. Does behav_am_df2 have 'do' column before merge?")
print("   → Check: 'do' in behav_am_df2.columns")
print("   → Check: behav_am_df2['do'].head()")
print("\n2. After merge, does behav_am_df3 have 'do' column?")
print("   → Check: 'do' in behav_am_df3.columns")
print("   → Check: behav_am_df3[['id', 'do', 'start_time']].head(10)")
print("\n3. Does start_time_dt parse correctly?")
print("   → Check: behav_am_df4['start_time_dt'].isna().sum()")
print("\n4. Does time_relative_td parse correctly?")
print("   → Check: behav_am_df4['time_relative_td'].isna().sum()")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)


