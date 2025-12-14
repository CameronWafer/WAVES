"""
Duplicate Row Analysis
Copy this into a new notebook cell to analyze duplicate rows in behav_copy
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("DUPLICATE ROW ANALYSIS")
print("=" * 80)

if 'behav_copy' not in locals():
    print("✗ behav_copy not found in memory")
    print("Please run the main processing cell first")
else:
    # ============================================================================
    # Step 1: Identify duplicate rows
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 1: Identifying Duplicate Rows")
    print("=" * 80)
    
    total_rows = len(behav_copy)
    print(f"Total rows in behav_copy: {total_rows:,}")
    
    # Find duplicates (keeping all instances, including originals)
    duplicate_mask = behav_copy.duplicated(keep=False)
    duplicate_count = duplicate_mask.sum()
    unique_duplicate_groups = behav_copy.duplicated().sum()
    
    print(f"\nRows that are duplicates (including originals): {duplicate_count:,}")
    print(f"Unique duplicate groups: {unique_duplicate_groups:,}")
    print(f"Percentage of data that is duplicated: {duplicate_count/total_rows*100:.2f}%")
    
    # Create dataframe with only duplicate rows
    duplicates_df = behav_copy[duplicate_mask].copy()
    
    print(f"\n✓ Created duplicates_df with {len(duplicates_df):,} duplicate rows")
    
    # ============================================================================
    # Step 2: Show sample of duplicates
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 2: Sample of Duplicate Rows (First 50)")
    print("=" * 80)
    
    print("\nFirst 50 duplicate rows:")
    print(duplicates_df.head(50))
    
    # ============================================================================
    # Step 3: Analyze duplicate patterns
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Duplicate Pattern Analysis")
    print("=" * 80)
    
    # Count how many times each duplicate appears
    duplicate_counts = duplicates_df.groupby(list(behav_copy.columns)).size().reset_index(name='duplicate_count')
    duplicate_counts = duplicate_counts.sort_values('duplicate_count', ascending=False)
    
    print(f"\nNumber of unique duplicate row patterns: {len(duplicate_counts):,}")
    print(f"\nTop 20 most frequent duplicate patterns:")
    print(duplicate_counts.head(20))
    
    # Summary statistics
    print(f"\nDuplicate frequency statistics:")
    print(f"  Min duplicates per pattern: {duplicate_counts['duplicate_count'].min()}")
    print(f"  Max duplicates per pattern: {duplicate_counts['duplicate_count'].max()}")
    print(f"  Mean duplicates per pattern: {duplicate_counts['duplicate_count'].mean():.2f}")
    print(f"  Median duplicates per pattern: {duplicate_counts['duplicate_count'].median():.2f}")
    
    # ============================================================================
    # Step 4: Check which columns are causing duplicates
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 4: Identifying Duplicate-Causing Columns")
    print("=" * 80)
    
    # Check if duplicates are exact matches across all columns
    all_cols_duplicates = behav_copy.duplicated(subset=list(behav_copy.columns), keep=False).sum()
    print(f"Duplicates when checking ALL columns: {all_cols_duplicates:,}")
    
    # Check each column combination to see what's unique
    key_columns = ['id', 'obs', 'rel_time']
    if all(c in behav_copy.columns for c in key_columns):
        key_duplicates = behav_copy.duplicated(subset=key_columns, keep=False).sum()
        print(f"Duplicates when checking key columns {key_columns}: {key_duplicates:,}")
        
        if key_duplicates == all_cols_duplicates:
            print("  → Duplicates are exact matches across all columns")
        else:
            print("  → Some duplicates differ in non-key columns")
    
    # ============================================================================
    # Step 5: Analyze duplicates by key dimensions
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 5: Duplicate Analysis by Key Dimensions")
    print("=" * 80)
    
    if 'id' in duplicates_df.columns:
        print("\nDuplicates by id:")
        id_dup_counts = duplicates_df['id'].value_counts().head(10)
        print(id_dup_counts)
    
    if 'obs' in duplicates_df.columns:
        print("\nDuplicates by obs:")
        obs_dup_counts = duplicates_df['obs'].value_counts().head(10)
        print(obs_dup_counts)
    
    if 'rel_time' in duplicates_df.columns:
        print("\nDuplicates by rel_time:")
        rel_time_dup_counts = duplicates_df['rel_time'].value_counts().head(10)
        print(rel_time_dup_counts)
    
    # Check (id, obs, rel_time) combinations
    if all(c in duplicates_df.columns for c in ['id', 'obs', 'rel_time']):
        combo_duplicates = duplicates_df.groupby(['id', 'obs', 'rel_time']).size()
        print(f"\nNumber of unique (id, obs, rel_time) combinations in duplicates: {len(combo_duplicates):,}")
        print(f"Most frequent (id, obs, rel_time) combinations:")
        print(combo_duplicates.sort_values(ascending=False).head(20))
    
    # ============================================================================
    # Step 6: Check for consecutive duplicates
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 6: Checking for Consecutive Duplicates")
    print("=" * 80)
    
    # Sort by key columns to see if duplicates are consecutive
    if all(c in behav_copy.columns for c in ['id', 'obs', 'rel_time']):
        sorted_copy = behav_copy.sort_values(['id', 'obs', 'rel_time']).reset_index(drop=True)
        consecutive_dups = sorted_copy.duplicated(keep='first')
        consecutive_count = consecutive_dups.sum()
        
        print(f"Consecutive duplicates (after sorting by id, obs, rel_time): {consecutive_count:,}")
        
        if consecutive_count > 0:
            print("\nSample of consecutive duplicates:")
            # Show rows where the previous row is a duplicate
            consecutive_mask = consecutive_dups
            consecutive_samples = sorted_copy[consecutive_mask].head(20)
            print(consecutive_samples)
            
            # Also show the row before each duplicate
            print("\nCorresponding original rows (rows before duplicates):")
            dup_indices = sorted_copy[consecutive_mask].index
            orig_indices = [idx - 1 for idx in dup_indices if idx > 0]
            if orig_indices:
                orig_samples = sorted_copy.loc[orig_indices[:20]]
                print(orig_samples)
    
    # ============================================================================
    # Step 7: Check if duplicates differ in any columns
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 7: Checking if Duplicates Differ in Any Columns")
    print("=" * 80)
    
    # For each duplicate group, check if all columns are identical
    duplicate_groups = duplicates_df.groupby(list(behav_copy.columns))
    
    identical_duplicates = 0
    differing_duplicates = 0
    
    for name, group in duplicate_groups:
        if len(group) > 1:
            # Check if all rows in group are identical
            if group.drop_duplicates().shape[0] == 1:
                identical_duplicates += len(group)
            else:
                differing_duplicates += len(group)
    
    print(f"Identical duplicates (all columns match): {identical_duplicates:,}")
    print(f"Duplicates that differ in some columns: {differing_duplicates:,}")
    
    # ============================================================================
    # Step 8: Summary and Recommendations
    # ============================================================================
    print("\n" + "=" * 80)
    print("SUMMARY AND RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\nTotal duplicate rows: {duplicate_count:,}")
    print(f"Unique duplicate patterns: {len(duplicate_counts):,}")
    
    if duplicate_count > 0:
        print("\n⚠ RECOMMENDATIONS:")
        print("1. Check if duplicates are expected (e.g., same activity at same time)")
        print("2. If duplicates are errors, use drop_duplicates() to remove them")
        print("3. Consider if you need to keep duplicates or can safely remove them")
        print("\nTo remove duplicates, use:")
        print("  behav_copy_clean = behav_copy.drop_duplicates()")
        print(f"  This would reduce rows from {total_rows:,} to {total_rows - unique_duplicate_groups:,}")
    else:
        print("\n✓ No duplicates found!")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    # Make duplicates_df available for further exploration
    print("\n✓ duplicates_df is now available for further exploration")
    print("  Try: duplicates_df.head(100)")
    print("  Try: duplicates_df.groupby(['id', 'obs', 'rel_time']).size()")

