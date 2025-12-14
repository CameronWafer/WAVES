"""
Export behav_copy to CSV
Copy this into a notebook cell
"""

# Export behav_copy to CSV
behav_copy.to_csv("Cameron_AM_Clean.csv", index=False)

print("✓ Successfully exported behav_copy to 'Cameron_AM_Clean.csv'")
print(f"  Rows: {len(behav_copy):,}")
print(f"  Columns: {list(behav_copy.columns)}")
print(f"  File saved to: {os.path.abspath('Cameron_AM_Clean.csv')}")

