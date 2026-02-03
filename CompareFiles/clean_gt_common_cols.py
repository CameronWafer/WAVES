import pandas as pd


def main() -> None:
    clean_path = "C:/Users/HELIOS-300/Desktop/WAVES/AM Full Code/Cameron_AM_Clean.csv"
    gt_path = "C:/Users/HELIOS-300/Desktop/Data/am_gt_3.csv"

    clean_df = pd.read_csv(clean_path)
    gt_df = pd.read_csv(gt_path)

    common_cols = [c for c in clean_df.columns if c in gt_df.columns]
    print("Common columns:", common_cols)

    for col in common_cols:
        print(f"\n===== {col} =====")
        print("Clean value counts:")
        print(clean_df[col].value_counts(dropna=False).to_string())
        print("\nGT value counts:")
        print(gt_df[col].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
