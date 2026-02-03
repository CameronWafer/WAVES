import pandas as pd


def main() -> None:
    gt_path = "C:/Users/HELIOS-300/Desktop/Data/am_gt_3.csv"
    gt_df = pd.read_csv(gt_path)

    col = "updated_activity"
    print(f"Using gt_df['{col}'] for Activity_Type comparison.")
    print("\nValue counts:")
    print(gt_df[col].value_counts(dropna=False).to_string())

    print("\nProportions:")
    print((gt_df[col].value_counts(dropna=False, normalize=True) * 100).round(3).to_string())


if __name__ == "__main__":
    main()
