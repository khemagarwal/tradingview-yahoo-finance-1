import s3fs
import pandas as pd
from datetime import datetime

# ✅ Setup Cloudflare R2 S3 filesystem
fs = s3fs.S3FileSystem(
    anon=False,
    key="5c8ea9c516abfc78987bc98c70d2868a",
    secret="0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
    client_kwargs={
        "endpoint_url": "https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com"
    },
)

# ✅ Paths to compare
paths = [
    "desiquant/data/candles/NIFTY50/",
    "desiquant/data/candles/NIFTY 50/"
]

dataframes = {}

# ✅ Read both datasets
for path in paths:
    print(f"\n📂 Listing files under: {path}")
    files = fs.ls(path, detail=True)
    for f in files:
        print(f"{f['Key']} ({f['Size']/1024:.1f} KB)")
    print(f"✅ Total files found: {len(files)}")

    if not files:
        continue

    file_path = f"s3://{files[0]['Key']}"
    try:
        df = pd.read_parquet(file_path, filesystem=fs)

        # Detect date column
        date_col = [c for c in df.columns if c.lower() in ["date", "datetime", "timestamp"]][0]
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col).sort_index()

        print(f"   🕒 Using column '{date_col}'")
        print(f"   Rows: {len(df):,}")
        print(f"   Date range: {df.index.min()} → {df.index.max()}")
        dataframes[path] = df
    except Exception as e:
        print(f"⚠️ Error reading file: {e}")

# ✅ Compare both datasets
if len(dataframes) == 2:
    (path1, df1), (path2, df2) = list(dataframes.items())

    print("\n🔍 Comparing both datasets...\n")

    print(f"📅 {path1} → {df1.index.min()} → {df1.index.max()} ({len(df1):,} rows)")
    print(f"📅 {path2} → {df2.index.min()} → {df2.index.max()} ({len(df2):,} rows)\n")

    # Find common timestamps
    common_times = df1.index.intersection(df2.index)
    only_in_1 = df1.index.difference(df2.index)
    only_in_2 = df2.index.difference(df1.index)

    print(f"🔁 Common timestamps: {len(common_times):,}")
    print(f"➕ Present only in {path1}: {len(only_in_1):,}")
    print(f"➕ Present only in {path2}: {len(only_in_2):,}\n")

    if len(only_in_2) > 0:
        print(f"🧩 Example few timestamps only in {path2}:")
        print(list(only_in_2[:10]))

    # ✅ Align and compare only on common timestamps
    df1_common = df1.loc[common_times]
    df2_common = df2.loc[common_times]

    # Identify differences
    diff_mask = (df1_common != df2_common) & ~(df1_common.isna() & df2_common.isna())
    differing_rows = diff_mask.any(axis=1)
    differences = []

    for idx in df1_common.index[differing_rows]:
        row1 = df1_common.loc[idx]
        row2 = df2_common.loc[idx]
        diffs = {}
        for col in df1_common.columns:
            val1, val2 = row1[col], row2[col]
            if pd.isna(val1) and pd.isna(val2):
                continue
            if val1 != val2:
                diffs[col] = {"Dataset1": val1, "Dataset2": val2}
        if diffs:
            flat = {"Timestamp": idx}
            for col, vals in diffs.items():
                flat[f"{col} (Dataset1)"] = vals["Dataset1"]
                flat[f"{col} (Dataset2)"] = vals["Dataset2"]
            differences.append(flat)

    if differences:
        diff_df = pd.DataFrame(differences)
        diff_file = f"nifty_data_differences_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        diff_df.to_excel(diff_file, index=False)
        print(f"\n⚠️ Found {len(diff_df):,} differing candles.")
        print(f"📤 Differences exported to: {diff_file}")
    else:
        print("\n✅ No differences found in overlapping timestamps.")

    # ✅ NEW SECTION: Check missing candles timing
    print("\n🔎 Checking missing candles in NIFTY50 that exist in NIFTY 50...")

    # Use previously calculated difference (only_in_2) as true missing candles from NIFTY50
    missing_in_df1 = only_in_2
    print(f"⏱️ Total missing candles in {path1}: {len(missing_in_df1):,}")

    # ✅ Check how many missing timestamps are after last timestamp in df1
    last_df1_time = df1.index.max()
    missing_after_last = [t for t in missing_in_df1 if t > last_df1_time]

    print(f"📆 Last timestamp in {path1}: {last_df1_time}")
    print(f"🕒 Missing candles that occur AFTER this timestamp: {len(missing_after_last):,}")

    if missing_after_last:
        print(f"🧩 Example few missing after last timestamp: {missing_after_last[:10]}")

    # ✅ Also check how many can be filled from df2 (for safety)
    missing_found_in_other = [t for t in missing_in_df1 if t in df2.index]
    print(f"🧩 Missing candles from '{path1}' that can be filled from '{path2}': {len(missing_found_in_other):,}")

    if missing_found_in_other:
        print(f"🕒 Example few that can be filled: {missing_found_in_other[:10]}")

else:
    print("❌ Could not load both datasets for comparison.")
