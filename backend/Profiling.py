import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
# import matplotlib.pyplot as plt
import seaborn as sns
import os
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
def integrated_profile_and_anomaly_with_charts(excel_file, sheet_name=None):
    # Load Excel and handle multi-sheet case
    sheets = pd.read_excel(excel_file, sheet_name=sheet_name)
    if isinstance(sheets, dict):
        df = list(sheets.values())[0]
    else:
        df = sheets
    df.columns = df.columns.str.strip()

    # Convert all columns to string for profiling consistency
    for col in df.columns:
        df[col] = df[col].astype(str)

    # Profile columns
    profiling_stats = {}
    for col in df.columns:
        col_data = df[col].astype(str)
        profiling_stats[col] = {
            "Total Count": len(col_data),
            "Total Columns": len(df.columns),
            "Null Count": col_data.isnull().sum(),
            "Null Count%": round(col_data.isnull().mean() * 100, 2),
            "Unique Values": col_data.nunique(),
            "Unique Value%": round(col_data.nunique() / len(col_data) * 100, 2),
            # Min/Max length of string values
            "Average Length": col_data.str.len().mean(),
            "Longest Length": col_data.str.len().max(),
            "Shortest Length": col_data.str.len().min(),
        }

    # Anomaly detection on specific columns
    anomaly_columns = [col for col in ['CUSTOMER_NAME', 'ADDRESS', 'CITY', 'STATE', 'ZIP', 'COUNTRY', 'COUNTY'] if col in df.columns]
    anomaly_df = df[anomaly_columns].dropna().copy() if anomaly_columns else pd.DataFrame()
    label_encoder = LabelEncoder()
    for col in anomaly_columns:
        anomaly_df[col] = label_encoder.fit_transform(anomaly_df[col].astype(str))

    anomalies_count = 0
    if not anomaly_df.empty:
        model = IsolationForest(contamination=0.2, random_state=42)
        model.fit(anomaly_df)
        preds = model.predict(anomaly_df)
        anomaly_df['Anomaly'] = [True if p == -1 else False for p in preds]
        anomalies_count = anomaly_df['Anomaly'].sum()

    # Generate a few simple profiling charts and save as PNG files (or handle as needed)
    os.makedirs('charts', exist_ok=True)

    # Example: Bar chart of null counts per column
    null_counts = [profiling_stats[col]["Null Count"] for col in df.columns]
    sns.barplot(x=list(df.columns), y=null_counts).set_title("Null Counts Per Column")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('charts/null_counts.png')
    plt.clf()

    # Example: Bar chart of unique values per column
    unique_counts = [profiling_stats[col]["Unique Values"] for col in df.columns]
    sns.barplot(x=list(df.columns), y=unique_counts).set_title("Unique Values Per Column")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('charts/unique_values.png')
    plt.clf()

    # Add more charts as needed...

    return {
        "Total Columns": len(df.columns),
        "Column Profiling": profiling_stats,
        "Total Records": len(df),
        "Anomalies Detected": anomalies_count,
        "Chart Paths": {
            "Null Counts": "charts/null_counts.png",
            "Unique Values": "charts/unique_values.png",
        }
    }

