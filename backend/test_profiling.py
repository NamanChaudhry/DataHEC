from Profiling import integrated_profile_and_anomaly_with_charts

if __name__ == "__main__":
    filepath = "data/Customer/PeopleSoft9.1/PS91_Customer.xlsx"

    # Call the profiling and anomaly detection function
    report = integrated_profile_and_anomaly_with_charts(filepath)

    print("Total Records:", report["Total Records"])
    print("Anomalies Detected:", report["Anomalies Detected"])
    print()

    # Print profiling for all columns
    for col, stats in report["Column Profiling"].items():
        print(f"Profiling for column: {col}")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        print()


