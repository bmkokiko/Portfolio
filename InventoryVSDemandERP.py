import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# just a simple setting for how far ahead to look
forecast_days = 30

# connect to the ERP DB
conn_str = (
    "DRIVER={SQL Server};SERVER=localhost\\SQLEXPRESSBK;"
    "DATABASE=ERP;Trusted_Connection=yes;"
)

# get the data we need from the ERP db
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

start_date = datetime.today().date()
end_date = start_date + timedelta(days=forecast_days)

# query to join items, inventory and forecast
query = f"""
SELECT 
    i.ItemID,
    i.ItemName,
    w.WarehouseName,
    ISNULL(inv.OnHandQuantity, 0) AS CurrentStock,
    ISNULL(SUM(df.ForecastQuantity), 0) AS ForecastDemand
FROM dbo.Items i
LEFT JOIN dbo.Inventory inv ON i.ItemID = inv.ItemID
LEFT JOIN dbo.Warehouses w ON inv.WarehouseID = w.WarehouseID
LEFT JOIN dbo.DemandForecast df 
    ON i.ItemID = df.ItemID
   AND df.ForecastDate BETWEEN ? AND ?
GROUP BY i.ItemID, i.ItemName, w.WarehouseName, inv.OnHandQuantity
"""

df = pd.read_sql(query, conn, params=[start_date, end_date])

# classify stuff
statuses = []
suggested = []

for index, row in df.iterrows():
    stock = row['CurrentStock']
    forecast = row['ForecastDemand']
    if forecast == 0:
        statuses.append("No Forecast")
        suggested.append(0)
    elif stock < forecast * 0.9:
        statuses.append("Low Stock - Needs Production")
        suggested.append(forecast - stock)
    elif stock > forecast * 1.1:
        statuses.append("Overstocked")
        suggested.append(0)
    else:
        statuses.append("Stock Meets Demand")
        suggested.append(0)

df['Status'] = statuses
df['SuggestedProductionQty'] = suggested

# print everything out
print("\n===== Inventory vs Forecast Report =====\n")
print(df[['ItemID', 'ItemName', 'WarehouseName', 'CurrentStock', 'ForecastDemand', 'Status', 'SuggestedProductionQty']])

# save to CSV
df.to_csv("inventory_report.csv", index=False)
print("\nSaved CSV file: inventory_report.csv")

# plot something
try:
    df.plot(kind='bar', x='ItemName', y=['CurrentStock', 'ForecastDemand'], figsize=(12,6))
    plt.title(f"Inventory vs Forecast (Next {forecast_days} days)")
    plt.ylabel("Qty")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("inventory_chart.png")
    print("Saved chart as: inventory_chart.png")
except:
    print("Something went wrong making the chart...")

conn.close()
