import pyodbc
from datetime import datetime, timedelta

# DB connections
CONTROL_CONN_STR = (
    "DRIVER={SQL Server};SERVER=localhost\\SQLEXPRESSBK;"
    "DATABASE=ControlSystem;Trusted_Connection=yes;"
)
MES_CONN_STR = (
    "DRIVER={SQL Server};SERVER=localhost\\SQLEXPRESSBK;"
    "DATABASE=MES;Trusted_Connection=yes;"
)

FUZZY_WINDOW_MINUTES = 5  # ±5 min window for alarm matching

def get_unlinked_downtimes():
    """
    MES downtimes that haven’t been linked to any alarm yet.
    """
    with pyodbc.connect(MES_CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DowntimeID, EquipmentID, SensorID, StartTime
            FROM dbo.DowntimeEvents
            WHERE AlarmID IS NULL
        """)
        return cursor.fetchall()

def find_matching_alarms(equipment_id, sensor_id, downtime_start):
    """
    Pull all alarms within ±5 minutes of downtime start for the same equipment and sensor.
    """
    window_start = downtime_start - timedelta(minutes=FUZZY_WINDOW_MINUTES)
    window_end = downtime_start + timedelta(minutes=FUZZY_WINDOW_MINUTES)

    with pyodbc.connect(CONTROL_CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT AlarmID, Timestamp, Description
            FROM dbo.AlarmEvents
            WHERE EquipmentID = ?
              AND SensorID = ?
              AND Timestamp BETWEEN ? AND ?
            ORDER BY Timestamp DESC
        """, equipment_id, sensor_id, window_start, window_end)
        return cursor.fetchall()

def write_audit_entry(cursor, downtime_id, alarm_id, match_type, timestamp, notes=""):
    """
    Insert match result into MES.dbo.AlarmDowntimeAudit.
    """
    cursor.execute("""
        INSERT INTO dbo.AlarmDowntimeAudit (DowntimeID, AlarmID, MatchType, MatchTimestamp, Notes)
        VALUES (?, ?, ?, ?, ?)
    """, downtime_id, alarm_id, match_type, timestamp, notes)

def associate_alarms():
    """
    Main logic: match alarms to downtime and log everything.
    """
    downtimes = get_unlinked_downtimes()
    if not downtimes:
        print("✅ No unlinked downtime events found.")
        return

    print(f"🔍 Processing {len(downtimes)} unlinked downtime event(s)...")

    with pyodbc.connect(MES_CONN_STR) as mes_conn:
        cursor = mes_conn.cursor()

        for dt in downtimes:
            dt_id = dt.DowntimeID
            eq_id = dt.EquipmentID
            sensor_id = dt.SensorID
            dt_start = dt.StartTime

            matching_alarms = find_matching_alarms(eq_id, sensor_id, dt_start)

            if not matching_alarms:
                print(f"⚠️  Downtime {dt_id} – No alarms found (Sensor {sensor_id}, Equipment {eq_id})")
                write_audit_entry(cursor, dt_id, None, 'NoMatch', dt_start, "No matching alarms found")
                continue

            # Use the latest alarm for linkage
            latest_alarm = matching_alarms[0]
            alarm_id, alarm_ts, _ = latest_alarm

            # Update downtime with chosen alarm
            cursor.execute("""
                UPDATE dbo.DowntimeEvents
                SET AlarmID = ?
                WHERE DowntimeID = ?
            """, alarm_id, dt_id)

            # Log all matching alarms
            for alarm in matching_alarms:
                is_primary = "PrimaryMatch" if alarm.AlarmID == alarm_id else "SecondaryMatch"
                write_audit_entry(cursor, dt_id, alarm.AlarmID, is_primary, alarm.Timestamp)

            print(f"[✔] Linked Downtime {dt_id} to Alarm {alarm_id} (Sensor: {sensor_id}, {len(matching_alarms)} alarms)")

        mes_conn.commit()
        print("\n✅ Downtimes updated and all matches logged.")

def main():
    associate_alarms()

if __name__ == "__main__":
    main()
