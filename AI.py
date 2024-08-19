import numpy as np
import mysql.connector
from mysql.connector import Error

# MySQL connection setup
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='CrashData',
            user='root',
            password='Mburuian'
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Fetch the three most recent crash points from the database
def fetch_recent_crash_points(connection):
    try:
        cursor = connection.cursor()
        select_query = """
        SELECT crash_point FROM CrashPoints 
        ORDER BY crash_time DESC 
        LIMIT 3
        """
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return [float(row[0].replace('x', '')) for row in rows][::-1]  # Reverse to maintain order
    except Error as e:
        print(f"Error fetching recent crash points: {e}")
        return None

# Find consecutive crash points that match the recent points within ±0.5 range
def find_consecutive_similar_crash_points(connection, recent_crash_points):
    if not recent_crash_points or len(recent_crash_points) != 3:
        print("Invalid number of recent crash points.")
        return []

    tolerance = 0.5
    recent_avg = np.mean(recent_crash_points)

    try:
        cursor = connection.cursor()
        select_query = """
        SELECT crash_point 
        FROM (
            SELECT crash_point, 
                   LEAD(crash_point) OVER (ORDER BY crash_time) AS next_point, 
                   LEAD(crash_point, 2) OVER (ORDER BY crash_time) AS next_next_point
            FROM CrashPoints
        ) AS subquery
        WHERE ABS(crash_point - %s) <= %s
          AND ABS(next_point - %s) <= %s
          AND ABS(next_next_point - %s) <= %s
        ORDER BY ABS(crash_point - %s)
        LIMIT 5
        """
        cursor.execute(select_query, (
            recent_crash_points[0], tolerance,
            recent_crash_points[1], tolerance,
            recent_crash_points[2], tolerance,
            recent_avg
        ))
        rows = cursor.fetchall()
        return [float(row[0].replace('x', '')) for row in rows]
    except Error as e:
        print(f"Error fetching consecutive similar crash points: {e}")
        return []

if __name__ == "__main__":
    connection = create_connection()
    if connection:
        recent_crash_points = fetch_recent_crash_points(connection)
        if recent_crash_points:
            print(f"Recent crash points: {recent_crash_points}")
            
            similar_crash_points = find_consecutive_similar_crash_points(connection, recent_crash_points)
            if similar_crash_points:
                print(f"Similar consecutive crash points: {similar_crash_points}")
            else:
                print("No similar consecutive crash points found.")
        
        connection.close()
