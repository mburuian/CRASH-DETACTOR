from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import mysql.connector
from mysql.connector import Error
import traceback

# Set up the Chrome WebDriver
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless for no GUI
service = Service('C:\\Users\\Rose Psalms\\Downloads\\chromedriver-win32\\chromedriver-win32\\chromedriver.exe')
driver = webdriver.Chrome(service=service, options=chrome_options)

# MySQL connection setup
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',  # Update with your MySQL host
            database='CrashData',  # Update with your MySQL database name
            user='root',  # Update with your MySQL username
            password='Mburuian'  # Update with your MySQL password
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Function to check if a crash point has already been processed
def is_crash_point_processed(connection, crash_point):
    try:
        cursor = connection.cursor()
        check_query = "SELECT COUNT(*) FROM CrashPoints WHERE crash_point = %s"
        cursor.execute(check_query, (crash_point,))
        count = cursor.fetchone()[0]
        return count > 0
    except Error as e:
        print(f"Error checking if crash point is processed: {e}")
        return False

# Function to insert crash point into MySQL database
def insert_crash_point(connection, crash_point):
    try:
        cursor = connection.cursor()
        insert_query = """INSERT INTO CrashPoints (crash_point) VALUES (%s)"""
        cursor.execute(insert_query, (crash_point,))
        connection.commit()
        print(f"Inserted crash point {crash_point} into database")
    except Error as e:
        print(f"Error inserting crash point into MySQL: {e}")

# Function to extract crash point from the page
def extract_crash_point():
    try:
        # Wait for the element that contains the crash point to be present
        crash_point_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div.css-ynz9y9 > div:nth-child(2) > div > div.css-1633bsf > div > div:nth-child(1) > div > div.css-lyld25 > div > div:nth-child(2) > div > a:nth-child(2)'))
        )
        crash_point = crash_point_element.text
        print(f"Extracted crash point: {crash_point}")
        return crash_point
    except Exception as e:
        print(f"Error extracting crash point: {e}")
        print("Traceback:", traceback.format_exc())
        return None

# Open the URL
url = 'https://play.pakakumi.com/'  # Replace with the actual URL you want to open
driver.get(url)

# Main loop to extract and store crash points
try:
    connection = create_connection()
    if connection:
        while True:
            crash_point = extract_crash_point()
            if crash_point:
                if not is_crash_point_processed(connection, crash_point):
                    insert_crash_point(connection, crash_point)
                else:
                    print(f"Crash point {crash_point} has already been processed.")
            else:
                print("No crash point extracted.")
            time.sleep(5)  # Wait for a few seconds before checking again
finally:
    if connection and connection.is_connected():
        connection.close()
        print("MySQL connection closed")
    driver.quit()
