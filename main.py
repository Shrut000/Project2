import re
from datetime import datetime
import mysql.connector
from pymongo import MongoClient


def extract_data(log_file):
    email_dates = []
    with open(log_file, 'r') as f:
        for line in f:
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', line)
            date_match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
            if email_match and date_match:
                email = email_match.group()
                date = datetime.strptime(date_match.group(), '%Y-%m-%d %H:%M:%S')
                email_dates.append((email, date.strftime('%Y-%m-%d %H:%M:%S')))
    return email_dates


def transform_data(email_dates):
    transformed_data = []
    for email, date in email_dates:
        transformed_data.append({
            'email': email,
            'date': date
        })
    return transformed_data


def save_to_mongodb(transformed_data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['user_history_db']
    collection = db['user_history']
    collection.insert_many(transformed_data)
    client.close()


def upload_to_mysql():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['user_history_db']
    collection = db['user_history']
    data = collection.find()
    
    connection = mysql.connector.connect(
        host="localhost",
        user="username",
        password="password",
        database="user_history_db"
    )
    mycursor = connection.mycursor()
    mycursor.execute('''
        CREATE TABLE IF NOT EXISTS user_history
        (email VARCHAR(255), date DATETIME, PRIMARY KEY (email, date))
    ''')
    
    for doc in data:
        mycursor.execute("INSERT OR IGNORE INTO user_history VALUES (%s, %s)", (doc['email'], doc['date']))
    connection.commit()
    connection.close()
    client.close()


def main():
    log_file = 'server_log.txt'
    email_dates = extract_data(log_file)
    transformed_data = transform_data(email_dates)
    save_to_mongodb(transformed_data)
    upload_to_mysql()

if __name__ == "__main__":
    main()
