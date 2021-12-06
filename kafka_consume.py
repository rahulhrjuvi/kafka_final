from kafka import KafkaConsumer
from json import loads
import requests
import os
import sys
import json
# To detect the language in text
# Reference: https://stackoverflow.com/questions/39142778/python-how-to-determine-the-language
import langid
import config
import datetime 
import argparse
import psycopg

# Reset tweets table
def reset_tweets():
    connection = psycopg.connect(user="gb760", dbname = "final_project")
    cursor = connection.cursor()
    cursor.execute("""TRUNCATE tweets""")
    connection.commit()
    if connection:
        cursor.close()
        connection.close()

def insert_value(insert_query):
    connection = psycopg.connect(user="gb760", dbname = "final_project")
    cursor = connection.cursor()
                    
    query = """INSERT INTO tweets (time_stamp, tweet) VALUES (%s,%s)"""
                    
    cursor.execute(query, insert_query)
    connection.commit()
                    
    if connection:
        cursor.close()
        connection.close()

reset_tweets()

#setting up consumer
consumer = KafkaConsumer(
    'o',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

#consuming and pushing to postgres
print("consuming tweets and pushing to postgres")
for message in consumer:
    insert_query = message.value
    
    if insert_query: 
        insert_value(insert_query)
        
    
