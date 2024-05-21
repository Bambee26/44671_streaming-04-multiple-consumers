"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Bambee Garfield
    Date: May 21st, 2024

"""
import pika
import csv
import time

def publish_tasks_from_csv(csv_file, queue_name):
    # Establish connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare queue
    channel.queue_declare(queue=queue_name, durable=True)

    # Read tasks from CSV and publish them
    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for task_num, row in enumerate(reader, start=1):
            task_message = row[0]
            channel.basic_publish(exchange='', routing_key=queue_name, body=task_message,
                                  properties=pika.BasicProperties(delivery_mode=2,))
            print(f" [x] Sent '{task_message}' to {queue_name}")
            time.sleep(1)  # Sleep for 1 second between each task

    connection.close()

# Publish tasks from tasks.csv to task_queue
publish_tasks_from_csv('tasks.csv', 'task_queue')

# Publish tasks from tasks.csv to task_queue2
publish_tasks_from_csv('tasks.csv', 'task_queue2')
