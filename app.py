from flask import Flask, request, render_template
from google.cloud import pubsub_v1
from google.cloud import bigquery
from datetime import datetime
import time

app = Flask(__name__)

# Pub/Sub Configuration
# replace "gcp-project-id" with your project id and "flask-topic" with your flask topic name.

project_id = "gcp-project-id"
topic_name = "flask-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# BigQuery Configuration
bq_dataset = "dataset-name"
bq_table = "table-name"
bq_client = bigquery.Client()
dataset_ref = bq_client.dataset(bq_dataset)
table_ref = dataset_ref.table(bq_table)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        name = request.form['name']
        age = request.form['age']
        company = request.form['company']
        salary = request.form['salary']
        current_time = time.time()

        # Publish to Pub/Sub
        data = {'name': name, 'age': age, 'company': company, 'salary': salary, 'timestamp': current_time}
        publisher.publish(topic_path, data=str(data).encode('utf-8'))

        # Insert into BigQuery # This directly inserts submitted data in BQ.
        row_to_insert = [{'name': name, 'age': age, 'company': company, 'salary': salary, 'timestamp': current_time}]
        errors = bq_client.insert_rows_json(table_ref, row_to_insert)

        if not errors:
            return "Message sent, Thanks!"
        else:
            return f"Error: {errors}"

    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
