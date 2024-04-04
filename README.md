# streaming
## This project uses flask and html to create a simple frontend which takes user data via a form. This data is streamed to pubsub and then to simple streaming or aggregation job using dataflow. The dataflow output is inserted into Bigquery for analysis. In order to test working of app and bigquery authorisation , i am also directly inserting data from app to bigquery using bigquery client for python.

FLASK APP  &#8594; PUBSUB  &#8594; DATAFLOW  &#8594; Bigquery

## Steps for running this project on GCP

1. create vm and ssh into it
2. gcloud auth login
3. generate SA key and export it to app credentials check script.sh
4. install dependencies check scripts.sh
5. create pubsub topic "flask-topic"
6. create app.py , tempaltes folder and index.html in it.
7. create stream_job.py and run it.
8. create process_job.py and run it.
9. use app.py on http://external_ip:port
