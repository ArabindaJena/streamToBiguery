import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from datetime import datetime
import time

# Replace 'your-project' and 'your-topic' with your actual project and Pub/Sub topic
pubsub_topic = 'projects/your-project/topics/your-topic'

# Replace 'your-bq-dataset' and 'your-bq-table' with your actual BigQuery dataset and table
bq_table = 'your-project.your-bq-dataset.your-bq-table'
agg_bq_table = 'your-project.your-bq-dataset.your-aggregate-table'  # New BigQuery table for aggregated data

def calculate_average(values):
    total_salary = sum(values['salary'])
    average_salary = total_salary / len(values['salary'])
    values['average_salary'] = average_salary
    return values

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=options)
    window_duration = 10

    (p
     | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=pubsub_topic)
     | 'Parse JSON' >> beam.Map(lambda x: eval(x.decode('utf-8')))
     | 'Add timestamp to events' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp']))
     | 'Fixed-size windows' >> beam.WindowInto(beam.window.FixedWindows(window_duration))
     | 'Calculate average salary per company' >> beam.Map(lambda x: (x['company'], {'salary': [x['salary']]}))
     | 'Group by company' >> beam.GroupByKey()
     | 'Calculate average' >> beam.Map(calculate_average)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         agg_bq_table,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    result = p.run()

if __name__ == '__main__':
    run()
