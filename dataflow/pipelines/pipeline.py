import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import yaml
import json
from google.cloud.sql.connector import Connector
import pymysql


class ConfigLoader:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
    
    def get_pubsub_topics(self):
        return self.config['pubsub']['topics']
    
    def get_sensor_table_mapping(self):
        return self.config['sensors']
    
    def get_mysql_config(self):
        return self.config['mysql']
    
    def get_gcp_options(self):
        return self.config['gcp']


class BatchMySQLWriteFn(beam.DoFn):
    def __init__(self, sensor_table_mapping, mysql_config, batch_size=100):
        self.sensor_table_mapping = sensor_table_mapping
        self.mysql_config = mysql_config
        self.batch_size = batch_size

    def start_bundle(self):
        self.connector = Connector()
        self.conn = self.connector.connect(
            self.mysql_config['instance_connection_name'],
            "pymysql",
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            db=self.mysql_config['database'],
        )
        self.cursor = self.conn.cursor()
        self.buffer = []

    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        table_records = {}
        for element in self.buffer:
            sensor_id = element.get('sensor_id')
            if sensor_id in self.sensor_table_mapping:
                table = self.sensor_table_mapping[sensor_id]['table']
                table_records.setdefault(table, []).append(element)
            else:
                print(f"Unknown sensor_id: {sensor_id}")
        for table, records in table_records.items():
            if records:
                columns = records[0].keys()
                columns_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
                values = [tuple(record[col] for col in columns) for record in records]
                try:
                    self.cursor.executemany(sql, values)
                    self.conn.commit()
                except Exception as e:
                    print(f"Error inserting into MySQL: {e}")
        self.buffer = []

    def finish_bundle(self):
        self.flush()
        self.cursor.close()
        self.conn.close()


class DataflowPipeline:
    def __init__(self, config_path):
        self.config_loader = ConfigLoader(config_path)
        self.gcp_options = self.config_loader.get_gcp_options()
        self.pubsub_topics = self.config_loader.get_pubsub_topics()
        self.sensor_table_mapping = self.config_loader.get_sensor_table_mapping()
        self.mysql_config = self.config_loader.get_mysql_config()

    def create_pipeline_options(self):
        pipeline_options = PipelineOptions()
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.gcp_options['project']
        google_cloud_options.region = self.gcp_options['region']
        google_cloud_options.job_name = self.gcp_options['job_name']
        google_cloud_options.staging_location = self.gcp_options['staging_location']
        google_cloud_options.temp_location = self.gcp_options['temp_location']
        pipeline_options.view_as(SetupOptions).save_main_session = True
        pipeline_options.view_as(GoogleCloudOptions).runner = self.gcp_options['runner']
        return pipeline_options

    def run(self):
        pipeline_options = self.create_pipeline_options()
        with beam.Pipeline(options=pipeline_options) as p:
            topic_pcollections = []
            for topic in self.pubsub_topics:
                pcoll = (p
                         | f"ReadFromPubSub_{topic}" >> beam.io.ReadFromPubSub(topic=topic)
                         )
                topic_pcollections.append(pcoll)
            messages = (topic_pcollections | "FlattenTopics" >> beam.Flatten())
            parsed_messages = (messages
                               | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
                               | "ParseJSON" >> beam.Map(json.loads)
                               )
            (parsed_messages
             | "WriteToMySQL" >> beam.ParDo(BatchMySQLWriteFn(self.sensor_table_mapping, self.mysql_config))
             )


def main():
    config_path = 'config.yaml'
    pipeline = DataflowPipeline(config_path)
    pipeline.run()


if __name__ == "__main__":
    main()
