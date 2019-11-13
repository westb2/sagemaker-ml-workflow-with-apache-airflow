from operators import ingest_operator
from airflow.plugins_manager import AirflowPlugin

class IngestPlugin(AirflowPlugin):
    name = "ingest_operator"
    operators = [ingest_operator.S3IngestOperator]
