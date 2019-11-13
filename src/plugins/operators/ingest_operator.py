import logging
import requests
import json
from requests.auth import HTTPBasicAuth

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook

log = logging.getLogger(__name__)

class BaseIngestOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(BaseIngestOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        print("Base operator")
        message = "operator_param: {}".format(self.operator_param)
        print(message)
        return message

'''
The S3 Ingest operator is currently configured to call a StreamSets Data Collector instance.
TODO: Find way to reduce cohesion. Right now this operator just "knows" the endpoint
'''
class S3IngestOperator(BaseIngestOperator):
    def __init__(self, sourceBucket, sourceFilePattern, assetName, *args, **kwargs):
        self.sourceBucket = sourceBucket
        self.sourceFilePattern = sourceFilePattern
        self.assetName = assetName
        super(S3IngestOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        ingestionParams = {
            "sourceBucket": self.sourceBucket,
            "sourceFilePattern": self.sourceFilePattern,
            "assetName": self.assetName
        }

        sdcHeaders = {
            "X-Requested-By": "sdc",
            "Content-Type": "application/json"
        }

        ingestS3PipelineId = "ingests3655d04b7-19bd-4e1f-846f-050d22dd8d60"
        hook = HttpHook(http_conn_id="streamsets_api", method="POST")
        hook.run(
            endpoint="/rest/v1/pipeline/{}/start".format(ingestS3PipelineId),
            data=json.dumps(ingestionParams),
            headers=sdcHeaders
        )
