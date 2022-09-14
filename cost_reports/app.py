# project imports
import cost_reports.finops as finops

# stdlib imports
import io
import json
import logging
import os
import re
import datetime

# other imports
import boto3
from urllib.parse import unquote_plus as url_unquote


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

ACCOUNT_CSV_S3_KEY = 'account_cost_centers/table.csv'
CUR_S3_PREFIX = 'sagebase'
REPORT_NAME = 'monthly-costs'
INPUT_PARQUET = f"{REPORT_NAME}-00001.snappy.parquet"
OUTPUT_PARQUET = f"{REPORT_NAME}-by-center.parquet"


class App:
    s3 = None

    def __init__(self, bucket, s3_key):
        if App.s3 is None:
            App.s3 = boto3.client('s3')

        self.account_csv = None
        self.cur_parquet = None

        self.summary = None
        self.data = None

        self.bucket = bucket

        if s3_key is None:
            self._year_month()
            self.cur_key = self._build_cur_key(INPUT_PARQUET)

        else:
            LOG.info(f"S3 key found in event: {s3_key}")
            self.cur_key = s3_key

            ym_re = re.compile(r'year=(\d{4})/month=(\d{1,2})')
            found = ym_re.search(s3_key)

            if found:
                when = datetime.datetime(year=int(found.group(1)),
                                         month=int(found.group(2)),
                                         day=1)
                self._year_month(when)
            else:
                LOG.error("Year/month could not be found in S3 path")

    def _year_month(self, when=None):
        """Return a string in the format `year=XXXX/month=XX`
        representing the given `datetime` (or current month if None)."""

        now = datetime.datetime.now()
        if when is None:
            when = now

        # First create the string used in the S3 path
        self.year_month = f"year={when.year}/month={when.month}"

        # Then create the string used in the report title
        # format syntax: https://docs.python.org/3.9/library/time.html?highlight=time.strftime#time.strftime
        self.title_date = when.strftime('%B %Y')
        if when.month == now.month:
            LOG.info("Processing month-to-date report")
            self.title_date += ' (Month-to-Date)'
        else:
            LOG.info("Processing completed report")
            self.title_date += ' (Complete)'


    def _build_cur_key(self, s3_file):
        """Build the path to the parquet file for last month"""

        s3_path = f"{CUR_S3_PREFIX}/{REPORT_NAME}/{REPORT_NAME}/{self.year_month}"

        s3_key = f"{s3_path}/{s3_file}"
        LOG.info(f"Built S3 key: {s3_key}")
        return s3_key

    def _download_cur_parquet(self):
        """Download the cost-and-usage report for the current month"""

        s3_resp = App.s3.get_object(Bucket=self.bucket, Key=self.cur_key)
        self.cur_parquet = io.BytesIO(s3_resp['Body'].read())

    def _download_account_csv(self):
        """Download a CSV mapping accounts to cost centers"""

        s3_resp = App.s3.get_object(Bucket=self.bucket, Key=ACCOUNT_CSV_S3_KEY)
        self.account_csv = io.BytesIO(s3_resp['Body'].read())

    def _process_report(self):
        """Group costs by program cost center"""

        if self.cur_parquet is None:
            raise Exception("No cost and usage data loaded")

        if self.account_csv is None:
            raise Exception("No account cost center data loaded")

        self.summary, self.data = finops.main(self.cur_parquet, self.account_csv, self.title_date)
        LOG.info("Summary generated")

    def _upload_results(self):
        """Upload the per-cost-center summary back to the S3 bucket"""

        # Don't write an empty file
        if self.summary is None:
            LOG.error("No summary generated")
            raise Exception("No summary generated")
        if self.data is None:
            LOG.error("No data generated")
            raise Exception("No data generated")

        summary_key = self._build_cur_key("summary.html")
        App.s3.put_object(Bucket=self.bucket, Key=summary_key, Body=self.summary)

        data_key = self._build_cur_key(OUTPUT_PARQUET)
        self.data.to_parquet(f"s3://{self.bucket}/{data_key}")

    def run(self):
        self._download_account_csv()
        self._download_cur_parquet()
        self._process_report()
        self._upload_results()

def lambda_handler(event, context):
    """Recurring lambda to group monthly Cost and Usage Reports by tagged Cost Center

    Parameters
    ----------
    event: dict, required
        Scheduled Event Input Format

        Example event in `events/api.json` generated using the
        command `sam local generate-event apigateway http-api-proxy`.

        Recurring Lambda doc: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-tutorial.html

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """

    #LOG.debug(f"Event received: {json.dumps(event)}")

    try:
        s3_key = None
        if 'Records' in event: # s3 notification
            record = event['Records'][0] # assume one record
            try:
                s3_key = url_unquote(record['s3']['object']['key'])
                bucket = url_unquote(record['s3']['bucket']['name'])
                LOG.info(f"Found S3 bucket/key: {bucket}/{s3_key}")
            except KeyError:
                LOG.warn("No S3 info found in event record")
        elif 'detail' in event:  # event-bridge event
            detail = event['detail']
            try:
                s3_key = url_unquote(detail['requestParameters']['key'])
                bucket = url_unquote(detail['requestParameters']['bucketName'])
                LOG.info(f"Found S3 bucket/key: {bucket}/{s3_key}")
            except KeyError:
                LOG.warn("No S3 info found in event detail")


        app = App(bucket, s3_key)
        app.run()

    except Exception as e:
        LOG.exception(e)
        message = f"Exception encountered: {e}"
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": message,
            }),
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
        }),
    }
