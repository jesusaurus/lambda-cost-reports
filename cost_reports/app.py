#!/usr/bin/env python3

# project imports
import cost_reports.finops as finops

# stdlib imports
import logging
import time

# other imports
import boto3


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

ACCOUNT_TSV_S3_KEY = 'account_cost_centers/table.csv'
CUR_S3_BUCKET = 'sagebase-cost-reports'
CUR_S3_PREFIX = 'sagebase'
REPORT_NAME = 'monthly-costs'


class App:
    s3 = None

    def __init__(self):
        if App.s3 is None:
            App.s3 = boto3.client('s3')

        self.account_csv = None
        self.cur_parquet = None
        self.report = None

        self.year_month = self._last_month()

    def _last_month(self):
        """What month was it last month?

        Return a string in the formart `year=XXXX/month=XX` representing
        the previous month."""

        # format syntax: https://docs.python.org/3.9/library/time.html?highlight=time.strftime#time.strftime
        year = int(time.strftime("%Y"))
        month = int(time.strftime("%m")) -  1   # last month
        return f"year={year}/month={month}"

    def _build_cur_key(self):
        """Build the path to the parquet file for the current month"""

        s3_path = f"{CUR_S3_PREFIX}/{REPORT_NAME}/{REPORT_NAME}/{self.year_month}"

        # TODO: check for file indicies other than '00001'
        s3_file = f"{REPORT_NAME}-00001.snappy.parquet"

        s3_key = f"{s3_path}/{s3_file}"
        LOG.info(f"S3 Key: {s3_key}")
        return s3_key

    def _download_cur_parquet(self):
        """Download the cost-and-usage report for the current month"""

        cur_parquet_key = self._build_cur_key()
        self.cur_parquet = App.s3.get_object(Bucket=CUR_S3_BUCKET, Key=cur_parquet_key)

    def _download_account_csv(self):
        """Download a TSV mapping accounts to cost centers"""

        self.account_csv = App.s3.get_object(Bucket=CUR_S3_BUCKET, Key=ACCOUNT_TSV_S3_KEY)

    def _process_report(self):
        """Group costs by program cost center"""

        if self.cur_parquet is None:
            raise Exception("No cost and usage data loaded")

        if self.account_csv is None:
            raise Exception("No account cost center data loaded")

        self.report = finops.main(self.cur_parquet, self.account_csv)

    def _upload_report(self):
        """Upload the per-cost-center report back to the S3 bucket"""

        # Don't write an empty file
        if self.report is None:
            LOG.Error("No report generated")
            raise Exception("No report generated")

        report_key = "by_program/{self.year_month}/report.txt"
        App.s3.put_object(Bucket=CUR_S3_BUCKET, Key=report_key, Body=self.report)

    def run(self):
        self._download_account_csv()
        self._download_cur_parquet()
        self._process_report()
        self._upload_report()

def lambda_handler(event, context):
    """Recurring lambda to group monthly Cost and Usage Reports by tagged Cost Center

    Parameters
    ----------
    event: dict, required
        Scheduled Event Input Format

        Example event in `events/event.json` generated using the
        command `sam local generate-event config periodic-rule`.

        Recurring Lambda doc: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-tutorial.html

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """

    app = App()
    app.run()
