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
import numpy as np
import synapseclient
from urllib.parse import unquote_plus as url_unquote


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


class App:
    s3 = None
    orgclient = None
    synclient = None

    def __init__(self, bucket, prefix, report, key):
        self.account_dict = {}
        self.cur_parquet = None
        self.munged_data = None

        if App.s3 is None:
            App.s3 = boto3.client('s3')

        if App.orgclient is None:
            App.orgclient = boto3.client('organizations')

        if App.synclient is None:
            App.synclient = synapseclient.Synapse()

        self.cur_prefix = prefix
        self.cur_name = report
        self.bucket = bucket

        self.aws_parquet = f"{report}-00001.snappy.parquet"

        # The key will be None when triggered by API endpoint
        if key is None:
            self._year_month()
            self.cur_key = self._build_cur_key(self.aws_parquet)

        else:
            LOG.info(f"S3 key given: {key}")
            self.cur_key = key

            ym_re = re.compile(r'year=(\d{4})/month=(\d{1,2})')
            found = ym_re.search(key)

            if found:
                when = datetime.datetime(year=int(found.group(1)),
                                         month=int(found.group(2)),
                                         day=1)
                self._year_month(when)
            else:
                raise Exception("Year/month could not be found in S3 path")

    def _year_month(self, when=None):
        """Return a string in the format `year=XXXX/month=XX`
        representing the given `datetime` (or current month if None)."""

        now = datetime.datetime.now()
        if when is None:
            when = now

        # First create the string used in the S3 path
        self.year_month = f"year={when.year}/month={when.month}"

    def _build_cur_key(self, s3_file):
        """Build the path to the parquet file for last month"""

        s3_path = f"{self.cur_prefix}/{self.cur_name}/{self.cur_name}/{self.year_month}"

        s3_key = f"{s3_path}/{s3_file}"
        LOG.info(f"Built S3 key: {s3_key}")
        return s3_key

    def download_cur_parquet(self):
        """Download the cost-and-usage report for the current month"""

        s3_resp = App.s3.get_object(Bucket=self.bucket, Key=self.cur_key)
        self.cur_parquet = io.BytesIO(s3_resp['Body'].read())

    def build_account_dict(self):
        """Build a dictionary to be consumed by pandas.

        {'account_id': [,,,],
         'account_name': [,,,],
         'account_cost_center': [,,,]}
        """

        accounts = []
        account_ids = []
        account_names = []
        account_tags = []
        token = ''

        response = App.orgclient.list_accounts()
        accounts.extend(response['Accounts'])

        if 'NextToken' in response:
            token = response['NextToken']

        while token != '':
            response = App.orgclient.list_accounts()
            accounts.extend(response['Accounts'])

            if 'NextToken' in response:
                token = response['NextToken']
            else:
                token = ''

        for account in accounts:
            _name = account['Name']
            _id = account['Id']

            tags = App.orgclient.list_tags_for_resource(ResourceId=_id)['Tags']
            for tag in tags:
                if tag['Key'] == 'CostCenter':
                    account_ids.append(_id)
                    account_names.append(_name)
                    account_tags.append(tag['Value'])
                    break
            else:
                LOG.warning(f"No CostCenter tag for account {_name} ({_id})")

        self.account_dict['account_id'] = account_ids
        self.account_dict['account_name'] = account_names
        self.account_dict['account_cost_center'] = account_tags

    def process_report(self):
        """Group costs by program cost center"""

        if self.account_dict is {}:
            raise Exception("No account tag data loaded")

        if self.cur_parquet is None:
            raise Exception("No cost and usage data loaded")

        self.bill_date, self.munged_data, self.total = finops.main(self.cur_parquet, self.account_dict)
        LOG.info("Grouped data generated")

    def update_synapse(self):
        """Upload the per-cost-center summary back to the S3 bucket"""

        # Don't write an empty file
        if self.bill_date is None:
            raise Exception("No summary generated")
        if self.munged_data is None:
            raise Exception("No data generated")

        # Collect some synapse environment variables
        if 'SYNAPSE_TABLE' in os.environ:
            table_id = os.environ['SYNAPSE_TABLE']
        else:
            raise Exception("Environment variable 'SYNAPSE_TABLE' must be set")

        if 'SYNAPSE_AUTH_TOKEN' not in os.environ:
            raise Exception("Environment variable 'SYNAPSE_AUTH_TOKEN' must be set")

        # Log in to synapse
        App.synclient.login()

        # Load our table
        table = App.synclient.get(table_id)

        # Compare schemas and add any needed columns
        old_cols = list(App.synclient.getTableColumns(table_id))
        old_names = [c['name'] for c in old_cols]
        new_cols = synapseclient.as_table_columns(self.munged_data)

        altered = False
        for col in new_cols:
            cname = col['name']
            if cname not in old_names:
                LOG.info(f"Adding column '{cname}'")
                table.addColumn(col)
                altered = True
        if altered:
            App.synclient.store(table)

        # Get existing rows for the current bill
        query = f"select * from {table_id} where {finops.BILL_START_DATE_COLUMN}='{self.bill_date}'"
        old = App.synclient.tableQuery(query)

        # TODO: intelligently calculate a difference to apply to synapse rather than
        # deleting all out-dated rows before uploading potentially identical rows;
        # but previous attepmts have resulted in inaccurate totals.

        # Drop all out-dated rows
        App.synclient.delete(old)

        # Store the updated data
        table = synapseclient.Table(table_id, self.munged_data)
        App.synclient.store(table)

        # Log out and forget this transient lambda environment
        App.synclient.logout(forgetMe=True)

    def run(self):
        self.build_account_dict()
        self.download_cur_parquet()
        self.process_report()
        self.update_synapse()

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
        else:
            bucket = os.environ['CUR_BUCKET']

        prefix = os.environ['CUR_PREFIX']
        report = os.environ['CUR_REPORT_NAME']

        app = App(bucket, prefix, report, s3_key)
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
