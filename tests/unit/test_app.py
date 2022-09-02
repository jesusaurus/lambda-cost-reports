from cost_reports import app

import os

import boto3
import botocore
import pytest
from botocore.stub import Stubber


FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixture',
)

TEST_BUCKET = 'testBucket'

MOCK_ACCOUNT_LIST = {'Accounts': [
    {'Id': '804034162148', 'Name': 'itsandbox'},
]}

MOCK_ACCOUNT_TAGS = {'Tags': [
    {'Key': 'CostCenter', 'Value': 'Platform Infrastructure / 990300'},
]}

@pytest.fixture()
def parquet():
    """Create a file-like object encapsulated in an S3-response-like object"""
    response = {}

    fp = os.path.join(FIXTURE_DIR, 'cur.parquet')
    fileLike = open(fp, 'rb')
    if fileLike is None:
        raise Exception("Error opening cur.parquet fixture")

    response['Body'] = fileLike
    return response


def test_api(caplog, mocker, parquet):
    """Test API event trigger (no S3 key)"""
    checks = [
        '(Month-to-Date)',
        'Total: $101823.89',
    ]

    # Test initialization with no input key
    _run_test(caplog, mocker, parquet, None, checks)

def test_s3_eb(caplog, mocker, parquet):
    """Test S3 EventBridge event trigger (with S3 key)"""
    s3_key_with_date = 'monthly-costs/year=2022/month=7/test.snappy.parquet'
    checks = [
        'July 2022 (Complete)',
        'Total: $101823.89',
    ]

    # Test initialization with a known past date
    _run_test(caplog, mocker, parquet, s3_key_with_date, checks)


def _run_test(caplog, mocker, parquet, s3_key, check_strings):
    s3 = boto3.client('s3')
    app.App.s3 = s3

    orgclient = boto3.client('organizations')
    app.App.orgclient = orgclient

    s3 = boto3.client('s3')
    app.App.s3 = s3
    s3_stub = Stubber(s3)
    s3_stub.add_response('get_object', parquet)

    orgclient = boto3.client('organizations')
    app.App.orgclient = orgclient
    org_stub = Stubber(orgclient)
    org_stub.add_response('list_accounts', MOCK_ACCOUNT_LIST)
    org_stub.add_response('list_tags_for_resource', MOCK_ACCOUNT_TAGS)

    s3_stub.activate()
    org_stub.activate()

    test_app = app.App(TEST_BUCKET, s3_key)

    # Skip file upload
    test_app._upload_results = mocker.MagicMock()

    # Run the app
    test_app.run()

    for cs in check_strings:
        assert cs in test_app.summary
