from cost_reports import app

import os

import boto3
import botocore
import numpy
import pytest
from botocore.stub import Stubber


FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixture',
)

TEST_BUCKET = 'testBucket'
TEST_PREFIX = 'testPrefix'
TEST_REPORT = 'testReport'

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

def test_api(caplog, mocker, monkeypatch, parquet):
    """Test API event trigger (no S3 key)"""
    _run_test(caplog, mocker, monkeypatch, parquet, None)

def test_s3_eb(caplog, mocker, monkeypatch, parquet):
    """Test S3 EventBridge event trigger (with S3 key)"""
    s3_key_with_date = 'monthly-costs/year=2022/month=6/test.snappy.parquet'
    _run_test(caplog, mocker, monkeypatch, parquet, s3_key_with_date)

def _run_test(caplog, mocker, monkeypatch, parquet, s3_key):
    # create boto stubs
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

    # Create a mock synapse client
    app.App.synclient = mocker.MagicMock()
    monkeypatch.setenv('SYNAPSE_AUTH_TOKEN', 'test')
    monkeypatch.setenv('SYNAPSE_TABLE', 'table')
    mocker.patch('synapseclient.Table')
    mocker.patch('synapseclient.as_table_columns')

    # create and run test App
    test_app = app.App(TEST_BUCKET, TEST_PREFIX, TEST_REPORT, s3_key)
    test_app.run()

    # Check that our fixture data was successfully processed
    assert test_app.bill_date == numpy.datetime64('2022-06')
    assert test_app.total == 101823.8892761993
