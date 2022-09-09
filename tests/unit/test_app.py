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


@pytest.fixture()
def csv():
    """Create a file-like object encapsulated in an S3-response-like object"""
    response = {}

    fp = os.path.join(FIXTURE_DIR, 'acc.csv')
    fileLike = open(fp, 'rb')
    if fileLike is None:
        raise Exception("Error opening acc.csv fixture")

    response['Body'] = fileLike
    return response


def test_api(caplog, mocker, parquet, csv):
    """Test API event trigger (no S3 key)"""

    s3 = boto3.client('s3')
    app.App.s3 = s3

    with Stubber(s3) as stub:
        stub.add_response('get_object', csv)
        stub.add_response('get_object', parquet)

        # Test initialization without an input key
        test_app = app.App(TEST_BUCKET, None)

        # Skip file upload
        test_app._upload_results = mocker.MagicMock()

        # Run the app
        test_app.run()

        # API events always generate a month-to-date report
        assert '(Month-to-Date)' in test_app.summary

        # Known total for the fixture data
        assert 'Total: $101823.89' in test_app.summary


def test_s3_eb(caplog, mocker, parquet, csv):
    """Test S3 EventBridge event trigger (with S3 key)"""

    s3 = boto3.client('s3')
    app.App.s3 = s3

    with Stubber(s3) as stub:
        stub.add_response('get_object', csv)
        stub.add_response('get_object', parquet)

        # Test initialization with a known past date
        s3_key_with_date = 'monthly-costs/year=2022/month=7/test.snappy.parquet'
        test_app = app.App(TEST_BUCKET, s3_key_with_date)

        # Skip file upload
        test_app._upload_results = mocker.MagicMock()

        # Run the app
        test_app.run()

        # Check for the date in the test S3 key
        assert 'July 2022 (Complete)' in test_app.summary

        # Known total for the fixture data
        assert 'Total: $101823.89' in test_app.summary
