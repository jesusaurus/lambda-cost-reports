import os
import pathlib

import pytest

from cost_reports import app

ROOT_DIR = pathlib.Path(os.path.realpath(__file__)).parents[2]
EVENT_FILE = ROOT_DIR.joinpath('events', 'event.json')

@pytest.fixture()
def event():
    """Generates an object from `events/event.json`"""
    return open(EVENT_FILE).read()

# TODO: uncomment this once a full report is available in S3
#def test_s3_download(mocker, event):
#    mock_process = mocker.patch.object(app.App, '_process_report')
#    mock_upload = mocker.patch.object(app.App, '_upload_report')
#
#    app.lambda_handler(event, "")
#
#    mock_process.assert_called_once()
#    mock_upload.assert_called_once()
