import logging
import os

import pytest

from cost_reports import finops


LOG = logging.getLogger(__name__)

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixture',
)


@pytest.fixture()
def parquet():
    """Create a file-like object for the parquet fixture"""
    fp = os.path.join(FIXTURE_DIR, 'cur.parquet')
    rv = open(fp, 'rb')
    if rv is None:
        raise Exception("Error opening cur.parquet fixture")
    return rv

@pytest.fixture()
def csv():
    """Create a file-like object for the csv fixture"""
    fp = os.path.join(FIXTURE_DIR, 'acc.csv')
    rv = open(fp, 'r')
    if rv is None:
        raise Exception("Error opening acc.csv fixture")
    return rv


def test_finops(caplog, parquet, csv):
    """Regenerate a known report from fixtures"""
    summary, _ = finops.main(parquet, csv, '2022 May (Test Fixture)')
    #LOG.info(summary)

    # Check the first and last line of the known report
    assert '2022 May (Test Fixture)' in summary
    assert 'Total: $101823.89' in summary
