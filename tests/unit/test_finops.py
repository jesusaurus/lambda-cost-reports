import logging
import os
import random
import string

import numpy as np
import pandas as pd
import pytest

from cost_reports import finops


def test_valid_values():
    valid1 = 'string'
    valid2 = 1.23

    invalid1 = 'na'
    invalid2 = None
    invalid3 = np.NAN

    assert finops.valid_value(valid1) is True
    assert finops.valid_value(valid2) is True

    assert finops.valid_value(invalid1) is False
    assert finops.valid_value(invalid2) is False
    assert finops.valid_value(invalid3) is False

def test_lookup():
    random.seed()

    test_tag = 'Test / 123123'
    other_tag = 'Other / 000001'
    test_other_tag = 'Test Other / 456456'
    test_account_tag = 'Test Account / 789789'

    test_regex_in = 'Name_123456'
    test_regex_out = 'Name / 123456'
    tag_replace_in = 'No Program / 000000'
    tag_replace_out = 'NO PROGRAM / 000000'

    def fuzz_id():
        # 5 character string
        return random.choices(string.ascii_letters, k=5)

    def fuzz_cost():
        # float in the range [0.0, 1.0)
        return random.random()

    def fuzz_invalid():
        # one of three invalid values
        return random.choice(['na', None, np.NAN])

    # column headings
    col_heads = [
            finops.LINE_ITEM_ID_COLUMN,
            finops.VALUE_COLUMN,
            finops.COST_CENTER_COLUMN,
            finops.COST_CENTER_OTHER_COLUMN,
            finops.ACCOUNT_COST_CENTER_COLUMN,
    ]

    # valid tag
    row1 = [fuzz_id(), fuzz_cost(), test_tag, fuzz_invalid(), fuzz_invalid()]

    # valid other tag
    row2 = [fuzz_id(), fuzz_cost(), other_tag, test_other_tag, fuzz_invalid()]

    # valid account tag
    row3 = [fuzz_id(), fuzz_cost(), fuzz_invalid(), fuzz_invalid(), test_account_tag]

    # rename from regex
    row4 = [fuzz_id(), fuzz_cost(), test_regex_in, fuzz_invalid(), fuzz_invalid()]

    # rename from tag replacement
    row5 = [fuzz_id(), fuzz_cost(), tag_replace_in, fuzz_invalid(), fuzz_invalid()]

    # expected output
    expected = pd.Series([
        test_tag,
        test_other_tag,
        test_account_tag,
        test_regex_out,
        tag_replace_out,
    ])

    # exercise our function under test
    df = pd.DataFrame([row1, row2, row3, row4, row5], columns=col_heads)
    tags = df.apply(lambda row: finops.cost_center_lookup(row), axis=1)

    # check the result
    pd.testing.assert_series_equal(tags, expected, check_names=False)
