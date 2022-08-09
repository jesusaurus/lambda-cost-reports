#!/usr/bin/env python3

# script breaks down an AWS Organization bill by CostCenter
# If an account in the Organization is dedicated to a CostCenter
# then its entire amount will be categorized as such.
# For other accounts, the CostCenter tag will be used to categorized
# the line item

import logging
import re

import pandas as pd
import matplotlib.pyplot as plt


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

ACCOUNT_ID_COLUMN = 'line_item_usage_account_id'
VALUE_COLUMN = 'line_item_unblended_cost'
COST_CENTER_COLUMN = 'resource_tags_user_cost_center'
COST_CENTER_OTHER_COLUMN = 'resource_tags_user_cost_center_other'
CLOUDFORMATION_STACK_COLUMN = 'resource_tags_aws_cloudformation_stack_name'
SERVICE_CATALOG_PRINCIPAL_COLUMN = 'resource_tags_aws_servicecatalog_provisioning_principal_arn'
LINE_ITEM_TYPE_COLUMN = 'line_item_line_item_type'
PRODUCT_NAME_COLUMN = 'product_product_name'
USAGE_TYPE_COLUMN = 'line_item_usage_type'
DESCRIPTION_COLUMN = 'line_item_line_item_description'

ACCOUNT_COST_CENTER_COLUMN = 'account_cost_center'
ACCOUNT_NAME_COLUMN = 'account_name'
DISPLAY_VALUE_COLUMN = 'Cost'

TAG_REPLACEMENT = {
    'Indirects':'NO PROGRAM / 000000',
    'No Program / 000000':'NO PROGRAM / 000000'
}


def report_head(title):
    return f"""<html>
        <head><title>{title}</title></head>
        <body><h2>{title}</h2>"""

def report_foot(total):
    return f"<h4>Total: ${total:.2f}</h4></body></html>"

def main(cur_parquet, account_csv, title_month):
    report_body = report_head(title_month)

    # Read in the account labels, for those accounts whose costs are all allocated the same way
    LOG.info("Reading accounts csv file")
    accounts_columns = {
        'account_id':'str',
        ACCOUNT_NAME_COLUMN:'str',
        ACCOUNT_COST_CENTER_COLUMN:'str'
    }
    accounts = pd.read_csv(account_csv, usecols=accounts_columns.keys(), dtype=accounts_columns)

    # Read in the month's cost and usage report
    columns=[
        VALUE_COLUMN,
        ACCOUNT_ID_COLUMN,
        COST_CENTER_COLUMN,
        COST_CENTER_OTHER_COLUMN,
        CLOUDFORMATION_STACK_COLUMN,
        SERVICE_CATALOG_PRINCIPAL_COLUMN,
        LINE_ITEM_TYPE_COLUMN,
        PRODUCT_NAME_COLUMN,
        USAGE_TYPE_COLUMN,
        DESCRIPTION_COLUMN
    ]

    LOG.info("Reading billing parquet file")
    acct_rows = pd.read_parquet(cur_parquet, columns=columns)

    # Compute total cost for the month
    LOG.info("Calculating original total")
    total_cost = acct_rows[VALUE_COLUMN].sum()

    # we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
    LOG.info("Inner joining data sources on account id")
    accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
    joined = acct_rows.join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")

    # group expenses by their cost center.  See group_by() for details
    LOG.info("Grouping by cost center")
    by_center = acct_rows.groupby((lambda row_index: group_by_cost_center(joined,row_index)), dropna=False, group_keys=False, as_index=True)

    # sum up each category and sort, descending
    center_summed = by_center.sum().rename(columns={VALUE_COLUMN:DISPLAY_VALUE_COLUMN})
    center_sorted = center_summed.sort_values(DISPLAY_VALUE_COLUMN, ascending=False)

    # Generate a pretty table
    LOG.info("Generating report")
    report_format = {DISPLAY_VALUE_COLUMN: '${:.2f}'}
    index_format = '<div align="right" style="padding: 2px 16px">{0}</div>'
    report_body += center_sorted.style.format_index(index_format).format(report_format).set_caption("Costs by Program").to_html()

    # Check that everything adds up
    LOG.info("Verifying total")
    total = center_sorted[DISPLAY_VALUE_COLUMN].sum()
    if (abs(total-total_cost)>0.01):
        raise Exception("categorized costs do not add up to total bill.")

    # Add total to the report
    report_body += report_foot(total)

    # Convert DataFrameGroupBy to DataFrame so it can be written to parquet
    report_data = by_center.apply(lambda x: x)

    return report_body, report_data

def safe_at(df, r, c):
    try:
        return df.at[r,c]
    except KeyError:
        return None

def valid_value(check_value):
    if check_value is None or check_value == 'na':
        return False
    return True

def cost_center_lookup(df, row_index):
    cost_center = safe_at(df, row_index, COST_CENTER_COLUMN)

    # don't try to process an invalid value
    if not valid_value(cost_center):
        return cost_center

    # check secondary cost center for Other values
    if cost_center == 'Other / 000001':
        cost_center = safe_at(df, row_index, COST_CENTER_OTHER_COLUMN)
        if not valid_value(cost_center):
            cost_center = 'Other (Unspecified) / 000001'

    # replace tags that look like "name_123456" with "name / 123456"
    cost_center = re.sub(r"(_)([0-9]{5,6})", (lambda x: " / "+x.group(2)), cost_center)

    # normalize 000000 case
    if cost_center in TAG_REPLACEMENT:
        cost_center = TAG_REPLACEMENT[cost_center]

    return cost_center

def group_by_cost_center(df, row_index):
    # if cost_center is a valid value, then return it
    # else return account_cost_center for the line item:

    cost_center = cost_center_lookup(df, row_index)
    if valid_value(cost_center):
        return cost_center

    account_cost_center = safe_at(df, row_index, ACCOUNT_COST_CENTER_COLUMN)
    if valid_value(account_cost_center):
        return account_cost_center

    return "Unknown / 999999"
