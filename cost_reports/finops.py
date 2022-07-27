#!/usr/bin/env python3

# script breaks down an AWS Organization bill by CostCenter
# If an account in the Organization is dedicated to a CostCenter
# then its entire amount will be categorized as such.
# For other accounts, the CostCenter tag will be used to categorized
# the line item

import math
import re
import pandas as pd


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


def main(cur_parquet, account_csv):
    report_body = ""

    # Read in the account labels, for those accounts whose costs are all allocated the same way
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

    acct_rows = pd.read_parquet(cur_parquet, columns=columns)

    # Compute total cost for the month
    total_cost = acct_rows[VALUE_COLUMN].sum()

    # we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
    accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
    joined = acct_rows.join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")
    report_body += f"Total line items: {joined.shape[0]}\n"

    # either cost center is not null or account cost center is not null (one of the two IS set)
    has_center = joined[
        (
            (joined[COST_CENTER_COLUMN].notna()) &
            (joined[COST_CENTER_COLUMN] != 'na')
        ) | (
            (joined[ACCOUNT_COST_CENTER_COLUMN].notna()) &
            (joined[ACCOUNT_COST_CENTER_COLUMN] != 'na')
        )
    ]
    report_body += f"Rows with Cost Center data: {has_center.shape[0]}\n"

    # cost center is either null or 'na' AND account cost center is either null or 'na'
    no_center = joined[
        (
            (joined[COST_CENTER_COLUMN].isna()) |
            (joined[COST_CENTER_COLUMN] == 'na')
        ) & (
            (joined[ACCOUNT_COST_CENTER_COLUMN].isna()) |
            (joined[ACCOUNT_COST_CENTER_COLUMN] == 'na')
        )
    ]
    report_body += f"Rows without Cost Center data: {no_center.shape[0]}\n"

    # group expenses by their cost center.  See group_by() for details
    by_center = acct_rows.groupby((lambda row_index: group_by_cost_center(joined,row_index)), dropna=False, group_keys=False)

    # group expenses by their cloudformation stack.  See group_by() for details
    by_stack = no_center.groupby((lambda row_index: group_by_tag(no_center,row_index,CLOUDFORMATION_STACK_COLUMN,"Cloudformation Stack")), dropna=False, group_keys=False)

    # group expenses by their service catalog provisioner.  See group_by() for details
    by_catalog = no_center.groupby((lambda row_index: group_by_tag(no_center,row_index,SERVICE_CATALOG_PRINCIPAL_COLUMN,"Service Catalog Provisioner")), dropna=False, group_keys=False)

    # sum up each category and sort, descending
    center_summed = by_center.sum().sort_values(VALUE_COLUMN, ascending=False)[[VALUE_COLUMN]]
    stack_summed = by_stack.sum().sort_values(VALUE_COLUMN, ascending=False)[[VALUE_COLUMN]]
    catalog_summed = by_catalog.sum().sort_values(VALUE_COLUMN, ascending=False)[[VALUE_COLUMN]]

    # display the cost centers
    report_body += "\nCosts by Program\n"
    sorted = center_summed.rename(columns={VALUE_COLUMN:DISPLAY_VALUE_COLUMN})
    with pd.option_context( 'display.precision', 2),\
        pd.option_context('display.max_rows', 500),\
        pd.option_context('display.max_colwidth', 200),\
        pd.option_context('display.float_format', (lambda x : f'${x:.2f}')):
        report_body += sorted.to_string()

    total = sorted[DISPLAY_VALUE_COLUMN].sum()
    if (abs(total-total_cost)>0.01):
        raise Exception("categorized costs do not add up to total bill.")
    report_body += f"\nTotal: ${total:.2f}\n"

    # display cfn stacks and service catalog owners for line items with no cost center
    for summed in [stack_summed, catalog_summed]:
        report_body += "\nCosts without a known Program\n"
        sorted = summed[summed[VALUE_COLUMN] >= 0.01].rename(columns={VALUE_COLUMN:DISPLAY_VALUE_COLUMN})
        with pd.option_context( 'display.precision', 2),\
            pd.option_context('display.max_rows', 500),\
            pd.option_context('display.max_colwidth', 200),\
            pd.option_context('display.float_format', (lambda x : f'${x:.2f}')):
            report_body += sorted.to_string()

    return report_body

def safe_at(df, r, c):
    try:
        return df.at[r,c]
    except KeyError:
        return None

def valid_value(check_value):
    if check_value is None or check_value == 'na':
        return False
    return True

def group_by_tag(df, row_index, tag_column, heading):
    tag = safe_at(df, row_index, tag_column)
    if not valid_value(tag):
        return f"{heading} / None"
    return f"{heading} / {tag}"

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
    cost_center = TAG_REPLACEMENT.get(cost_center, cost_center)

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
