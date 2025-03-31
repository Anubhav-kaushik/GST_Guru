# cross_check.py
import pandas as pd
import numpy as np
import re
import networkx as nx  # Import for analyze_circular_trading

#Helper Function to process itc eligible
def process_itc_eligible(itc_str):
    """
    Converts ITC Eligible strings to boolean values.

    Args:
        itc_str (str): The string representing ITC eligibility.

    Returns:
        bool: True if eligible, False otherwise. Returns None if input is NaN.
    """
    if pd.isna(itc_str):
        return None  # Or some other default value
    itc_str = str(itc_str).lower()
    if itc_str in ("yes", "true", "1"):
        return True
    return False

def validate_gstin(gstin):
    """
    Validates the format of a GSTIN (Goods and Services Tax Identification Number).

    Args:
        gstin (str): The GSTIN to validate.

    Returns:
        bool: True if the GSTIN is valid, False otherwise.
    """
    if not isinstance(gstin, str):
        return False
    pattern = r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$" #Regex to validate GSTIN
    return bool(re.match(pattern, gstin))

def safe_string_comparison(str1, str2, ignore_case=True):
    """
    Compares two strings safely, handling potential NaN/None values.

    Args:
        str1 (str or float): First string to compare.
        str2 (str or float): Second string to compare.
        ignore_case (bool): Whether to ignore case during comparison.

    Returns:
        bool: True if the strings are equal (or both are NaN/None), False otherwise.
    """
    if pd.isna(str1) and pd.isna(str2):  # Both NaN or None are considered equal
        return True
    if pd.isna(str1) or pd.isna(str2):  # One is NaN/None, the other is not
        return False

    str1 = str(str1)
    str2 = str(str2)

    if ignore_case:
        return str1.lower() == str2.lower()
    else:
        return str1 == str2

def cross_check_invoice_ewaybill(invoice_df, ewaybill_df):
    """
    Cross-checks invoice data against e-way bill data for discrepancies.

    Args:
        invoice_df (pd.DataFrame): DataFrame containing invoice data.  Must have columns:
                                     'invoice_number', 'invoice_date', 'gstin_supplier',
                                     'gstin_recipient', 'invoice_value'.
        ewaybill_df (pd.DataFrame): DataFrame containing e-way bill data.  Must have columns:
                                      'ewaybill_number', 'invoice_number', 'invoice_date',
                                      'gstin_supplier', 'gstin_recipient', 'invoice_value'.

    Returns:
        pd.DataFrame: DataFrame containing discrepancies found, or an empty DataFrame if no discrepancies.
                      The DataFrame will include the columns from both input DataFrames plus a 'discrepancy' column.
    """

    # Validate GSTIN format on both dataframes.
    invoice_df = invoice_df.copy() # Avoid modifying the original DataFrame
    ewaybill_df = ewaybill_df.copy()
    invoice_df['gstin_supplier_valid'] = invoice_df['gstin_supplier'].apply(validate_gstin)
    invoice_df['gstin_recipient_valid'] = invoice_df['gstin_recipient'].apply(validate_gstin)
    ewaybill_df['gstin_supplier_valid'] = ewaybill_df['gstin_supplier'].apply(validate_gstin)
    ewaybill_df['gstin_recipient_valid'] = ewaybill_df['gstin_recipient'].apply(validate_gstin)


    # Merge the dataframes on invoice_number
    merged_df = pd.merge(invoice_df, ewaybill_df, on='invoice_number', suffixes=('_invoice', '_ewaybill'), how='inner') #Inner join means that only those invoices existing on both invoice_df and ewaybill_df will be considered.

    # Create a 'discrepancy' column
    merged_df['discrepancy'] = ''

    #Cross-check common fields using the helper function.
    merged_df['invoice_date_match'] = merged_df.apply(lambda row: safe_string_comparison(row['invoice_date_invoice'], row['invoice_date_ewaybill']), axis=1)
    merged_df['gstin_supplier_match'] = merged_df.apply(lambda row: safe_string_comparison(row['gstin_supplier_invoice'], row['gstin_supplier_ewaybill']), axis=1)
    merged_df['gstin_recipient_match'] = merged_df.apply(lambda row: safe_string_comparison(row['gstin_recipient_invoice'], row['gstin_recipient_ewaybill']), axis=1)
    merged_df['invoice_value_match'] = np.isclose(merged_df['invoice_value_invoice'], merged_df['invoice_value_ewaybill']) #For comparing float values

    #Identify discrepancies
    merged_df.loc[~merged_df['invoice_date_match'], 'discrepancy'] += 'Invoice Date Mismatch; '
    merged_df.loc[~merged_df['gstin_supplier_match'], 'discrepancy'] += 'Supplier GSTIN Mismatch; '
    merged_df.loc[~merged_df['gstin_recipient_match'], 'discrepancy'] += 'Recipient GSTIN Mismatch; '
    merged_df.loc[~merged_df['invoice_value_match'], 'discrepancy'] += 'Invoice Value Mismatch; '
    merged_df.loc[~merged_df['gstin_supplier_valid_invoice'], 'discrepancy'] += 'Invalid Supplier GSTIN in Invoice; '
    merged_df.loc[~merged_df['gstin_recipient_valid_invoice'], 'discrepancy'] += 'Invalid Recipient GSTIN in Invoice; '
    merged_df.loc[~merged_df['gstin_supplier_valid_ewaybill'], 'discrepancy'] += 'Invalid Supplier GSTIN in Ewaybill; '
    merged_df.loc[~merged_df['gstin_recipient_valid_ewaybill'], 'discrepancy'] += 'Invalid Recipient GSTIN in Ewaybill; '

    # Filter out rows with no discrepancies
    discrepancies_df = merged_df[merged_df['discrepancy'] != '']

    return discrepancies_df

def cross_check_gstr1_gstr3b(gstr1_df, gstr3b_df):
    """
    Cross-checks GSTR-1 data against GSTR-3B data for discrepancies in outward supplies and tax liability.

    Args:
        gstr1_df (pd.DataFrame): DataFrame containing GSTR-1 data.  Must have columns:
                                     'taxable_value', 'igst', 'cgst', 'sgst'. Assumed to be total values.
        gstr3b_df (pd.DataFrame): DataFrame containing GSTR-3B data.  Must have columns:
                                     'taxable_value', 'igst', 'cgst', 'sgst'. Assumed to be total values.

    Returns:
        pd.DataFrame: DataFrame with a single row containing the discrepancies found. Empty if no discrepancies.
                      Columns: 'taxable_value_diff', 'igst_diff', 'cgst_diff', 'sgst_diff', 'total_diff', 'discrepancy'
    """

    #Calculate the totals from GSTR-1 and GSTR-3B
    gstr1_totals = gstr1_df[['taxable_value', 'igst', 'cgst', 'sgst']].sum()
    gstr3b_totals = gstr3b_df[['taxable_value', 'igst', 'cgst', 'sgst']].sum()

    #Calculate the differences
    diff_series = gstr1_totals - gstr3b_totals
    diff_df = pd.DataFrame(diff_series).T
    diff_df.columns = [f'{col}_diff' for col in diff_df.columns] #Rename columns to indicate differences

    #Calculate Total Difference
    diff_df['total_diff'] = diff_df['igst_diff'] + diff_df['cgst_diff'] + diff_df['sgst_diff']

    #Initialize discrepancy column
    diff_df['discrepancy'] = ''

    #Flag discrepancies
    tolerance = 1.0 #Set a tolerance for differences due to rounding errors.
    for col in ['taxable_value', 'igst', 'cgst', 'sgst', 'total']:
        diff_col = f'{col}_diff'
        if abs(diff_df[diff_col][0]) > tolerance: #Access the value in the dataframe to compare with tolerance
            diff_df.loc[0, 'discrepancy'] += f'{col} difference exceeds tolerance ({diff_df[diff_col][0]}); ' #Access the value in the dataframe to add to the string


    if diff_df['discrepancy'][0] == '':
        return pd.DataFrame() #return an empty dataframe if no discrepancies
    else:
        return diff_df #Return the dataframe with the discrepancy.

def cross_check_gstr2b_purchase_records(gstr2b_df, purchase_records_df):
    """
    Cross-checks GSTR-2B data against purchase records for discrepancies in Input Tax Credit (ITC).

    Args:
        gstr2b_df (pd.DataFrame): DataFrame containing GSTR-2B data. Must have columns:
                                    'invoice_number', 'invoice_date', 'gstin_supplier',
                                    'itc_eligible', 'itc_amount'.
        purchase_records_df (pd.DataFrame): DataFrame containing purchase records. Must have columns:
                                            'invoice_number', 'invoice_date', 'gstin_supplier',
                                            'itc_claimed', 'invoice_value'.

    Returns:
        pd.DataFrame: DataFrame containing discrepancies found, or an empty DataFrame if no discrepancies.
                       The DataFrame will include columns from both input DataFrames plus a 'discrepancy' column.
    """

    #Validate GSTIN
    gstr2b_df = gstr2b_df.copy() # Avoid modifying the original DataFrame
    purchase_records_df = purchase_records_df.copy()
    gstr2b_df['gstin_supplier_valid'] = gstr2b_df['gstin_supplier'].apply(validate_gstin)
    purchase_records_df['gstin_supplier_valid'] = purchase_records_df['gstin_supplier'].apply(validate_gstin)

    # Merge the dataframes on invoice_number and gstin_supplier
    merged_df = pd.merge(gstr2b_df, purchase_records_df, on=['invoice_number', 'gstin_supplier'], suffixes=('_gstr2b', '_purchase'), how='outer', indicator=True)

    # Create a 'discrepancy' column
    merged_df['discrepancy'] = ''

    # Check for invoices only in GSTR-2B
    gstr2b_only = merged_df['_merge'] == 'left_only'
    merged_df.loc[gstr2b_only, 'discrepancy'] += 'Invoice only in GSTR-2B; '

    # Check for invoices only in Purchase Records
    purchase_only = merged_df['_merge'] == 'right_only'
    merged_df.loc[purchase_only, 'discrepancy'] += 'Invoice only in Purchase Records; '

    # Cross-check common fields for matching invoices
    both_present = merged_df['_merge'] == 'both'
    merged_df.loc[both_present, 'invoice_date_match'] = merged_df.loc[both_present].apply(lambda row: safe_string_comparison(row['invoice_date_gstr2b'], row['invoice_date_purchase']), axis=1)
    merged_df.loc[both_present, 'itc_amount_match'] = np.isclose(merged_df.loc[both_present, 'itc_amount'], merged_df.loc[both_present, 'itc_claimed']) #Comparing ITC amounts
    merged_df.loc[both_present, 'gstin_supplier_valid_gstr2b_flag'] = merged_df.loc[both_present, 'gstin_supplier_valid_gstr2b'] #Boolean Value
    merged_df.loc[both_present, 'gstin_supplier_valid_purchase_flag'] = merged_df.loc[both_present, 'gstin_supplier_valid_purchase'] #Boolean Value

    #Identify discrepancies for matching invoices
    merged_df.loc[(both_present) & (~merged_df['invoice_date_match']), 'discrepancy'] += 'Invoice Date Mismatch; '
    merged_df.loc[(both_present) & (~merged_df['itc_amount_match']), 'discrepancy'] += 'ITC Amount Mismatch; '
    merged_df.loc[(both_present) & (merged_df['itc_eligible'].astype(str).str.lower() == 'false'), 'discrepancy'] += 'ITC Not Eligible as per GSTR-2B; ' #Check if ITC is eligible according to GSTR-2B
    merged_df.loc[(both_present) & (~merged_df['gstin_supplier_valid_gstr2b_flag']), 'discrepancy'] += 'Invalid Supplier GSTIN in GSTR2B; '
    merged_df.loc[(both_present) & (~merged_df['gstin_supplier_valid_purchase_flag']), 'discrepancy'] += 'Invalid Supplier GSTIN in Purchase Records; '


    # Filter out rows with no discrepancies
    discrepancies_df = merged_df[merged_df['discrepancy'] != '']

    return discrepancies_df

def cross_check_export_documents(export_invoice_df, shipping_bill_df, brc_df):
    """
    Cross-checks export invoice data against shipping bill and bank realization certificate (BRC) data.

    Args:
        export_invoice_df (pd.DataFrame): DataFrame containing export invoice data. Must have columns:
                                            'invoice_number', 'invoice_date', 'invoice_value',
                                            'gstin_supplier', 'export_value'.
        shipping_bill_df (pd.DataFrame): DataFrame containing shipping bill data. Must have columns:
                                            'shipping_bill_number', 'invoice_number', 'shipping_date',
                                            'export_value_shipping'.
        brc_df (pd.DataFrame): DataFrame containing bank realization certificate data. Must have columns:
                                    'brc_number', 'invoice_number', 'realization_date', 'realized_amount'.

    Returns:
        pd.DataFrame: DataFrame containing discrepancies found, or an empty DataFrame if no discrepancies.
    """

    #Validate GSTIN
    export_invoice_df = export_invoice_df.copy() # Avoid modifying the original DataFrame
    export_invoice_df['gstin_supplier_valid'] = export_invoice_df['gstin_supplier'].apply(validate_gstin)

    # Merge export invoice with shipping bill on invoice_number
    merged_df = pd.merge(export_invoice_df, shipping_bill_df, on='invoice_number', suffixes=('_invoice', '_shipping'), how='left') # Left join to keep all export invoices

    # Merge with BRC data on invoice_number
    merged_df = pd.merge(merged_df, brc_df, on='invoice_number', how='left') # Left join to keep all export invoices + shipping bills

    # Create a discrepancy column
    merged_df['discrepancy'] = ''

    # Check for missing shipping bill
    merged_df.loc[merged_df['shipping_bill_number'].isnull(), 'discrepancy'] += 'Missing Shipping Bill; '

    # Check for missing BRC
    merged_df.loc[merged_df['brc_number'].isnull(), 'discrepancy'] += 'Missing Bank Realization Certificate; '

    # Cross-check common fields (for rows with both shipping bill and BRC)
    both_present = ~(merged_df['shipping_bill_number'].isnull() | merged_df['brc_number'].isnull())

    merged_df.loc[both_present, 'invoice_date_match'] = merged_df.loc[both_present].apply(lambda row: safe_string_comparison(row['invoice_date'], row['shipping_date']), axis=1)
    merged_df.loc[both_present, 'export_value_match'] = np.isclose(merged_df.loc[both_present, 'export_value'], merged_df.loc[both_present, 'export_value_shipping'])
    merged_df.loc[both_present, 'realized_amount_less_than_export'] = merged_df.loc[both_present, 'realized_amount'] < merged_df.loc[both_present, 'export_value']
    merged_df.loc[both_present, 'gstin_supplier_valid_flag'] = merged_df.loc[both_present, 'gstin_supplier_valid'] #Boolean Value

    # Identify discrepancies in common fields
    merged_df.loc[(both_present) & (~merged_df['invoice_date_match']), 'discrepancy'] += 'Invoice Date and Shipping Date Mismatch; '
    merged_df.loc[(both_present) & (~merged_df['export_value_match']), 'discrepancy'] += 'Export Value Mismatch between Invoice and Shipping Bill; '
    merged_df.loc[(both_present) & (merged_df['realized_amount_less_than_export']), 'discrepancy'] += 'Realized Amount Less Than Export Value; '
    merged_df.loc[(both_present) & (~merged_df['gstin_supplier_valid_flag']), 'discrepancy'] += 'Invalid Supplier GSTIN in Export Invoice; '

    # Filter out rows with no discrepancies
    discrepancies_df = merged_df[merged_df['discrepancy'] != '']

    return discrepancies_df

def analyze_circular_trading(transaction_df, gstin_col='gstin_supplier', threshold=3):
    """
    Analyzes transaction data to identify potential circular trading patterns.

    Args:
        transaction_df (pd.DataFrame): DataFrame containing transaction data. Must have columns:
                                          'gstin_supplier', 'gstin_recipient', 'invoice_number'.  Other columns
                                          are acceptable, but these three are required.
        gstin_col (str): The column name representing the GSTIN to analyze (default: 'gstin_supplier').
        threshold (int): The minimum number of transactions forming a cycle to flag as suspicious (default: 3).

    Returns:
        pd.DataFrame: DataFrame containing information about potential circular trading cycles.
                       Returns an empty DataFrame if no cycles are found.  Columns:
                       'cycle': A list of GSTINs forming the cycle.
                       'cycle_length': The number of GSTINs in the cycle.
    """

    # Create a directed graph from the transaction data
    graph = nx.DiGraph()

    for _, row in transaction_df.iterrows():
        supplier = row['gstin_supplier']
        recipient = row['gstin_recipient']
        graph.add_edge(supplier, recipient)

    # Find cycles in the graph
    cycles = list(nx.simple_cycles(graph))

    # Filter cycles based on the threshold
    suspicious_cycles = [cycle for cycle in cycles if len(cycle) >= threshold]

    if not suspicious_cycles:
        return pd.DataFrame()  # Return empty DataFrame if no suspicious cycles found

    # Create a DataFrame from the suspicious cycles
    cycles_df = pd.DataFrame({'cycle': suspicious_cycles})
    cycles_df['cycle_length'] = cycles_df['cycle'].apply(len)

    return cycles_df

def cross_check_gstr2b_annexureb(gstr2b_df, annexureb_df, inv_num_col='invoice_number', itc_available_col='ITC Available', export_inv_num_col='Export Invoice Number'):
    """
    Checks if export invoices (Annexure B) are claiming ITC in GSTR-2B, which is generally not allowed.

    Args:
        gstr2b_df (pd.DataFrame): DataFrame containing GSTR-2B data. Must have columns:
                                     invoice_number, ITC Available.
        annexureb_df (pd.DataFrame): DataFrame containing Annexure B (export invoices) data.
                                      Must have columns: Export Invoice Number.
        inv_num_col (str, optional): Column name for invoice number in GSTR-2B (default: 'invoice_number').
        itc_available_col (str, optional): Column name for ITC Available in GSTR-2B (default: 'ITC Available').
        export_inv_num_col (str, optional): Column name for Export Invoice Number in Annexure B (default: 'Export Invoice Number').

    Returns:
        pd.DataFrame: DataFrame containing discrepancies found, or an empty DataFrame if no discrepancies.
                       Columns will include the merged data and a 'discrepancy' column.
    """

    #Make a copy to avoid modifying original DataFrames
    gstr2b_df = gstr2b_df.copy()
    annexureb_df = annexureb_df.copy()

    # Convert ITC Available to boolean
    gstr2b_df["ITC Available Bool"] = gstr2b_df[itc_available_col].astype(str).str.lower().map({"yes": True, "true": True, "1": True}).fillna(False)

    #Lowercase invoice numbers for safer merging
    annexureb_df["Export Invoice Number Lower"] = annexureb_df[export_inv_num_col].astype(str).str.lower()
    gstr2b_df[inv_num_col + "_Lower"] = gstr2b_df[inv_num_col].astype(str).str.lower()

    # Merge based on the invoice number
    merged_df = pd.merge(
        annexureb_df,
        gstr2b_df[gstr2b_df["ITC Available Bool"]],  # Only consider rows where ITC is available in 2B
        left_on="Export Invoice Number Lower",
        right_on=inv_num_col + "_Lower",
        how="inner",  # Find rows present in both
        suffixes=('_annexb', '_gstr2b')
    )

    #Create Discrepancy Column
    merged_df['discrepancy'] = ''
    merged_df.loc[~merged_df["ITC Available Bool"], 'discrepancy'] += "Export Invoice found in GSTR-2B with ITC claimed"

    # Select only the rows with discrepancies
    discrepancies_df = merged_df[merged_df['discrepancy'] != '']

    return discrepancies_df

def cross_check_gstr3b_generaldata(gstr3b_df, gstgen_df, total_taxable_value_col='Total Taxable Value',
                                     total_amount_col='Total Amount', tax_period_col='Tax Period',
                                     invoice_date_col='Invoice Date', tolerance_rel=0.01, tolerance_abs=0.01):
    """
    Compares the total taxable value reported in GSTR-3B with the sum of invoice amounts in the general data.

    Args:
        gstr3b_df (pd.DataFrame): DataFrame containing GSTR-3B data. Must have columns:
                                     Total Taxable Value, Tax Period.
        gstgen_df (pd.DataFrame): DataFrame containing general invoice data. Must have columns:
                                     Total Amount, Invoice Date.
        total_taxable_value_col (str, optional): Column name for Total Taxable Value in GSTR-3B (default: 'Total Taxable Value').
        total_amount_col (str, optional): Column name for Total Amount in general data (default: 'Total Amount').
        tax_period_col (str, optional): Column name for Tax Period in GSTR-3B (default: 'Tax Period').
        invoice_date_col (str, optional): Column name for Invoice Date in general data (default: 'Invoice Date').
        tolerance_rel (float, optional): Relative tolerance for comparison (default: 0.01).
        tolerance_abs (float, optional): Absolute tolerance for comparison (default: 0.01).

    Returns:
        pd.DataFrame: DataFrame with a single row containing the comparison results and discrepancy flag,
                       or an empty DataFrame if no discrepancies are found.
    """

    #Make copies to avoid modifying original dataframes
    gstr3b_df = gstr3b_df.copy()
    gstgen_df = gstgen_df.copy()

    #Convert amounts to numeric
    gstgen_df[total_amount_col] = pd.to_numeric(gstgen_df[total_amount_col], errors="coerce")
    gstr3b_df[total_taxable_value_col] = pd.to_numeric(gstr3b_df[total_taxable_value_col], errors="coerce")

    #Sum total amount from general data
    total_taxable_value_gen = gstgen_df[total_amount_col].sum()

    #Sum total taxable value from GSTR-3B
    total_taxable_value_3b = gstr3b_df[total_taxable_value_col].sum()

    #Compare sums with tolerance
    is_close = np.isclose(
        total_taxable_value_gen,
        total_taxable_value_3b,
        rtol=tolerance_rel,
        atol=tolerance_abs,
    )

    #Create DataFrame for results
    results_df = pd.DataFrame({
        'total_taxable_value_gen': [total_taxable_value_gen],
        'total_taxable_value_3b': [total_taxable_value_3b],
        'is_close': [is_close]
    })

    results_df['discrepancy'] = ''
    if not is_close:
        results_df['discrepancy'] = f"Significant Mismatch: GSTR-3B Total={total_taxable_value_3b:.2f}, General Data Total={total_taxable_value_gen:.2f}"

    if results_df['discrepancy'][0] == '': #Check if there are no discrepancies
        return pd.DataFrame() #Returns Empty dataframe if there are no discrepancy.
    else:
        return results_df #Returns DataFrame if there is any discrepancy

def cross_check_ewaybill_generaldata(ewaybill_df, gstgen_df, eway_inv_num_col='Invoice Number', eway_total_val_col='Total Value',
                                      inv_num_col='Invoice Number', total_amount_col='Total Amount'):
    """
    Compares the total value in e-way bills with the total amount in general invoice data, matching by invoice number.

    Args:
        ewaybill_df (pd.DataFrame): DataFrame containing e-way bill data. Must have columns:
                                      Invoice Number, Total Value.
        gstgen_df (pd.DataFrame): DataFrame containing general invoice data. Must have columns:
                                      Invoice Number, Total Amount.
        eway_inv_num_col (str, optional): Column name for Invoice Number in e-way bill data (default: 'Invoice Number').
        eway_total_val_col (str, optional): Column name for Total Value in e-way bill data (default: 'Total Value').
        inv_num_col (str, optional): Column name for Invoice Number in general data (default: 'Invoice Number').
        total_amount_col (str, optional): Column name for Total Amount in general data (default: 'Total Amount').

    Returns:
        pd.DataFrame: DataFrame containing discrepancies found, or an empty DataFrame if no discrepancies.
    """

    #Make a copy to avoid modifying original dataframes
    ewaybill_df = ewaybill_df.copy()
    gstgen_df = gstgen_df.copy()

    # Prepare for merge: lowercase invoice numbers, convert amounts
    ewaybill_df[eway_inv_num_col + "_Lower"] = ewaybill_df[eway_inv_num_col].astype(str).str.lower()
    gstgen_df[inv_num_col + "_Lower"] = gstgen_df[inv_num_col].astype(str).str.lower()
    ewaybill_df[eway_total_val_col] = pd.to_numeric(ewaybill_df[eway_total_val_col], errors="coerce")
    gstgen_df[total_amount_col] = pd.to_numeric(gstgen_df[total_amount_col], errors="coerce")

    # Keep only relevant columns and drop NaNs before merge
    ewaybill_compare = ewaybill_df[[eway_inv_num_col + "_Lower", eway_total_val_col]].dropna().drop_duplicates(subset=[eway_inv_num_col + "_Lower"])
    gstgen_compare = gstgen_df[[inv_num_col + "_Lower", total_amount_col]].dropna().drop_duplicates(subset=[inv_num_col + "_Lower"])

    # Merge on lowercase invoice number
    merged_inv_val = pd.merge(
        ewaybill_compare,
        gstgen_compare,
        left_on=eway_inv_num_col + "_Lower",
        right_on=inv_num_col + "_Lower",
        how="inner",  # Only compare invoices present in both
        suffixes=('_ewaybill', '_general')
    )

    # Check for mismatches with tolerance
    tolerance_abs = 0.01
    mismatch_mask = ~np.isclose(
        merged_inv_val[eway_total_val_col],
        merged_inv_val[total_amount_col],
        atol=tolerance_abs,
    )

    merged_inv_val['discrepancy'] = '' #Added to create the discrepancy column
    merged_inv_val.loc[mismatch_mask, 'discrepancy'] = f"Total value mismatch between E-way Bill and General Invoice" #Adding the discrepancy to column if it exists
    discrepancies_df = merged_inv_val[merged_inv_val['discrepancy'] != ''] #Get the dataframe that has discrepancy

    return discrepancies_df