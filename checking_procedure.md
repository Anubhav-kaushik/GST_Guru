**General Principles for All Document Checks:**

1.  **Data Loading:**  Each checking function will need to accept the filepath of the document to be checked (CSV, JSON, Excel). It should load the data into a pandas DataFrame. Include error handling for file not found, incorrect format, etc.
2.  **Column Existence:** Verify that all expected columns exist in the loaded data. Report if any required columns are missing.
3.  **Data Type Validation:** Check the data types of each column. For example, dates should be in date format, amounts should be numeric, GSTINs should be strings.
4.  **Null/Empty Value Handling:** Decide how to handle null or empty values in each column.  Should they be flagged as errors, or are they acceptable in certain cases?
5.  **Error Reporting:** The checking functions should return a structured report of any errors or discrepancies found. This report should include the row number, column name, the actual value, and a description of the error.
6.  **Modularity:** Design the checking functions to be modular and reusable.  If you have common validation logic (e.g., GSTIN validation), create separate helper functions.

**Document-Specific Check Roadmaps:**

**1. General Invoice Data (GST Test Data):**

*   **Columns:**
    *   GSTIN
    *   Invoice Number
    *   Invoice Date
    *   Supplier Name
    *   Recipient Name
    *   Total Amount
    *   Tax Amount
    *   Item Description
    *   Fraud Scenario (for testing, should be ignored in production)
*   **Checks:**
    *   **GSTIN:**
        *   Valid format (15-digit alphanumeric).
        *   Check for existence in GST database (if possible via API, otherwise flag as "check manually").
    *   **Invoice Number:**
        *   Check for duplicates within the dataset.
        *   Check for special characters or invalid characters.
    *   **Invoice Date:**
        *   Check for future dates.
        *   Check if the date is within the valid financial year.
    *   **Total Amount:**
        *   Check if the amount is positive.
        *   Check for unusually high amounts compared to other invoices from the same supplier (anomaly detection).
    *   **Tax Amount:**
        *   Check if the tax amount is consistent with the total amount (verify the GST rate).
        *   Check if the tax amount is within a reasonable range based on the item description.
    *   **Fraud Scenario:** (Ignore in production, only for testing)
        *   Verify that each known scenario are properly working

**2. GSTR-2B Data:**

*   **Columns:**
    *   GSTIN
    *   Invoice Number
    *   Invoice Date
    *   Supplier Name
    *   IGST Amount
    *   CGST Amount
    *   SGST Amount
    *   Total Taxable Value
    *   ITC Available
*   **Checks:**
    *   **GSTIN:**
        *   Valid format.
        *   Check against GST database (if possible).
    *   **Invoice Number:**
        *   Check for duplicates.
        *   Check for consistency with invoice numbers from other documents (e.g., purchase invoices).
    *   **Invoice Date:**
        *   Check for future dates.
        *   Check if the date is within the relevant tax period.
    *   **IGST Amount, CGST Amount, SGST Amount:**
        *   Check if the amounts are non-negative.
        *   Check if the sum of CGST and SGST is approximately equal to the IGST amount (for intra-state transactions).
        *   Check if tax amounts are reasonable based on the total taxable value.
    *   **Total Taxable Value:**
        *   Check if the value is positive.
    *   **ITC Available:**
        *   Check for inconsistencies where ITC is available, but the IGST, CGST, and SGST amounts are zero.  This could indicate a suspicious transaction.
        *   Compare the ITC available according to the GSTR-2B with the ITC claimed in the GSTR-3B (reconciliation).

**3. Annexure B Data (Exports):**

*   **Columns:**
    *   GSTIN
    *   Export Invoice Number
    *   Export Date
    *   Port Code
    *   Shipping Bill Number
    *   Shipping Bill Date
    *   Export Value
    *   Tax Paid
    *   Country of Destination
*   **Checks:**
    *   **GSTIN:**
        *   Valid format.
    *   **Export Invoice Number:**
        *   Check for duplicates.
    *   **Export Date:**
        *   Check for future dates.
    *   **Port Code:**
        *   Validate against a list of valid port codes.
    *   **Shipping Bill Number:**
        *   Check for valid format.
    *   **Shipping Bill Date:**
        *   Check for future dates.
        *   Ensure the shipping bill date is *not* before the export date.
    *   **Export Value:**
        *   Check if the value is positive.
    *   **Tax Paid:**
        *   Should typically be zero (exports are often zero-rated).  If not zero, investigate the reason.
    *   **Country of Destination:**
        *   Validate against a list of valid country codes.

**4. GSTR-3B Data:**

*   **Columns:**
    *   GSTIN
    *   Tax Period (YYYY-MM)
    *   Total Taxable Value
    *   IGST Paid
    *   CGST Paid
    *   SGST Paid
    *   ITC Claimed
*   **Checks:**
    *   **GSTIN:**
        *   Valid format.
    *   **Tax Period:**
        *   Check for valid format (YYYY-MM).
        *   Check for consistency with other returns for the same tax period.
    *   **Total Taxable Value:**
        *   Check if the value is non-negative.
    *   **IGST Paid, CGST Paid, SGST Paid:**
        *   Check if the amounts are non-negative.
    *   **ITC Claimed:**
        *   Check if the amount is non-negative.
        *   **Critical Check:** Verify that the ITC claimed in GSTR-3B is *less than or equal to* the ITC available as per GSTR-2B. This is a key reconciliation step.
        *   Check if the ITC claimed is reasonable based on the nature of the business and its input costs (anomaly detection).
    *   **Cross-Document Verification:**
        *   Compare the tax paid in GSTR-3B with the payment challans (GST PMT-06).

**5. RFD-01 Data (Refund Application):**

*   **Columns:**
    *   GSTIN
    *   Refund Period From
    *   Refund Period To
    *   Reason for Refund
    *   Refund Amount Claimed
    *   Bank Account Number
    *   Bank IFSC Code
*   **Checks:**
    *   **GSTIN:**
        *   Valid format.
    *   **Refund Period From & To:**
        *   Check for valid date formats.
        *   Ensure the "To" date is not before the "From" date.
        *   Ensure the refund period is within the allowed limits.
    *   **Reason for Refund:**
        *   Validate against a list of valid refund reasons.
    *   **Refund Amount Claimed:**
        *   Check if the amount is positive.
        *   **Critical Check:** Verify the refund amount claimed is justified based on the reason for refund and the supporting documents. This will require analyzing the relevant invoices, export data, or other information.
    *   **Bank Account Number:**
        *   Check for valid format.
    *   **Bank IFSC Code:**
        *   Check for valid format.  Validate against a list of valid IFSC codes (if possible).
    *   **Supporting Documents:**
        *   Based on the reason for the refund, verify that all required supporting documents are attached.  This might involve checking for the existence of invoices, shipping bills, BRCs, etc.

**6. E-way Bill Data:**

*   **Columns:**
    *   GSTIN
    *   E-way Bill Number
    *   Generated Date
    *   Valid Until
    *   Supplier GSTIN
    *   Recipient GSTIN
    *   Invoice Number
    *   Invoice Date
    *   Total Value
    *   Transport Mode
    *   Distance (km)
*   **Checks:**
    *   **GSTIN, Supplier GSTIN, Recipient GSTIN:**
        *   Valid format.
    *   **E-way Bill Number:**
        *   Check for valid format.
        *   Check for duplicates.
    *   **Generated Date:**
        *   Check for valid date format.
    *   **Valid Until:**
        *   Check for valid date format.
        *   Ensure the "Valid Until" date is after the "Generated Date."
        *   Check if the E-way Bill is currently valid (not expired).
    *   **Invoice Number:**
        *   Check for valid format.
    *   **Invoice Date:**
        *   Check for valid date format.
        *   Ensure invoice date is not after the E-way Bill generated date.
    *   **Total Value:**
        *   Check if it matches the invoice
        *   Value should be positive
    *   **Transport Mode:**
        *   Validate against a list of transport modes
    *   **Distance:**
        *   Value should be positive

**Key Implementation Considerations:**

*   **External Data Sources:** Consider using external APIs or databases to validate GSTINs, bank codes, port codes, and other information.
*   **Flexibility:**  Design your checking functions to be flexible and configurable.  Allow users to customize the validation rules and thresholds.
*   **Performance:** Optimize the code for performance, especially when dealing with large datasets. Use vectorized operations where possible.

This detailed roadmap will help you systematically create the checking functions for your GST verification software. Remember to start with the most critical checks and gradually add more sophisticated validation logic as you develop the software further. Now that you have the tests, create the verification.
