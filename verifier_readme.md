# GST Data Verifier - Documentation

## 1. Overview

The GST Data Verifier (`gst_verifier_optimized.py`) is a Python tool designed to automate the process of validating and verifying Goods and Services Tax (GST) related data from various sources like CSV, Excel (.xls, .xlsx), and JSON files. It checks individual data files for correctness (e.g., format, consistency, valid values) based on configurable rules and also performs cross-document consistency checks between different types of GST reports (like GSTR-2B vs. Annexure B).

The tool is optimized using the `pandas` and `numpy` libraries for efficient data handling and validation, especially with larger datasets. It operates via a command-line interface (CLI), prompting the user for necessary information and allowing them to select specific verification tasks.

## 2. Features

* **Multi-Format Support:** Loads data from CSV, Excel (.xls, .xlsx), and JSON files.
* **Configurable Validation:** Uses a `config.json` file for defining file identification, column mappings, date formats, GST rates, and valid code lists (e.g., port codes, transport modes).
* **Party-Specific Organization:** Manages data and configuration based on a "party" (e.g., client, entity), using dedicated folders.
* **Specific Data Checks:** Performs detailed validation on various GST data types:
  * General Invoice Data (`gst_gen_data`)
  * GSTR-2B Data (`gstr2b_data`)
  * Annexure B (Export) Data (`annexureb_data`)
  * GSTR-3B Data (`gstr3b_data`)
  * RFD-01 (Refund) Data (`rfd01_data`)
  * E-way Bill Data (`ewaybill_data`)
* **Validation Types:** Includes checks for:
  * Missing Columns
  * Invalid GSTIN Formats
  * Invalid Date Formats & Logic (e.g., future dates, period consistency)
  * Invalid Numeric Formats & Negative Values
  * Data Consistency (e.g., tax amount vs. total amount and rate)
  * Valid Codes (e.g., Port Codes, Transport Modes)
  * Anomaly Detection (e.g., unusually high ITC claims in GSTR-3B)
* **Cross-Document Consistency:** Compares data across different loaded files (e.g., ensuring export invoices don't have ITC claimed in GSTR-2B, comparing total values between E-way Bills and invoices).
* **Efficient Processing:** Leverages `pandas` and `numpy` for vectorized operations, significantly speeding up validation compared to row-by-row iteration.
* **Reporting:** Generates a detailed JSON report (`verification_report.json` by default) summarizing all errors found during the checks, saved within a party-specific `results` folder.
* **Interactive CLI:** Provides a menu-driven interface for selecting checks, generating reports, and viewing summaries.

## 3. Requirements

* **Python:** Python 3.7 or higher recommended.
* **Libraries:**
  * `pandas`
  * `numpy`
  * `openpyxl` (for reading `.xlsx` files, automatically installed with pandas usually)
  * `xlrd` (may be needed for reading older `.xls` files)

    Install required libraries using pip:

    ```bash
    pip install pandas numpy openpyxl xlrd
    ```

## 4. File Structure

The script expects a specific directory structure based on the "party name" provided by the user:

```
Your_Project_Folder/
├── gst_verifier_optimized.py   # The main script
│
└── party_xyz/                  # <-- Party Folder (name entered by user, e.g., "client_a")
    │
    ├── config.json             # <-- Party-specific configuration file (REQUIRED)
    │
    ├── results/                # <-- Output reports folder (created automatically)
    │   └── party_xyz_verification_report_YYYYMMDD.json # Example report
    │
    ├── Invoices_Q1_2024.xlsx   # <-- Example data file (matches a prefix in config.json)
    ├── GSTR2B_Apr2024.csv      # <-- Example data file
    ├── Exports_Apr_Jun.xls     # <-- Example data file
    └── EwayBills_May2024.json  # <-- Example data file
    └── ... (other data files)
│
└── another_party/              # <-- Folder for a different party
    └── ... (similar structure with its own config.json, data, results)

```

* The script should be run from `Your_Project_Folder`.
* When prompted, enter the exact name of the party folder (e.g., `party_xyz`).
* The script will look for `config.json` inside the specified party folder.
* Data files should be placed directly inside the party folder.
* The `results` subfolder will be created automatically if it doesn't exist.

## 5. Configuration (`config.json`)

This file is **essential** and must exist inside each party folder. It tells the script how to identify files, find specific data columns, and apply certain validation rules.

```json
{
  "file_prefixes": {
    "gst_gen": "Invoices_",
    "gstr2b": "GSTR2B_",
    "annexureb": "Exports_",
    "gstr3b": "GSTR3B_",
    "rfd01": "RFD01_",
    "ewaybill": "EwayBills_"
  },
  "data_mapping": {
    "gstin": "GST Identification Number",
    "invoice_number": "Bill Number",
    "invoice_date": "Bill Date",
    "total_amount": "Total Value",
    "tax_amount": "Total Tax"
  },
  "date_format": "%d-%m-%Y",
  "gst_rate": 18.0,
  "valid_port_codes": [
    "INBOM4",
    "INMAA1",
    "INDEL4",
    "INNSA1"
  ],
  "valid_transport_modes": [
    "Road",
    "Rail",
    "Air",
    "Ship",
    "Vehicle"
  ],
  "valid_refund_reasons": [
    "Export of goods/services (with payment of tax)",
    "Export of goods/services (without payment of tax)",
    "Excess cash balance in electronic cash ledger",
    "Inverted tax structure",
    "Refund by recipient of deemed export"
  ]
}
```

**Explanation of Keys:**

* **`file_prefixes` (Required):** Maps internal data type keys (e.g., `gst_gen`) to the starting text of the corresponding filename. The script uses this to identify which file contains which type of data.
* **`data_mapping` (Optional but Recommended):** Maps standard internal column names (like `gstin`, `invoice_number`) to the actual column names used in your data files. If a mapping isn't provided for a standard name, the script will assume the standard name itself is the column header (e.g., expects a column named exactly "GSTIN" if `gstin` is not mapped).
* **`date_format` (Optional, Default: "%Y-%m-%d"):** Specifies the date format string (using Python's `strptime` conventions) present in your data files. Ensure consistency across your files or handle variations carefully.
* **`gst_rate` (Optional):**
  * If a **numeric value** (e.g., `18.0`), it's treated as a fixed GST rate percentage used for validating tax amounts in general invoice data.
  * If a **string value** (e.g., `"GST Rate Column"`), it's treated as the name of the column *within the general invoice data file* that contains the applicable GST rate for each row.
  * If **omitted**, tax amount consistency checks in general data might be skipped or produce warnings.
* **`valid_port_codes` (Optional):** A list of acceptable port codes for Annexure B validation. If omitted, uses a small default list.
* **`valid_transport_modes` (Optional):** A list of acceptable transport modes for E-way Bill validation. If omitted, uses a default list ("Road", "Rail", "Air", "Ship", "Vehicle").
* **`valid_refund_reasons` (Optional):** A list of acceptable reasons for refund for RFD-01 validation. If omitted, uses a small default list.

## 6. Usage / Running the Script

1. **Prepare:** Ensure Python and the required libraries are installed. Organize your data files and `config.json` into the correct party folder structure (see Section 4).
2. **Navigate:** Open your terminal or command prompt and navigate to the directory containing `gst_verifier_optimized.py` (`Your_Project_Folder`).
3. **Execute:** Run the script using:

    ```bash
    python gst_verifier_optimized.py
    ```

4. **Enter Party Name:** The script will first prompt you to enter the party name. Type the exact name of the folder containing the data and config file (e.g., `party_xyz`).
5. **Loading:** The script will attempt to:
    * Find the party folder.
    * Load `config.json`.
    * Create the `results` subfolder if needed.
    * Load all data files matching the prefixes defined in `config.json`.
    * Prompt for missing essential configurations (like `date_format`, `file_prefixes`) if `config.json` is missing or incomplete.
6. **Main Menu:** You will be presented with a menu of available actions based on the data loaded:
    * Options to check specific data types (e.g., "Check General Data") will appear if the corresponding data file was loaded successfully.
    * "Check Cross-Document Consistency" will appear if at least two relevant data files for comparison are loaded.
    * Standard options: "Display Summary", "Generate Report", "Exit".
7. **Select Action:** Enter the number corresponding to the desired action and press Enter.
8. **Follow Prompts:** For report generation, you might be asked for an output filename (a default is provided).
9. **Repeat:** Continue selecting actions until you choose "Exit".

## 7. Code Structure / Key Components

* **`GSTVerifier` Class:** Encapsulates all the logic for loading, configuring, validating, and reporting.
  * `__init__`: Initializes attributes (report dictionary, data dictionaries, config path).
  * `run`: The main execution loop driving the CLI menu and user interaction.
  * `get_party_details`: Handles setting up the context for a specific party (loading config, loading all data, prompting if needed).
  * `load_data`: Loads a single data file (CSV, Excel, JSON).
  * `load_all_data`: Scans the party directory and loads all files matching configured prefixes.
  * `load_and_set_current_data`: Helper to select the active DataFrame for checking.
  * `check_data`: Dispatches validation tasks to the appropriate specific check method based on user choice.
  * `_check_*` methods (e.g., `_check_general_data`, `_check_gstr2b_data`): Contain the core validation logic for each specific data type, heavily utilizing pandas/numpy.
  * `check_cross_document_consistency`: Performs comparisons between different loaded DataFrames.
  * `generate_report`: Saves the accumulated errors into a JSON file.
  * `display_summary`: Prints a high-level summary of errors found to the console.
  * `_load_config`, `_validate_gstin`, `_create_results_folder`, `_add_error`, `_check_columns`: Internal helper methods.

## 8. Output

* **Console Output:**
  * Progress messages during loading and checking.
  * Error messages printed directly when validations fail within a check function.
  * Summary of errors per section when using the "Display Summary" option.
* **JSON Report:**
  * A file (e.g., `party_xyz_verification_report_YYYYMMDD.json`) saved in the `results` subfolder of the party's directory.
  * The JSON contains a top-level object where keys correspond to the checks performed (e.g., `"gst_gen_data"`, `"cross_document_consistency"`).
  * Each key maps to an object containing an `"errors"` key, which holds a list of dictionaries.
  * Each error dictionary provides details like:
    * `row`: The approximate row number in the original file (Excel-style, 1-based index + header).
    * `column`/`columns`: The column(s) involved in the error.
    * `value`/`values`: The problematic value(s).
    * `description`: A textual description of the error found.
    * (Sometimes) `documents`: Indicates which files were involved in a cross-document error.
    * (Sometimes) `invoice_number` or other identifiers for context.

## 9. Customization / Extension

* **Add New Data Types:**
    1. Define a new key and filename prefix in `config.json` (`file_prefixes`).
    2. Create a new `_check_newdata_data` method in the `GSTVerifier` class with the specific validation logic using pandas.
    3. Add the new key and method to the `check_methods` dictionary in `check_data`.
    4. Update the dynamic menu generation in `run` to include an option for the new check if its data is loaded.
* **Modify Validations:** Edit the logic within the existing `_check_*` methods or `check_cross_document_consistency`.
* **Adjust Configuration:** Modify `config.json` within each party folder to match their specific file naming conventions, column headers, and validation parameters.
* **Support New File Formats:** Enhance the `load_data` method.

## 10. Troubleshooting

* **`FileNotFoundError`:** Double-check that the party name entered matches the folder name exactly and that the required `config.json` or data files exist within that folder. Ensure you are running the script from the correct parent directory.
* **`KeyError`:** Often indicates a missing key in `config.json` (`file_prefixes`, or a specific mapping in `data_mapping`) or a mismatch between column names defined in `config.json` (`data_mapping`) and the actual column names in the data files. Verify your `config.json`.
* **JSON Decode Error:** The `config.json` file might have syntax errors. Use a JSON validator to check it. Or, a data JSON file might be corrupt.
* **Date Parsing Errors:** Ensure the `date_format` in `config.json` precisely matches the format in your data files. Check for inconsistent date formats within a single column.
* **Encoding Errors (CSV):** The script tries `utf-8` then `latin1`. If you encounter persistent encoding errors, you may need to identify the correct encoding of your CSV file and potentially modify the `load_data` function or resave the CSV with UTF-8 encoding.
* **Numeric Conversion Errors:** Check the relevant amount/value columns in your source files for non-numeric characters (e.g., currency symbols, commas, text) that `pandas.to_numeric` cannot automatically handle. Data cleaning might be required before running the verifier.
