# gst_verifier_optimized.py

import pandas as pd
import numpy as np
import re
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import logging

# Import the DataManager class from data_manager.py
from data_manager import DataManager

# Import the cross-checking functions from cross_check.py
import cross_check  # Import the module
from cross_check import (
    validate_gstin,
    cross_check_invoice_ewaybill,
    cross_check_gstr1_gstr3b,
    cross_check_gstr2b_purchase_records,
    cross_check_export_documents,
    analyze_circular_trading,
    cross_check_gstr2b_annexureb,
    cross_check_gstr3b_generaldata,
    cross_check_ewaybill_generaldata,
    process_itc_eligible,  # Import if you're using this directly in your Verifier class
    safe_string_comparison,
)


class GSTVerifier:
    """A class for verifying GST data from various sources, optimized with pandas and numpy."""

    def __init__(self, config_file="config.json"):
        self.report: Dict[str, Any] = {}
        self.all_data: Dict[str, pd.DataFrame] = {}  # Store all DataFrames
        self.data_files: Dict[str, str] = {}  # Store filepaths for each data type
        self.config_file: str = config_file
        self.config: Dict[str, Any] = {}  # Use Dict for type hinting
        self.party_name: Optional[str] = None
        self.results_folder: Optional[str] = None
        self.data: Optional[pd.DataFrame] = (
            None  # To hold the current DataFrame being processed
        )

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Loads configuration settings from a JSON file."""
        try:
            with open(config_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print(
                f"Error: Configuration file not found in {config_file}. Using default settings."
            )
            return {}
        except json.JSONDecodeError as e:
            print(f"Error decoding configuration file: {e}. Using default settings.")
            return {}

    def _validate_gstin(self, gstin: Any) -> bool:
        """Validates a GSTIN using a regular expression. Handles potential non-string inputs."""
        if pd.isna(gstin):
            return False
        pattern = r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$"
        return bool(re.match(pattern, str(gstin)))

    def _create_results_folder(self, party_folder: str) -> Optional[str]:
        """Creates the 'results' subfolder inside the party's folder."""
        results_folder = os.path.join(party_folder, "results")
        try:
            os.makedirs(results_folder, exist_ok=True)  # Create if not exists
            print(f"Created results folder: {results_folder}")
            return results_folder
        except Exception as e:
            print(f"Error creating results folder: {e}")
            return None

    def load_data(self, filepath: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Loads data from CSV, Excel, or JSON files using DataManager."""
        try:
            data_manager = DataManager(file_path=filepath)
            data = data_manager.data
            file_type = data_manager.file_type
            # Basic cleaning: strip whitespace from column names
            data.columns = data.columns.str.strip()
            print(f"Data loaded successfully from {filepath} ({file_type})")
            return data, file_type  # Indicate successful loading
        except Exception as e:
            print(f"Error loading data from {filepath}: {e}")
            return None, None  # Other load data errors

    def load_all_data(self, directory: str) -> bool:
        """Loads all GST-related data files from a directory based on config prefixes."""
        self.all_data = {}  # Reset data
        self.data_files = {}  # Reset file paths
        file_prefixes = self.config.get(
            "file_prefixes", {}
        )  # Get the file prefixes config
        if not file_prefixes:
            print(
                "Warning: 'file_prefixes' not found in config. Cannot identify specific data types."
            )
            # Optionally, load all supported files without specific types
            # return False # Or proceed to load all files generically

        loaded_files = 0
        try:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                if not os.path.isfile(filepath):  # Skip directories
                    continue

                file_loaded_for_type = False
                for data_type, prefix in file_prefixes.items():
                    # Ensure prefix comparison is case-insensitive if needed
                    if filename.lower().startswith(
                        prefix.lower()
                    ) and filename.lower().endswith((".csv", ".xls", ".xlsx", ".json")):

                        data, file_type = self.load_data(filepath)
                        if data is not None:
                            if data_type + "_data" in self.all_data:
                                print(
                                    f"Warning: Multiple files found for prefix '{prefix}' ({data_type}). Replacing previous data."
                                )
                            self.all_data[data_type + "_data"] = data
                            self.data_files[data_type + "_data"] = filepath
                            file_loaded_for_type = True
                            loaded_files += 1
                            break  # Move to the next file once a match is found

                # Optionally load files that don't match prefixes if needed
                # if not file_loaded_for_type and filename.lower().endswith((".csv", ".xls", ".xlsx", ".json")):
                #     # Handle files without a matching prefix
                #     pass

            if loaded_files > 0:
                print(f"Total {loaded_files} data files loaded successfully.")
                return True
            else:
                print(
                    f"No data files matching the prefixes in config were found in {directory}."
                )
                return False

        except FileNotFoundError:
            print(f"Error: Directory not found: {directory}")
            return False
        except Exception as e:
            print(f"Error loading all data from {directory}: {e}")
            return False

    def load_and_set_current_data(self, data_key: str) -> bool:
        """Sets the current DataFrame to be processed and ensures party details are handled."""
        if data_key not in self.all_data:
            print(
                f"Error: Data for '{data_key}' not loaded. Please load from Directory first."
            )
            self.data = None
            return False

        self.data = self.all_data[data_key]

        # Ensure party details and config are loaded if not already done
        if not self.party_name or not self.config:
            # This path might be complex if run() isn't called first.
            # Consider structuring so party setup happens once in run().
            if not self.party_name:
                print("Warning: Party name not set. Attempting to get details.")
                # Simplified: assume run() sets these up. If called directly, might need input here.
                return False  # Or prompt for party name/folder again
            party_folder = os.path.join(os.getcwd(), self.party_name)
            self.get_party_details(party_folder)  # Reload config if needed

        if self.data is None or self.data.empty:
            print(f"Warning: Data for '{data_key}' is empty.")
            return False

        print(f"Processing data for: {data_key}")
        return True

    def check_data(self, data_type_short: str) -> None:
        """
        Checks data based on the short data type key (e.g., 'gst_gen', 'gstr2b').
        """
        data_key = data_type_short + "_data"
        if not self.load_and_set_current_data(data_key):
            print(f"Skipping checks for {data_key} due to loading issues.")
            return

        # Ensure self.data is not None before proceeding
        if self.data is None:
            print(f"Error: self.data is None for {data_key}. Cannot perform checks.")
            return

        # Mapping short type to check method
        check_methods = {
            "gst_gen": self._check_general_data,
            "gstr2b": self._check_gstr2b_data,
            "annexureb": self._check_annexureb_data,
            "gstr3b": self._check_gstr3b_data,
            "rfd01": self._check_rfd01_data,
            "ewaybill": self._check_ewaybill_data,
        }

        check_function = check_methods.get(data_type_short)
        if check_function:
            check_function()
        else:
            print(f"Invalid data type specified: {data_type_short}")

    def _add_error(self, data_type_key: str, error_details: Dict[str, Any]):
        """Helper to add an error to the report."""
        if data_type_key not in self.report:
            self.report[data_type_key] = {"errors": []}
        # Ensure 'row' is adjusted for 1-based indexing + header
        if "row" in error_details and isinstance(
            error_details["row"], (int, np.integer)
        ):
            error_details["row"] += 2
        self.report[data_type_key]["errors"].append(error_details)

    def _check_columns(
        self,
        data_type_key: str,
        required_columns: List[str],
        optional_columns: List[str] = [],
    ) -> bool:
        """Checks for missing columns and adds errors.
        Handles optional columns by only checking if they exist, but not erroring if missing.
        """
        if self.data is None:
            return False  # Should not happen if load_and_set_current_data works
        missing_cols = [col for col in required_columns if col not in self.data.columns]
        if missing_cols:
            for col in missing_cols:
                self._add_error(
                    data_type_key, {"description": f"Missing required column: {col}"}
                )
            print(
                f"Errors found for {data_type_key}: Missing required columns - {missing_cols}"
            )
            return False
        # Check existence of optional columns, but don't add errors if missing
        for col in optional_columns:
            if col not in self.data.columns:
                print(
                    f"Optional column '{col}' not found in {data_type_key}. Skipping checks that use it."
                )
                # Optionally store info that this column is missing and skip further checks using it

        return True

    # --- Individual Check Methods (Optimized) ---

    def _check_general_data(self):
        """Checks data from general invoice data using vectorized operations."""
        data_type_key = "gst_gen_data"
        data_mapping = self.config.get("data_mapping", {})
        # Load specific column mapping if available, fallback to generic mapping
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )

        gstin_col = file_specific_mapping.get(
            "gstin", data_mapping.get("gstin", "GSTIN")
        )
        inv_num_col = file_specific_mapping.get(
            "invoice_number", data_mapping.get("invoice_number", "Invoice Number")
        )
        inv_date_col = file_specific_mapping.get(
            "invoice_date", data_mapping.get("invoice_date", "Invoice Date")
        )
        tot_amt_col = file_specific_mapping.get(
            "total_amount", data_mapping.get("total_amount", "Total Amount")
        )
        tax_amt_col = file_specific_mapping.get(
            "tax_amount", data_mapping.get("tax_amount", "Tax Amount")
        )

        required_columns = [
            gstin_col,
            inv_num_col,
            inv_date_col,
            tot_amt_col,
        ]
        optional_columns = [
            data_mapping.get("supplier_name", "Supplier Name"),
            data_mapping.get("recipient_name", "Recipient Name"),
            tax_amt_col,
            data_mapping.get("item_description", "Item Description"),
        ]  # Example of optional columns

        if not self._check_columns(data_type_key, required_columns, optional_columns):
            return  # Stop if essential columns are missing

        df = self.data.copy()  # Work on a copy

        # --- Vectorized Validations ---

        # 2. GSTIN Validation
        if gstin_col in df.columns:
            invalid_gstin_mask = ~df[gstin_col].apply(self._validate_gstin)
            for index in df.loc[invalid_gstin_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": gstin_col,
                        "value": df.loc[index, gstin_col],
                        "description": "Invalid GSTIN format",
                    },
                )
        else:
            print(f"Skipping GSTIN validation, required column {gstin_col} is missing")

        # 3. Invoice Date Validation
        if inv_date_col in df.columns:
            date_format = self.config.get("date_format", "%Y-%m-%d")
            df["parsed_date"] = pd.to_datetime(
                df[inv_date_col], format=date_format, errors="coerce"
            ).dt.date
            invalid_date_mask = df["parsed_date"].isna()
            future_date_mask = (~invalid_date_mask) & (
                df["parsed_date"] > datetime.now().date()
            )

            for index in df.loc[invalid_date_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": inv_date_col,
                        "value": df.loc[index, inv_date_col],
                        "description": f"Invalid date format (expected {date_format})",
                    },
                )
            for index in df.loc[future_date_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": inv_date_col,
                        "value": df.loc[index, inv_date_col],
                        "description": "Future invoice date",
                    },
                )
        else:
            print(
                f"Skipping invoice date validation, required column {inv_date_col} is missing"
            )

        # 4. Amount Validation (Total & Tax)
        if tot_amt_col in df.columns:
            df[tot_amt_col] = pd.to_numeric(df[tot_amt_col], errors="coerce")
            invalid_numeric_mask = df[tot_amt_col].isna()
            negative_total_mask = (~invalid_numeric_mask) & (df[tot_amt_col] < 0)

            for index in df.loc[invalid_numeric_mask].index:
                # Distinguish which column caused the NaN if possible, or report both
                val_tot = df.loc[index, tot_amt_col]
                col_str = f"{tot_amt_col}"
                val_str = f"Total: {val_tot}"
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": col_str,
                        "value": val_str,
                        "description": "Invalid numeric format",
                    },
                )

            for index in df.loc[negative_total_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": tot_amt_col,
                        "value": df.loc[index, tot_amt_col],
                        "description": "Negative total amount",
                    },
                )
        else:
            print(
                f"Skipping total amount validation, required column {tot_amt_col} is missing"
            )

        if tax_amt_col in df.columns and tot_amt_col in df.columns:
            df[tax_amt_col] = pd.to_numeric(df[tax_amt_col], errors="coerce")
            invalid_numeric_mask_tax = df[tax_amt_col].isna()
            # Tax calculation consistency
            gst_rate_config = self.config.get("gst_rate")
            df["expected_tax"] = np.nan  # Initialize column

            if isinstance(gst_rate_config, str) and gst_rate_config in df.columns:
                df[gst_rate_config] = pd.to_numeric(
                    df[gst_rate_config], errors="coerce"
                )
                df["expected_tax"] = (
                    df[tot_amt_col] * df[gst_rate_config] / 100
                ).round(2)
            elif isinstance(gst_rate_config, (int, float)):
                df["expected_tax"] = (
                    df[tot_amt_col] * float(gst_rate_config) / 100
                ).round(2)
            else:
                print(
                    f"Warning for {data_type_key}: 'gst_rate' in config is missing or invalid. Cannot check tax consistency."
                )

            # Check only where amounts and expected tax are valid numbers
            valid_for_tax_check = (
                (~invalid_numeric_mask)
                & (~invalid_numeric_mask_tax)
                & df["expected_tax"].notna()
            )
            tax_inconsistency_mask = valid_for_tax_check & (
                np.abs(df[tax_amt_col] - df["expected_tax"]) > 0.01
            )

            for index in df.loc[tax_inconsistency_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": tax_amt_col,
                        "value": df.loc[index, tax_amt_col],
                        "description": f"Tax amount inconsistent with total amount (expected ~{df.loc[index, 'expected_tax']})",
                    },
                )
        else:
            print(f"Skipping tax amount validation due to missing columns")

        # 5. Invoice Number Validation
        if inv_num_col in df.columns:
            pattern = (
                r"^[A-Za-z0-9/-]+$"  # Slightly more permissive pattern allowing '/'
            )
            invalid_inv_num_mask = (
                ~df[inv_num_col].astype(str).str.match(pattern, na=False)
            )
            for index in df.loc[invalid_inv_num_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": inv_num_col,
                        "value": df.loc[index, inv_num_col],
                        "description": "Invalid invoice number format (only A-Z, a-z, 0-9, -, / allowed)",
                    },
                )
        else:
            print(
                f"Skipping invoice number validation, required column {inv_num_col} is missing"
            )

        # --- Final Report ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Validation Errors found for {data_type_key}.")  # Summary msg
        else:
            print(f"No validation errors found for {data_type_key}.")

    def _check_gstr2b_data(self):
        """Checks data from GSTR-2B data using vectorized operations."""
        data_type_key = "gstr2b_data"
        data_mapping = self.config.get("data_mapping", {})
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )

        gstin_col = file_specific_mapping.get(
            "gstin", data_mapping.get("gstin", "GSTIN")
        )
        inv_date_col = file_specific_mapping.get(
            "invoice_date", data_mapping.get("invoice_date", "Invoice Date")
        )
        inv_num_col = file_specific_mapping.get(
            "invoice_number", data_mapping.get("invoice_number", "Invoice Number")
        )
        igst_col, cgst_col, sgst_col = "IGST Amount", "CGST Amount", "SGST Amount"
        taxable_val_col = "Total Taxable Value"
        itc_avail_col = "ITC Available"

        required_columns = [
            gstin_col,
            inv_num_col,
            inv_date_col,
            igst_col,
            cgst_col,
            sgst_col,
            taxable_val_col,
            itc_avail_col,
        ]

        optional_columns = [
            data_mapping.get("supplier_name", "Supplier Name"),
        ]

        if not self._check_columns(data_type_key, required_columns, optional_columns):
            return

        df = self.data.copy()

        # 2. GSTIN Validation
        if gstin_col in df.columns:
            invalid_gstin_mask = ~df[gstin_col].apply(self._validate_gstin)
            for index in df.loc[invalid_gstin_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": gstin_col,
                        "value": df.loc[index, gstin_col],
                        "description": "Invalid GSTIN format",
                    },
                )
        else:
            print(f"Skipping GSTIN validation due to missing column: {gstin_col}")

        # 3. Invoice Date Validation
        if inv_date_col in df.columns:
            date_format = self.config.get("date_format", "%Y-%m-%d")
            df["parsed_date"] = pd.to_datetime(
                df[inv_date_col], format=date_format, errors="coerce"
            ).dt.date
            invalid_date_mask = df["parsed_date"].isna()
            for index in df.loc[invalid_date_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": inv_date_col,
                        "value": df.loc[index, inv_date_col],
                        "description": f"Invalid date format (expected {date_format})",
                    },
                )
        else:
            print(
                f"Skipping invoice date validation due to missing column: {inv_date_col}"
            )

        # 4. Amount Validations
        amount_cols = [igst_col, cgst_col, sgst_col, taxable_val_col]
        if all(col in df.columns for col in amount_cols):
            df[amount_cols] = df[amount_cols].apply(pd.to_numeric, errors="coerce")
            invalid_numeric_mask = df[amount_cols].isna().any(axis=1)
            negative_amount_mask = (~invalid_numeric_mask) & (df[amount_cols] < 0).any(
                axis=1
            )

            for index in df.loc[invalid_numeric_mask].index:
                vals = df.loc[index, amount_cols].to_dict()
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": ", ".join(amount_cols),
                        "values": str(vals),
                        "description": "Non-numeric data found in amount columns",
                    },
                )

            for index in df.loc[negative_amount_mask].index:
                vals = df.loc[index, amount_cols].to_dict()
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": ", ".join(amount_cols),
                        "values": str(vals),
                        "description": "Negative amount found",
                    },
                )
        else:
            print(f"Skipping amount validation due to one or more missing columns")

        # 5. ITC Availability Check
        if itc_avail_col in df.columns:
            # Convert ITC Available to boolean, treating common variations. Fill NA with False.
            df[itc_avail_col] = (
                df[itc_avail_col]
                .astype(str)
                .str.lower()
                .map(
                    {
                        "yes": True,
                        "true": True,
                        "1": True,
                        "no": False,
                        "false": False,
                        "0": False,
                    }
                )
                .fillna(False)
            )

            itc_zero_tax_mask = (
                (~invalid_numeric_mask)
                & df[itc_avail_col]
                & (df[igst_col] == 0)
                & (df[cgst_col] == 0)
                & (df[sgst_col] == 0)
            )

            for index in df.loc[itc_zero_tax_mask].index:
                vals = {
                    itc_avail_col: df.loc[index, itc_avail_col],
                    igst_col: df.loc[index, igst_col],
                    cgst_col: df.loc[index, cgst_col],
                    sgst_col: df.loc[index, sgst_col],
                }
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{itc_avail_col}, {igst_col}, {cgst_col}, {sgst_col}",
                        "values": str(vals),
                        "description": "ITC available but all tax amounts are zero",
                    },
                )
        else:
            print(f"Skipping ITC check due to missing column: {itc_avail_col}")

        # --- Final Report ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Validation Errors found for {data_type_key}.")
        else:
            print(f"No validation errors found for {data_type_key}.")

    def _check_annexureb_data(self):
        """Checks data from Annexure B (Exports) data using vectorized operations."""
        data_type_key = "annexureb_data"

        data_mapping = self.config.get("data_mapping", {})
        # Load specific column mapping if available, fallback to generic mapping
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )

        gstin_col = file_specific_mapping.get(
            "gstin", data_mapping.get("gstin", "GSTIN")
        )
        export_date_col = "Export Date"
        ship_bill_date_col = "Shipping Bill Date"
        port_code_col = "Port Code"
        export_val_col = "Export Value"
        tax_paid_col = "Tax Paid"

        required_columns = [
            gstin_col,
            "Export Invoice Number",
            export_date_col,
            port_code_col,
            "Shipping Bill Number",
            ship_bill_date_col,
            export_val_col,
            tax_paid_col,
            "Country of Destination",
        ]

        if not self._check_columns(data_type_key, required_columns):
            return

        df = self.data.copy()

        # 2. GSTIN Validation
        if gstin_col in df.columns:
            invalid_gstin_mask = ~df[gstin_col].apply(self._validate_gstin)
            for index in df.loc[invalid_gstin_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": gstin_col,
                        "value": df.loc[index, gstin_col],
                        "description": "Invalid GSTIN format",
                    },
                )
        else:
            print(f"Skipping GSTIN validation, required column {gstin_col} is missing")

        # 3. Date Validations
        if export_date_col in df.columns and ship_bill_date_col in df.columns:
            date_format = self.config.get("date_format", "%Y-%m-%d")
            df["Export Date Parsed"] = pd.to_datetime(
                df[export_date_col], format=date_format, errors="coerce"
            ).dt.date
            df["Shipping Bill Date Parsed"] = pd.to_datetime(
                df[ship_bill_date_col], format=date_format, errors="coerce"
            ).dt.date

            invalid_dates_mask = (
                df["Export Date Parsed"].isna() | df["Shipping Bill Date Parsed"].isna()
            )
            date_mismatch_mask = (~invalid_dates_mask) & (
                df["Shipping Bill Date Parsed"] < df["Export Date Parsed"]
            )

            for index in df.loc[invalid_dates_mask].index:
                vals = f"{df.loc[index, export_date_col]}, {df.loc[index, ship_bill_date_col]}"
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{export_date_col}, {ship_bill_date_col}",
                        "values": vals,
                        "description": f"Invalid date format (expected {date_format})",
                    },
                )

            for index in df.loc[date_mismatch_mask].index:
                vals = f"{df.loc[index, export_date_col]}, {df.loc[index, ship_bill_date_col]}"
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{ship_bill_date_col}, {export_date_col}",
                        "values": vals,
                        "description": "Shipping Bill Date cannot be before Export Date",
                    },
                )
        else:
            print(
                f"Skipping date validation, required columns {export_date_col} and {ship_bill_date_col} are missing"
            )

        # 4. Port Code Validation
        if port_code_col in df.columns:
            valid_port_codes = set(
                self.config.get("valid_port_codes", ["INBOM", "INDEL", "INMAA"])
            )  # Use set for faster lookup
            invalid_port_code_mask = (
                ~df[port_code_col].astype(str).isin(valid_port_codes)
            )
            for index in df.loc[invalid_port_code_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": port_code_col,
                        "value": df.loc[index, port_code_col],
                        "description": "Invalid Port Code",
                    },
                )
        else:
            print(
                f"Skipping port code validation, required column {port_code_col} is missing"
            )

        # 5. Export Value and Tax Paid Validation
        if export_val_col in df.columns and tax_paid_col in df.columns:
            df[export_val_col] = pd.to_numeric(df[export_val_col], errors="coerce")
            df[tax_paid_col] = pd.to_numeric(df[tax_paid_col], errors="coerce")

            invalid_numeric_mask = df[export_val_col].isna() | df[tax_paid_col].isna()
            negative_export_mask = (~invalid_numeric_mask) & (df[export_val_col] < 0)
            tax_paid_non_zero_mask = (~invalid_numeric_mask) & (df[tax_paid_col] != 0)

            for index in df.loc[invalid_numeric_mask].index:
                vals = f"{df.loc[index, export_val_col]}, {df.loc[index, tax_paid_col]}"
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{export_val_col}, {tax_paid_col}",
                        "values": vals,
                        "description": "Invalid numeric format",
                    },
                )

            for index in df.loc[negative_export_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": export_val_col,
                        "value": df.loc[index, export_val_col],
                        "description": "Negative Export Value",
                    },
                )

            for index in df.loc[tax_paid_non_zero_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": tax_paid_col,
                        "value": df.loc[index, tax_paid_col],
                        "description": "Tax should typically be zero for exports",
                    },
                )
        else:
            print(
                f"Skipping export/tax validation, required columns {export_val_col} and {tax_paid_col} are missing"
            )

        # --- Final Report ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Validation Errors found for {data_type_key}.")
        else:
            print(f"No validation errors found for {data_type_key}.")

    def _check_gstr3b_data(self):
        """Checks data from GSTR-3B data using vectorized operations."""
        data_type_key = "gstr3b_data"

        data_mapping = self.config.get("data_mapping", {})
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )

        gstin_col = file_specific_mapping.get(
            "gstin", data_mapping.get("gstin", "GSTIN")
        )
        tax_period_col = file_specific_mapping.get("tax_period", "Tax Period")
        taxable_val_col = file_specific_mapping.get(
            "taxable_value", "Total Taxable Value"
        )
        igst_paid_col = file_specific_mapping.get("igst_paid", "IGST Paid")
        cgst_paid_col = file_specific_mapping.get("cgst_paid", "CGST Paid")
        sgst_paid_col = file_specific_mapping.get("sgst_paid", "SGST Paid")
        itc_claimed_col = file_specific_mapping.get("itc_claimed", "ITC Claimed")

        required_columns = [
            gstin_col,
            tax_period_col,
        ]

        optional_columns = [
            taxable_val_col,
            igst_paid_col,
            cgst_paid_col,
            sgst_paid_col,
            itc_claimed_col,
        ]

        if not self._check_columns(data_type_key, required_columns, optional_columns):
            return

        df = self.data.copy()

        # 2. GSTIN Validation
        if gstin_col in df.columns:
            invalid_gstin_mask = ~df[gstin_col].apply(self._validate_gstin)
            for index in df.loc[invalid_gstin_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": gstin_col,
                        "value": df.loc[index, gstin_col],
                        "description": "Invalid GSTIN format",
                    },
                )
        else:
            print(f"Skipping GSTIN validation, required column {gstin_col} is missing")

        # 3. Tax Period Validation (YYYY-MM format)
        if tax_period_col in df.columns:
            tax_period_pattern = r"^\d{4}-(0[1-9]|1[0-2])$"
            invalid_tax_period_mask = (
                ~df[tax_period_col].astype(str).str.match(tax_period_pattern, na=False)
            )
            for index in df.loc[invalid_tax_period_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": tax_period_col,
                        "value": df.loc[index, tax_period_col],
                        "description": "Invalid Tax Period format (expected YYYY-MM)",
                    },
                )
        else:
            print(
                f"Skipping Tax period validation, required column {tax_period_col} is missing"
            )

        # 4. Amount Validations and ITC Claim Check
        amount_cols = [
            taxable_val_col,
            igst_paid_col,
            cgst_paid_col,
            sgst_paid_col,
            itc_claimed_col,
        ]

        present_amount_cols = [col for col in amount_cols if col in df.columns]

        if present_amount_cols:  # Proceed only if any amount column is present
            df[present_amount_cols] = df[present_amount_cols].apply(
                pd.to_numeric, errors="coerce"
            )

            invalid_numeric_mask = df[present_amount_cols].isna().any(axis=1)
            negative_amount_mask = (~invalid_numeric_mask) & (
                df[present_amount_cols] < 0
            ).any(axis=1)

            for index in df.loc[invalid_numeric_mask].index:
                vals = df.loc[index, present_amount_cols].to_dict()
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": ", ".join(present_amount_cols),
                        "values": str(vals),
                        "description": "Non-numeric data found",
                    },
                )

            for index in df.loc[negative_amount_mask].index:
                vals = df.loc[index, present_amount_cols].to_dict()
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": ", ".join(present_amount_cols),
                        "values": str(vals),
                        "description": "Negative amount found",
                    },
                )

            # Anomaly Detection: ITC claimed > 120% of (IGST + CGST + SGST paid)
            total_taxes_paid_col = "total_taxes_paid"  # Added assignment
            if (
                igst_paid_col in df.columns
                and cgst_paid_col in df.columns
                and sgst_paid_col in df.columns
            ):
                df[total_taxes_paid_col] = df[
                    [igst_paid_col, cgst_paid_col, sgst_paid_col]
                ].sum(
                    axis=1,
                    skipna=True,  # Use skipna = True to avoid any issues for NA cells in different columns
                )

                anomaly_mask = (~invalid_numeric_mask) & (
                    df[itc_claimed_col] > (df[total_taxes_paid_col] * 1.2)
                )

                for index in df.loc[anomaly_mask].index:
                    vals = {
                        itc_claimed_col: df.loc[index, itc_claimed_col],
                        total_taxes_paid_col: df.loc[index, total_taxes_paid_col],
                    }
                    cols = f"{itc_claimed_col}, {igst_paid_col}, {cgst_paid_col}, {sgst_paid_col}"
                    self._add_error(
                        data_type_key,
                        {
                            "row": index,
                            "columns": cols,
                            "values": str(vals),
                            "description": "ITC claimed significantly higher than total taxes paid",
                        },
                    )

        else:
            print("No amount columns found, so amount related Validations Skipped")

        # --- Final Report ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Validation Errors found for {data_type_key}.")
        else:
            print(f"No validation errors found for {data_type_key}.")

    def _check_rfd01_data(self):
        """Checks data from RFD-01 (Refund Application) data using vectorized operations."""
        data_type_key = "rfd01_data"

        data_mapping = self.config.get("data_mapping", {})
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )

        gstin_col = file_specific_mapping.get(
            "gstin", data_mapping.get("gstin", "GSTIN")
        )
        period_from_col = file_specific_mapping.get(
            "refund_period_from", "Refund Period From"
        )
        period_to_col = file_specific_mapping.get(
            "refund_period_to", "Refund Period To"
        )
        reason_col = file_specific_mapping.get("reason_for_refund", "Reason for Refund")
        amount_col = file_specific_mapping.get(
            "refund_amount_claimed", "Refund Amount Claimed"
        )

        bank_acc_col = file_specific_mapping.get("bank_account", "Bank Account Number")
        ifsc_col = file_specific_mapping.get("bank_ifsc", "Bank IFSC Code")

        required_columns = [
            gstin_col,
            period_from_col,
            period_to_col,
        ]

        optional_columns = [
            reason_col,
            amount_col,
            bank_acc_col,
            ifsc_col,
        ]

        if not self._check_columns(data_type_key, required_columns, optional_columns):
            return

        df = self.data.copy()

        # 2. GSTIN Validation
        if gstin_col in df.columns:
            invalid_gstin_mask = ~df[gstin_col].apply(self._validate_gstin)
            for index in df.loc[invalid_gstin_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": gstin_col,
                        "value": df.loc[index, gstin_col],
                        "description": "Invalid GSTIN format",
                    },
                )
        else:
            print(f"Skipping GSTIN validation, required column {gstin_col} is missing")

        # 3. Refund Period Validation
        if period_from_col in df.columns and period_to_col in df.columns:
            date_format = self.config.get("date_format", "%Y-%m-%d")
            df["Period From Parsed"] = pd.to_datetime(
                df[period_from_col], format=date_format, errors="coerce"
            ).dt.date
            df["Period To Parsed"] = pd.to_datetime(
                df[period_to_col], format=date_format, errors="coerce"
            ).dt.date

            invalid_dates_mask = (
                df["Period From Parsed"].isna() | df["Period To Parsed"].isna()
            )
            period_mismatch_mask = (~invalid_dates_mask) & (
                df["Period To Parsed"] < df["Period From Parsed"]
            )
            period_too_long_mask = (~invalid_dates_mask) & (
                (df["Period To Parsed"] - df["Period From Parsed"])
                > timedelta(days=730)
            )  # 2 years

            for index in df.loc[invalid_dates_mask].index:
                vals = (
                    f"{df.loc[index, period_from_col]}, {df.loc[index, period_to_col]}"
                )
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{period_from_col}, {period_to_col}",
                        "values": vals,
                        "description": f"Invalid date format (expected {date_format})",
                    },
                )

            for index in df.loc[period_mismatch_mask].index:
                vals = (
                    f"{df.loc[index, period_from_col]}, {df.loc[index, period_to_col]}"
                )
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{period_from_col}, {period_to_col}",
                        "values": vals,
                        "description": "Refund Period To cannot be before Refund Period From",
                    },
                )

            for index in df.loc[period_too_long_mask].index:
                vals = (
                    f"{df.loc[index, period_from_col]}, {df.loc[index, period_to_col]}"
                )
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "columns": f"{period_from_col}, {period_to_col}",
                        "values": vals,
                        "description": "Refund Period cannot be more than 2 years",
                    },
                )
        else:
            print(
                f"Skipping date validations refund period since Date columns were not detected"
            )
        # 4. Refund Amount Validation
        if amount_col in df.columns:
            df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
            invalid_numeric_mask = df[amount_col].isna()
            negative_amount_mask = (~invalid_numeric_mask) & (df[amount_col] < 0)

            for index in df.loc[invalid_numeric_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": amount_col,
                        "value": df.loc[index, amount_col],
                        "description": "Invalid numeric format",
                    },
                )
            for index in df.loc[negative_amount_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": amount_col,
                        "value": df.loc[index, amount_col],
                        "description": "Negative refund amount",
                    },
                )
        else:
            print(
                f"Skipping Refund amount validation as  column Refund Amount not detected "
            )
        # 5. Reason for Refund Validation
        if reason_col in df.columns:
            valid_reasons = set(
                self.config.get(
                    "valid_refund_reasons",
                    [
                        "Excess cash balance in electronic cash ledger",
                        "Export of goods/services (with payment of tax)",  # Example reason
                        "Export of goods/services (without payment of tax)",  # Example reason
                        "Inverted tax structure",  # Example reason
                        "Refund by recipient of deemed export",  # Example reason
                        # Add more valid reasons as needed from config or defaults
                    ],
                )
            )

            invalid_reason_mask = ~df[reason_col].astype(str).isin(valid_reasons)
            for index in df.loc[invalid_reason_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": reason_col,
                        "value": df.loc[index, reason_col],
                        "description": "Not a valid or configured reason for refund",
                    },
                )
        else:
            print(f"skipping Reason Validation Column not found")
        # --- Final Report ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Validation Errors found for {data_type_key}.")
        else:
            print(f"No validation errors found for {data_type_key}.")

    def _check_ewaybill_data(self):
        """
        Validates E-way bill data using vectorized operations. Handles missing
        columns gracefully and logs errors with context.
        """
        data_type_key = "ewaybill_data"
        file_specific_mapping = self.config.get("file_column_mapping", {}).get(
            data_type_key, {}
        )
        date_format = self.config.get("date_format", "%Y-%m-%d")

        # Column mappings with default values
        column_mapping = {
            "eway_bill_number": file_specific_mapping.get(
                "eway_bill_number", "E-way Bill Number"
            ),
            "generated_date": file_specific_mapping.get(
                "generated_date", "Generated Date"
            ),
            "valid_until": file_specific_mapping.get("valid_until", "Valid Until"),
            "supplier_gstin": file_specific_mapping.get(
                "supplier_gstin", "Supplier GSTIN"
            ),
            "recipient_gstin": file_specific_mapping.get(
                "recipient_gstin", "Recipient GSTIN"
            ),
            "invoice_number": file_specific_mapping.get(
                "invoice_number", "Invoice Number"
            ),
            "invoice_date": file_specific_mapping.get("invoice_date", "Invoice Date"),
            "total_value": file_specific_mapping.get("total_value", "Total Value"),
            "transport_mode": file_specific_mapping.get(
                "transport_mode", "Transport Mode"
            ),
            "distance": file_specific_mapping.get("distance", "Distance (km)"),
        }

        required_columns = [column_mapping["eway_bill_number"]]
        optional_columns = [
            column_mapping["generated_date"],
            column_mapping["valid_until"],
            column_mapping["supplier_gstin"],
            column_mapping["recipient_gstin"],
            column_mapping["invoice_number"],
            column_mapping["invoice_date"],
            column_mapping["total_value"],
            column_mapping["transport_mode"],
            column_mapping["distance"],
        ]
        # Check for missing columns
        if not self._check_columns(data_type_key, required_columns, optional_columns):
            return

        df = self.data.copy()  # Create a copy to avoid modifying original data

        # Helper function to validate GSTIN and report errors
        def _validate_and_report_gstin(df, column_name, gstin_type):
            if column_name not in df.columns:
                logging.info(
                    f"Skipping {gstin_type} GSTIN validation: Column '{column_name}' not found."
                )
                return

            invalid_mask = ~df[column_name].apply(self._validate_gstin)
            for index in df.loc[invalid_mask].index:
                gstin_value = df.loc[index, column_name]
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": column_name,
                        "value": gstin_value,
                        "description": f"Invalid {gstin_type} GSTIN format: {gstin_value}",
                    },
                )

        # 2. GSTIN Validation
        _validate_and_report_gstin(df, column_mapping["supplier_gstin"], "Supplier")
        _validate_and_report_gstin(df, column_mapping["recipient_gstin"], "Recipient")

        # 3. Date Validations
        gen_date_col = column_mapping["generated_date"]
        valid_until_col = column_mapping["valid_until"]
        inv_date_col = column_mapping["invoice_date"]
        if all(
            col in df.columns for col in [gen_date_col, valid_until_col, inv_date_col]
        ):
            try:
                df["Generated Date Parsed"] = pd.to_datetime(
                    df[gen_date_col], format=date_format, errors="coerce"
                ).dt.date
                df["Valid Until Parsed"] = pd.to_datetime(
                    df[valid_until_col], format=date_format, errors="coerce"
                ).dt.date
                df["Invoice Date Parsed"] = pd.to_datetime(
                    df[inv_date_col], format=date_format, errors="coerce"
                ).dt.date

                date_checks = [
                    (
                        "Valid Until date cannot be before Generated Date",
                        df["Valid Until Parsed"] < df["Generated Date Parsed"],
                        [gen_date_col, valid_until_col],
                    ),
                    (
                        "Invoice Date cannot be after Generated Date",
                        df["Invoice Date Parsed"] > df["Generated Date Parsed"],
                        [gen_date_col, inv_date_col],
                    ),
                ]

                invalid_date_mask = (
                    df["Generated Date Parsed"].isna()
                    | df["Valid Until Parsed"].isna()
                    | df["Invoice Date Parsed"].isna()
                )
                if invalid_date_mask.any():
                    for index in df.loc[invalid_date_mask].index:
                        date_values = f"{df.loc[index, gen_date_col]}, {df.loc[index, valid_until_col]}, {df.loc[index, inv_date_col]}"
                        self._add_error(
                            data_type_key,
                            {
                                "row": index,
                                "column": f"{gen_date_col}, {valid_until_col}, {inv_date_col}",
                                "value": date_values,
                                "description": f"Invalid date format (expected {date_format})",
                            },
                        )
                for error_message, mask, columns in date_checks:
                    if mask.any():
                        for index in df.loc[mask].index:
                            date_values = ", ".join(
                                f"{df.loc[index, col]}" for col in columns
                            )
                            self._add_error(
                                data_type_key,
                                {
                                    "row": index,
                                    "column": ", ".join(columns),
                                    "value": date_values,
                                    "description": error_message,
                                },
                            )
            except ValueError as e:
                logging.error(f"Date parsing error: {e}.  Skipping date validations.")
            except Exception as e:
                logging.exception(
                    f"An unexpected error occurred: {e}. Skipping date validations."
                )
        else:
            logging.info(
                "Skipping date validations:  Missing one or more date columns."
            )

        # 4. Total Value Validation
        total_val_col = column_mapping["total_value"]
        if total_val_col in df.columns:
            df[total_val_col] = pd.to_numeric(df[total_val_col], errors="coerce")
            invalid_numeric_mask_val = df[total_val_col].isna()
            negative_value_mask = (~invalid_numeric_mask_val) & (df[total_val_col] < 0)

            for index in df.loc[invalid_numeric_mask_val].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": total_val_col,
                        "value": df.loc[index, total_val_col],
                        "description": "Invalid numeric format",
                    },
                )
            for index in df.loc[negative_value_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": total_val_col,
                        "value": df.loc[index, total_val_col],
                        "description": "Negative total value",
                    },
                )
        else:
            logging.info(
                f"Skipping total value validation: Column '{total_val_col}' not found."
            )

        # 5. Transport Mode Validation
        transport_mode_col = column_mapping["transport_mode"]
        if transport_mode_col in df.columns:
            valid_transport_modes = set(
                self.config.get(
                    "valid_transport_modes", ["Road", "Rail", "Air", "Ship", "Vehicle"]
                )
            )  # Use set, add Vehicle
            invalid_transport_mask = (
                ~df[transport_mode_col].astype(str).isin(valid_transport_modes)
            )
            for index in df.loc[invalid_transport_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": transport_mode_col,
                        "value": df.loc[index, transport_mode_col],
                        "description": "Not a valid or configured transport mode",
                    },
                )
        else:
            logging.info(
                f"Skipping transport mode validation: Column '{transport_mode_col}' not found."
            )

        # 6. Distance (KM) validation
        distance_col = column_mapping["distance"]
        if distance_col in df.columns:
            df[distance_col] = pd.to_numeric(df[distance_col], errors="coerce")
            invalid_numeric_mask_dist = df[distance_col].isna()
            negative_distance_mask = (~invalid_numeric_mask_dist) & (
                df[distance_col] < 0
            )

            for index in df.loc[invalid_numeric_mask_dist].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": distance_col,
                        "value": df.loc[index, distance_col],
                        "description": "Invalid numeric format",
                    },
                )
            for index in df.loc[negative_distance_mask].index:
                self._add_error(
                    data_type_key,
                    {
                        "row": index,
                        "column": distance_col,
                        "value": df.loc[index, distance_col],
                        "description": "Distance cannot be negative",
                    },
                )
        else:
            logging.info(
                f"Skipping distance validation: Column '{distance_col}' not found."
            )

    def check_cross_document_consistency(self):
        """Checks for consistency across different GST documents using pandas merge."""
        data_type_key = "cross_document_consistency"
        self.report[data_type_key] = {"errors": []}  # Initialize errors for this check
        print("\n--- Checking Cross-Document Consistency ---")

        if not self.all_data:
            print("Error: No data loaded. Load all data first.")
            return

        data_mapping = self.config.get("data_mapping", {})
        inv_num_col = data_mapping.get("invoice_number", "Invoice Number")
        tot_amt_col = data_mapping.get("total_amount", "Total Amount")
        itc_available_col = data_mapping.get("itc_available", "ITC Available")
        export_inv_num_col = data_mapping.get(
            "export_invoice_number", "Export Invoice Number"
        )

        # --- Check 1: GSTR-2B vs Annexure B (Exports should not claim ITC) ---
        if "gstr2b_data" in self.all_data and "annexureb_data" in self.all_data:
            print("Checking GSTR-2B vs Annexure B...")
            gstr2b_df = self.all_data["gstr2b_data"].copy()
            annexb_df = self.all_data["annexureb_data"].copy()

            discrepancies_df = cross_check_gstr2b_annexureb(
                gstr2b_df,
                annexb_df,
                inv_num_col=inv_num_col,
                itc_available_col=itc_available_col,
                export_inv_num_col=export_inv_num_col,
            )

            for index, row in discrepancies_df.iterrows():
                self._add_error(
                    data_type_key,
                    {
                        "documents": "gstr2b_data, annexureb_data",
                        "values": f"Export Invoice: {row['Export Invoice Number']}, ITC Available in GSTR-2B: {row['ITC Available']}",
                        "description": "Export Invoice found in GSTR-2B with ITC Available claimed.",
                        "row_annexb": index + 2,  # Original Annex B row index
                    },
                )

        else:
            print("Skipping GSTR-2B vs Annexure B check: Data not available.")

        # --- Check 2: GSTR-3B vs General Data (Taxable Value Consistency) ---
        if "gstr3b_data" in self.all_data and "gst_gen_data" in self.all_data:
            print("Checking GSTR-3B vs General Data (Total Taxable Value)...")
            gstr3b_df = self.all_data["gstr3b_data"].copy()
            gstgen_df = self.all_data["gst_gen_data"].copy()

            comparison_df = cross_check_gstr3b_generaldata(
                gstr3b_df, gstgen_df, total_amount_col=tot_amt_col
            )

            if not comparison_df.empty:
                for index, row in comparison_df.iterrows():
                    self._add_error(
                        data_type_key,
                        {
                            "documents": "gstr3b_data, gst_gen_data",
                            "values": f"Total Taxable Value GSTR-3B (Sum): {row['total_taxable_value_3b']:.2f}, Total Amount Invoices (Sum): {row['total_taxable_value_gen']:.2f}",
                            "description": row["discrepancy"],
                        },
                    )
            else:
                print(
                    "GSTR-3B vs General Data Total Taxable Value sums appear consistent."
                )
        else:
            print("Skipping GSTR-3B vs General Data check: Data not available.")

        # --- Check 3: E-way Bill vs General Data (Total Value per Invoice) ---
        if "ewaybill_data" in self.all_data and "gst_gen_data" in self.all_data:
            print("Checking E-way Bill vs General Data (Invoice Total Value)...")
            eway_df = self.all_data["ewaybill_data"].copy()
            gstgen_df = self.all_data["gst_gen_data"].copy()

            # Map e-way invoice number column if different in config
            eway_inv_num_col = self.config.get(
                "eway_invoice_number_column", "Invoice Number"
            )
            eway_total_val_col = self.config.get(
                "eway_total_value_column", "Total Value"
            )

            discrepancies_df = cross_check_ewaybill_generaldata(
                eway_df,
                gstgen_df,
                eway_inv_num_col=eway_inv_num_col,
                eway_total_val_col=eway_total_val_col,
                inv_num_col=inv_num_col,
                total_amount_col=tot_amt_col,
            )

            for index, row in discrepancies_df.iterrows():
                invoice_num = row[f"{eway_inv_num_col}_Lower"]
                self._add_error(
                    data_type_key,
                    {
                        "documents": "ewaybill_data, gst_gen_data",
                        "values": f"E-way Bill Total: {row[eway_total_val_col]:.2f}, General Invoice Total: {row[tot_amt_col]:.2f}",
                        "invoice_number": invoice_num,
                        "description": "Total value mismatch between E-way Bill and General Invoice for the same invoice number.",
                    },
                )

        else:
            print("Skipping E-way Bill vs General Data check: Data not available.")

        # --- Final Report for Cross-Checks ---
        if self.report.get(data_type_key, {}).get("errors"):
            print(f"Cross-document consistency errors found.")
        else:
            print(
                "No cross-document consistency errors found based on checks performed."
            )

    def generate_report(self, output_file="verification_report.json"):
        """Generates a JSON report of the verification results."""
        if not self.report:
            print(
                "No verification tasks have been run or no errors found. Report is empty."
            )
            return

        report_path = output_file
        if self.results_folder:
            report_path = os.path.join(self.results_folder, output_file)
        else:
            # Fallback to party folder if results folder wasn't created/set
            if self.party_name:
                party_folder = os.path.join(os.getcwd(), self.party_name)
                # Ensure party folder exists before saving report there
                os.makedirs(party_folder, exist_ok=True)
                report_path = os.path.join(party_folder, output_file)
            # Else save in current working directory if no party context

        try:
            # Custom JSON encoder to handle numpy types if any sneak through
            class NpEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, np.integer):
                        return int(obj)
                    if isinstance(obj, np.floating):
                        return float(obj)
                    if isinstance(obj, np.ndarray):
                        return obj.tolist()
                    if pd.isna(obj):
                        return None  # Represent pandas NA as null in JSON
                    return super(NpEncoder, self).default(obj)

            report_data_clean = {}
            for key, value in self.report.items():
                # Ensure errors list exists even if empty for consistency
                report_data_clean[key] = {"errors": value.get("errors", [])}

            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(
                    report_data_clean, f, indent=4, cls=NpEncoder, ensure_ascii=False
                )
            print(f"Verification report generated: {report_path}")
        except TypeError as e:
            print(f"Error generating JSON report (potential type issue): {e}")
            print(
                "Report data snippet causing error:", str(self.report)[:500]
            )  # Log snippet
        except Exception as e:
            print(f"Error generating report at {report_path}: {e}")

    def get_party_details(self, party_folder: str) -> bool:
        """Loads config and data for a specific party folder."""
        print(f"\n--- Setting up for Party: {self.party_name} ---")
        print(f"Party Folder Path: {party_folder}")

        config_loaded = False
        # Check if party folder exists
        if os.path.isdir(party_folder):
            print(f"Party folder found: {party_folder}")

            # Check for config.json
            config_path = os.path.join(party_folder, self.config_file)
            if os.path.isfile(config_path):
                print(f"Configuration file found: {config_path}")
                self.config = self._load_config(config_path)
                config_loaded = bool(self.config)  # True if config is not empty {}
            else:
                print(
                    f"Configuration file '{self.config_file}' not found in party folder. Using default settings/prompts."
                )
                self.config = {}  # Ensure empty config

            # Attempt to create results folder *after* confirming party folder exists
            self.results_folder = self._create_results_folder(party_folder)

            # Load all available data from this party folder
            print("Loading all data files from the party folder...")
            if not self.load_all_data(party_folder):
                print(
                    "Warning: Failed to load any data files from party folder, but proceeding."
                )
                # Decide if this is critical - maybe return False?
        else:
            print(f"Party folder '{party_folder}' not found.")
            # Ask user if they want to create it? Or just stop?
            # For now, create it and the results subfolder
            print(f"Creating party folder: {party_folder}")
            try:
                os.makedirs(party_folder, exist_ok=True)
                self.results_folder = self._create_results_folder(party_folder)
            except Exception as e:
                print(f"Error creating party folder or results subfolder: {e}")
                return False  # Cannot proceed without a folder structure

            self.config = {}  # No config file if folder didn't exist
            print(
                "Party folder created. No data files loaded yet. Configuration will use defaults/prompts."
            )

        # Prompt for essential config if missing after attempting load
        if not config_loaded:
            print("Essential configuration missing, prompting user...")
            # Prompt for GST Rate OR Column if not in Config
            if "gst_rate" not in self.config:
                while True:
                    rate_choice = input(
                        "Enter the default GST rate (e.g., 18) OR the column name containing GST rates (if applicable, press Enter to skip): "
                    ).strip()
                    if not rate_choice:
                        print("Skipping GST rate configuration.")
                        break  # Allow skipping
                    try:
                        self.config["gst_rate"] = float(rate_choice)
                        print(f"Using fixed GST rate: {self.config['gst_rate']}")
                        break
                    except ValueError:
                        # Assume it's a column name - no easy way to validate without data loaded yet
                        self.config["gst_rate"] = rate_choice
                        print(f"Assuming '{rate_choice}' is the GST rate column name.")
                        break

            # Prompt for Date format
            if "date_format" not in self.config:
                default_date_format = "%Y-%m-%d"
                date_format_input = input(
                    f"Enter the date format (e.g., {default_date_format}, DD/MM/YYYY -> %d/%m/%Y) [Default: {default_date_format}]: "
                ).strip()
                self.config["date_format"] = date_format_input or default_date_format
                print(f"Using date format: {self.config['date_format']}")

            # Prompt for file prefixes if missing (crucial for load_all_data)
            if "file_prefixes" not in self.config:
                print("File identification prefixes missing. Please define them.")
                print("Example: {'gst_gen': 'Invoice', 'gstr2b': 'GSTR2B', ...}")
                prefixes = {}
                while True:
                    key = input(
                        "Enter data type key (e.g., 'gst_gen', 'gstr2b', or Enter to finish): "
                    ).strip()
                    if not key:
                        break
                    prefix = input(
                        f"Enter the starting filename prefix for '{key}' data: "
                    ).strip()
                    if prefix:
                        prefixes[key] = prefix
                    else:
                        print("Prefix cannot be empty.")
                self.config["file_prefixes"] = prefixes
                print(f"Using file prefixes: {self.config['file_prefixes']}")
                # Optionally save this minimal config back to the party folder?
                # config_path = os.path.join(party_folder, self.config_file)
                # try:
                #    with open(config_path, 'w') as f: json.dump(self.config, f, indent=4)
                #    print(f"Saved minimal config to {config_path}")
                # except Exception as e: print(f"Could not save minimal config: {e}")

        return True  # Party setup successful (folder exists/created, config attempted/prompted)

    def display_summary(self):
        """Display a summary of the report errors to the console."""
        if not self.report:
            print(
                "\nNo verification tasks have been run or no errors found. Summary is empty."
            )
            return

        print("\n--- Verification Summary ---")
        total_errors = 0
        found_errors = False

        # Map internal keys to user-friendly names
        report_key_map = {
            "gst_gen_data": "General Data",
            "gstr2b_data": "GSTR-2B Data",
            "annexureb_data": "Annexure B Data",
            "gstr3b_data": "GSTR-3B Data",
            "rfd01_data": "RFD-01 Data",
            "ewaybill_data": "E-way Bill Data",
            "cross_document_consistency": "Cross-Document Consistency",
        }

        for key, friendly_name in report_key_map.items():
            if key in self.report:
                errors_list = self.report[key].get("errors", [])
                error_count = len(errors_list)
                if error_count > 0:
                    print(f"\n{friendly_name} Verification:")
                    print(f"  Total Errors: {error_count}")
                    total_errors += error_count
                    found_errors = True
                    # Optionally show first few errors?
                    # for i, err in enumerate(errors_list[:3]):
                    #    print(f"    - Row {err.get('row', 'N/A')}: {err.get('description', 'No description')}")
                    # if error_count > 3: print("    ...")
                # else: # Optionally report sections with no errors
                # print(f"\n{friendly_name} Verification: No errors found.")

        if not found_errors:
            print("No errors found in any verified section.")
        else:
            print(f"\n--- Total Errors Across All Sections: {total_errors} ---")

        print("\nFor detailed results, check the generated report file.")

    def run(self):
        """Runs the GST verifier based on user input."""
        print("\n--- GST Data Verifier ---")

        # 1. Get Party Name and Set Up Folder/Config
        while not self.party_name:
            party_name_input = input("Enter the party name: ").strip()
            if party_name_input:
                self.party_name = party_name_input.replace(" ", "_").lower()
            else:
                print("Party name cannot be empty.")

        party_folder = os.path.join(os.getcwd(), self.party_name)

        if not self.get_party_details(party_folder):
            print("Failed to set up party details. Exiting.")
            return  # Exit if setup fails critically

        # 2. Main Menu Loop
        while True:
            print("\n--- GST Data Verifier Menu ---")
            # Dynamically generate menu options based on loaded data and checks
            menu_options = {}
            option_num = 1

            # Data specific checks (only if data exists)
            data_check_map = {
                "gst_gen": "Check General Data",
                "gstr2b": "Check GSTR-2B Data",
                "annexureb": "Check Annexure B Data",
                "gstr3b": "Check GSTR-3B Data",
                "rfd01": "Check RFD-01 Data",
                "ewaybill": "Check E-way Bill Data",
            }
            for short_key, desc in data_check_map.items():
                if short_key + "_data" in self.all_data:
                    menu_options[str(option_num)] = {
                        "action": "check",
                        "key": short_key,
                        "desc": desc,
                    }
                    print(f"{option_num}. {desc}")
                    option_num += 1
                # else: # Optionally show disabled options
                #    print(f"   ({desc} - Data not loaded)")

            # Cross-document check (only if enough data types are loaded)
            # Requires at least two relevant datasets
            relevant_data_count = sum(
                1
                for k in [
                    "gstr2b_data",
                    "annexureb_data",
                    "gstr3b_data",
                    "gst_gen_data",
                    "ewaybill_data",
                ]
                if k in self.all_data
            )
            if relevant_data_count >= 2:
                menu_options[str(option_num)] = {
                    "action": "cross_check",
                    "key": "cross_doc",
                    "desc": "Check Cross-Document Consistency",
                }
                print(f"{option_num}. Check Cross-Document Consistency")
                option_num += 1
            # else: # Optionally show disabled
            # print("   (Check Cross-Document Consistency - Insufficient data loaded)")

            # Standard options
            menu_options[str(option_num)] = {
                "action": "summary",
                "key": "summary",
                "desc": "Display Summary",
            }
            print(f"{option_num}. Display Summary")
            option_num += 1
            menu_options[str(option_num)] = {
                "action": "report",
                "key": "report",
                "desc": "Generate Report",
            }
            print(f"{option_num}. Generate Report")
            option_num += 1
            menu_options[str(option_num)] = {
                "action": "exit",
                "key": "exit",
                "desc": "Exit",
            }
            print(f"{option_num}. Exit")

            choice = input("Enter your choice: ").strip()

            if choice in menu_options:
                selected_option = menu_options[choice]
                action = selected_option["action"]
                key = selected_option["key"]

                if action == "check":
                    self.check_data(key)
                elif action == "cross_check":
                    self.check_cross_document_consistency()
                elif action == "summary":
                    self.display_summary()
                elif action == "report":
                    default_filename = f"{self.party_name}_verification_report_{datetime.now().strftime('%Y%m%d')}.json"
                    output_file = (
                        input(
                            f"Enter the output file name [Default: {default_filename}]: "
                        ).strip()
                        or default_filename
                    )
                    self.generate_report(output_file)
                elif action == "exit":
                    print("Exiting...")
                    break
            else:
                print("Invalid choice. Please try again.")


if __name__ == "__main__":
    verifier = GSTVerifier()
    verifier.run()
