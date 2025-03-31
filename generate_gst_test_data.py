# gst_data_generator.py

import csv
import json
import openpyxl
import random
import datetime
import re


def is_valid_gstin(gstin):
    """Validates a GSTIN using a regular expression."""
    pattern = r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$"
    return bool(re.match(pattern, str(gstin)))


def generate_gst_test_data(num_records=100, output_format="csv"):
    """Generates test data for GST-related document scrutiny, including various fraud scenarios."""
    data = []
    for _ in range(num_records):
        # Base valid data
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        invoice_number = f"INV-{random.randint(10000, 99999)}"
        invoice_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 365))
        ).strftime("%Y-%m-%d")
        supplier_name = f"Supplier {random.randint(1, 100)}"
        recipient_name = f"Recipient {random.randint(1, 100)}"
        total_amount = round(random.uniform(100, 10000), 2)
        tax_amount = round(total_amount * 0.18, 2)  # Assuming 18% GST
        item_description = f"Item {random.randint(1, 20)}"
        fraud_scenario = "valid"  # Default

        # Introduce fraud scenarios
        fraud_type = random.choices(
            [
                "valid",
                "invalid_gstin",
                "inflated_amount",
                "mismatched_names",
                "future_date",
                "zero_amount",
                "invalid_tax_amount",
                "invalid_invoice_number",
            ],
            weights=[0.6, 0.05, 0.07, 0.08, 0.05, 0.05, 0.05, 0.05],
        )[
            0
        ]  # Added weights to control frequency

        if fraud_type == "invalid_gstin":
            gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z{random.randint(0, 9)}"  # Guarantee Invalid
            if is_valid_gstin(gstin):  # Ensure it's invalid
                gstin = "invalid_gstin"  # if it's somehow still valid, mark it invalid

            fraud_scenario = "invalid_gstin"

        elif fraud_type == "inflated_amount":
            total_amount *= random.uniform(10, 20)  # Inflate by a random factor
            total_amount = round(total_amount, 2)
            tax_amount = round(total_amount * 0.18, 2)
            fraud_scenario = "inflated_amount"

        elif fraud_type == "mismatched_names":
            if random.random() < 0.5:
                supplier_name = f"Different Supplier {random.randint(101, 200)}"
            else:
                recipient_name = f"Different Recipient {random.randint(101, 200)}"
            fraud_scenario = "mismatched_names"

        elif fraud_type == "future_date":
            invoice_date = (
                datetime.date.today() + datetime.timedelta(days=random.randint(1, 30))
            ).strftime("%Y-%m-%d")
            fraud_scenario = "future_date"

        elif fraud_type == "zero_amount":
            total_amount = 0
            tax_amount = 0
            fraud_scenario = "zero_amount"

        elif fraud_type == "invalid_tax_amount":
            # Introduce error in tax calculation
            tax_amount = round(total_amount * 0.15, 2)  # Incorrect GST rate
            fraud_scenario = "invalid_tax_amount"

        elif fraud_type == "invalid_invoice_number":
            invoice_number = (
                f"INV-{random.choice(['!', '@', '#', '$', '%'])}"  # Invalid Characters
            )
            fraud_scenario = "invalid_invoice_number"

        data.append(
            {
                "GSTIN": gstin,
                "Invoice Number": invoice_number,
                "Invoice Date": invoice_date,
                "Supplier Name": supplier_name,
                "Recipient Name": recipient_name,
                "Total Amount": total_amount,
                "Tax Amount": tax_amount,
                "Item Description": item_description,
                "Fraud Scenario": fraud_scenario,
            }
        )

    return data, "gst_gen_test_data"


def generate_gstr2b_data(num_records=50, output_format="csv"):
    """Generates test data for GSTR-2B."""
    data = []
    for _ in range(num_records):
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        invoice_number = f"G2B-{random.randint(10000, 99999)}"
        invoice_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 365))
        ).strftime("%Y-%m-%d")
        supplier_name = f"G2B Supplier {random.randint(1, 50)}"
        igst_amount = round(random.uniform(10, 500), 2)
        cgst_amount = round(
            random.uniform(5, 250), 2
        )  # Example: Half of IGST if intra-state
        sgst_amount = round(
            random.uniform(5, 250), 2
        )  # Example: Half of IGST if intra-state
        total_taxable_value = round(random.uniform(100, 2000), 2)
        itc_available = random.choice(
            [True, False]
        )  # Simulate some invoices with blocked ITC

        # Fraud: Mismatched ITC availability with invoice amount
        mismatch_itc = random.random() < 0.1  # 10% chance of mismatched ITC
        if mismatch_itc:
            if itc_available:
                igst_amount = 0  # No IGST but ITC available - Suspicious
            else:
                igst_amount = round(
                    random.uniform(100, 500), 2
                )  # ITC blocked, but there is IGST

        data.append(
            {
                "GSTIN": gstin,
                "Invoice Number": invoice_number,
                "Invoice Date": invoice_date,
                "Supplier Name": supplier_name,
                "IGST Amount": igst_amount,
                "CGST Amount": cgst_amount,
                "SGST Amount": sgst_amount,
                "Total Taxable Value": total_taxable_value,
                "ITC Available": itc_available,
            }
        )

    return data, "gstr2b_test_data"


def generate_annexureb_data(num_records=30, output_format="csv"):
    """Generates test data for Annexure B (Exports)."""
    data = []
    for _ in range(num_records):
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        export_invoice_number = f"EXP-{random.randint(1000, 9999)}"
        export_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 365))
        ).strftime("%Y-%m-%d")
        port_code = f"IN{random.choice(['BOM', 'DEL', 'MAA'])}"
        shipping_bill_number = f"SB-{random.randint(100000, 999999)}"
        shipping_bill_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 365))
        ).strftime("%Y-%m-%d")
        export_value = round(random.uniform(500, 20000), 2)
        tax_paid = round(export_value * 0.0, 2)  # Exports are usually zero-rated
        country_of_destination = random.choice(["USA", "UK", "Germany", "Japan"])

        # Simulate shipping bill date before export invoice date
        date_mismatch = random.random() < 0.1
        if date_mismatch:
            shipping_bill_date = (
                datetime.date.today() + datetime.timedelta(days=random.randint(1, 30))
            ).strftime("%Y-%m-%d")

        data.append(
            {
                "GSTIN": gstin,
                "Export Invoice Number": export_invoice_number,
                "Export Date": export_date,
                "Port Code": port_code,
                "Shipping Bill Number": shipping_bill_number,
                "Shipping Bill Date": shipping_bill_date,
                "Export Value": export_value,
                "Tax Paid": tax_paid,
                "Country of Destination": country_of_destination,
            }
        )

    return data, "annexureb_test_data"


def generate_gstr3b_data(num_records=20, output_format="csv"):
    """Generates test data for GSTR-3B."""
    data = []
    for _ in range(num_records):
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        tax_period = (
            datetime.date.today() - datetime.timedelta(days=random.randint(30, 365))
        ).strftime(
            "%Y-%m"
        )  # YYYY-MM format
        total_taxable_value = round(random.uniform(1000, 50000), 2)
        igst_paid = round(random.uniform(100, 5000), 2)
        cgst_paid = round(random.uniform(50, 2500), 2)
        sgst_paid = round(random.uniform(50, 2500), 2)
        itc_claimed = round(random.uniform(50, 3000), 2)  # Input Tax Credit claimed

        # Simulate ITC claimed higher than tax paid
        itc_mismatch = random.random() < 0.1
        if itc_mismatch:
            itc_claimed = round(
                random.uniform(itc_claimed + 100, itc_claimed + 1000), 2
            )  # ITC claimed is substantially higher than taxes paid

        data.append(
            {
                "GSTIN": gstin,
                "Tax Period": tax_period,
                "Total Taxable Value": total_taxable_value,
                "IGST Paid": igst_paid,
                "CGST Paid": cgst_paid,
                "SGST Paid": sgst_paid,
                "ITC Claimed": itc_claimed,
            }
        )

    return data, "gstr3b_test_data"


def generate_rfd01_data(num_records=1, output_format="csv"):
    """Generates test data for GST RFD-01 form (Refund Application)."""
    data = []
    for _ in range(num_records):
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        refund_period_from = (
            datetime.date.today() - datetime.timedelta(days=random.randint(30, 365))
        ).strftime("%Y-%m-%d")
        refund_period_to = datetime.date.today().strftime("%Y-%m-%d")
        reason_for_refund = random.choice(
            [
                "Excess cash balance in electronic cash ledger",
                "Export of goods/services (with payment of tax)",
                "Inverted tax structure",
            ]
        )
        refund_amount_claimed = round(random.uniform(100, 10000), 2)
        bank_account_number = str(random.randint(1000000000, 9999999999))
        bank_ifsc_code = f"RBIS{random.randint(1000000, 9999999)}"

        # Simulate invalid refund amount (negative value)
        invalid_amount = random.random() < 0.05
        if invalid_amount:
            refund_amount_claimed = -abs(refund_amount_claimed)

        data.append(
            {
                "GSTIN": gstin,
                "Refund Period From": refund_period_from,
                "Refund Period To": refund_period_to,
                "Reason for Refund": reason_for_refund,
                "Refund Amount Claimed": refund_amount_claimed,
                "Bank Account Number": bank_account_number,
                "Bank IFSC Code": bank_ifsc_code,
            }
        )

    return data, "rfd01_test_data"


def generate_ewaybill_data(num_records=30, output_format="csv"):
    """Generates test data for E-way Bill."""
    data = []
    for _ in range(num_records):
        gstin = f"07AAAAA{random.randint(1000, 9999)}A1Z5"
        eway_bill_number = f"EWB-{random.randint(10000000, 99999999)}"
        generated_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 30))
        ).strftime("%Y-%m-%d")
        valid_until = (
            datetime.date.today() + datetime.timedelta(days=random.randint(1, 7))
        ).strftime("%Y-%m-%d")
        supplier_gstin = f"29BBBBB{random.randint(1000, 9999)}B2Z8"
        recipient_gstin = f"10CCCCC{random.randint(1000, 9999)}C3Z2"
        invoice_number = f"INV-{random.randint(10000, 99999)}"
        invoice_date = (
            datetime.date.today() - datetime.timedelta(days=random.randint(1, 30))
        ).strftime("%Y-%m-%d")
        total_value = round(random.uniform(50000, 200000), 2)
        transport_mode = random.choice(["Road", "Rail", "Air", "Ship"])
        distance = random.randint(100, 2000)

        # Simulate E-way bill expired
        expired = random.random() < 0.05
        if expired:
            valid_until = (
                datetime.date.today() - datetime.timedelta(days=random.randint(1, 7))
            ).strftime("%Y-%m-%d")

        data.append(
            {
                "GSTIN": gstin,
                "E-way Bill Number": eway_bill_number,
                "Generated Date": generated_date,
                "Valid Until": valid_until,
                "Supplier GSTIN": supplier_gstin,
                "Recipient GSTIN": recipient_gstin,
                "Invoice Number": invoice_number,
                "Invoice Date": invoice_date,
                "Total Value": total_value,
                "Transport Mode": transport_mode,
                "Distance (km)": distance,
            }
        )

    return data, "ewaybill_test_data"


def _write_data_to_file(data, output_format, output_file_name):
    """Writes data to a CSV, JSON, or Excel file."""
    if output_format == "csv":
        with open(
            f"{output_file_name}.csv", "w", newline="", encoding="utf-8"
        ) as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"CSV file '{output_file_name}.csv' generated.")

    elif output_format == "json":
        with open(f"{output_file_name}.json", "w", encoding="utf-8") as jsonfile:
            json.dump(data, jsonfile, indent=4)
        print(f"JSON file '{output_file_name}.json' generated.")

    elif output_format == "excel":
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.append(list(data[0].keys()))  # Write header

        for row in data:
            sheet.append(list(row.values()))

        workbook.save(f"{output_file_name}.xlsx")
        print(f"Excel file '{output_file_name}.xlsx' generated.")

    else:
        print("Invalid output format. Choose 'csv', 'json', or 'excel'.")


def main():
    """Generates test data for various GST-related documents based on user choice."""

    # Mapping of document names to their respective data generation functions
    document_generators = {
        "GST Test Data": generate_gst_test_data,
        "GSTR-2B Data": generate_gstr2b_data,
        "Annexure B Data": generate_annexureb_data,
        "GSTR-3B Data": generate_gstr3b_data,
        "RFD-01 Data": generate_rfd01_data,
        "E-way Bill Data": generate_ewaybill_data,
    }

    # Display the available document choices to the user
    print("Available document types:")
    for i, doc_name in enumerate(document_generators.keys()):
        print(f"{i + 1}. {doc_name}")

    while True:
        try:
            choice = int(
                input(
                    "Enter the number of the document type to generate (or 0 for all): "
                )
            )
            if choice < 0 or choice > len(document_generators):
                print("Invalid choice. Please try again.")
                continue
            break
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Output format
    output_format = input("Enter the output format (csv, json, excel): ").lower()
    while output_format not in ["csv", "json", "excel"]:
        print("Invalid output format. Please choose 'csv', 'json', or 'excel'.")
        output_format = input("Enter the output format (csv, json, excel): ").lower()

    # Number of records
    while True:
        try:
            num_records = int(input("Enter the number of records to generate: "))
            if num_records <= 0:
                print("Number of records must be positive. Please try again.")
                continue
            break
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Generate all documents
    if choice == 0:
        for generator_name, generator_func in document_generators.items():
            data, filename = generator_func(
                num_records=num_records, output_format=output_format
            )
            _write_data_to_file(data, output_format, filename)

    # Generate a specific document
    else:
        selected_doc = list(document_generators.keys())[choice - 1]
        generator_func = document_generators[selected_doc]
        data, filename = generator_func(
            num_records=num_records, output_format=output_format
        )
        _write_data_to_file(data, output_format, filename)

    print("\nData generation complete.")


if __name__ == "__main__":
    main()
