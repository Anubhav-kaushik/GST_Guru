import pandas as pd
import numpy as np
import json
from typing import List, Union, Optional, Any
import csv

class DataManager:
    """
    A class to manage structured data from various file formats, providing easy-to-use functions
    for common data manipulation tasks.  Designed with non-coders in mind, focusing on clarity
    and simple operations.
    """

    def __init__(self, file_path: Optional[str] = None, file_type: Optional[str] = None, data: Optional[pd.DataFrame] = None):
        """
        Initializes the DataManager with a file path, file type, or existing Pandas DataFrame.

        Args:
            file_path (str, optional): Path to the data file (e.g., 'data.csv', 'data.xlsx'). Defaults to None.
            file_type (str, optional): Type of the file ('csv', 'excel', 'json'). Defaults to None.  If `file_path` is provided,
                                          the `file_type` can be inferred. If not, it MUST be specified.
            data (pd.DataFrame, optional):  An existing Pandas DataFrame. Defaults to None.  If this is provided,
                                                `file_path` and `file_type` are ignored.
        Raises:
            ValueError: If neither file_path nor data is provided.  Also raised if file_type is not valid, or file fails to load.
        """
        if data is not None:
            if not isinstance(data, pd.DataFrame):
                raise ValueError("The 'data' argument must be a Pandas DataFrame.")
            self.data: pd.DataFrame = data
            self.file_path: Optional[str] = None
            self.file_type: Optional[str] = None

        elif file_path is not None:
            self.file_path = file_path
            if file_type is None:
                # Infer file type from the file extension
                if file_path.endswith(".csv"):
                    self.file_type = "csv"
                elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                    self.file_type = "excel"
                elif file_path.endswith(".json"):
                    self.file_type = "json"
                else:
                    raise ValueError("Could not infer file type from file extension. Please specify file_type.")
            else:
                self.file_type = file_type.lower()  # Ensure lowercase for consistency

            try:
                if self.file_type == "csv":
                    self.data = pd.read_csv(self.file_path)
                elif self.file_type == "excel":
                    self.data = pd.read_excel(self.file_path)
                elif self.file_type == "json":
                    # Handle JSON structures that are lists of dictionaries or a single dictionary
                    with open(self.file_path, 'r') as f:
                        json_data = json.load(f)
                    if isinstance(json_data, list):
                        self.data = pd.DataFrame(json_data)
                    elif isinstance(json_data, dict):
                        self.data = pd.DataFrame([json_data])  # Put a single dictionary into a list
                    else:
                        raise ValueError("JSON file must contain a list of dictionaries or a single dictionary at the top level.")
                else:
                    raise ValueError("Unsupported file type.  Must be 'csv', 'excel', or 'json'.")
            except Exception as e:
                raise ValueError(f"Error loading file: {e}")

        else:
            raise ValueError("Either 'file_path' or 'data' must be provided.")

        if not isinstance(self.data, pd.DataFrame):
            raise ValueError("Failed to load data into a Pandas DataFrame.")


    def select_columns(self, columns: List[str]) -> 'DataManager':
        """
        Selects specific columns from the data.

        Args:
            columns (list): A list of column names to select.

        Returns:
            DataManager: A new DataManager instance containing only the selected columns.

        Raises:
            KeyError: If any of the specified column names do not exist.
        """
        try:
            new_data = self.data[columns].copy()  # Create a copy to avoid modifying the original DataFrame
            return DataManager(data=new_data)  # Create a new DataManager instance
        except KeyError as e:
            raise KeyError(f"Column(s) not found: {e}")

    def compare_data(self, other_data: Union['DataManager', pd.DataFrame], on: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Compares the data with another DataFrame or DataManager.

        Args:
            other_data (DataManager or pd.DataFrame): The data to compare with.
            on (str or list, optional):  Column(s) to use as a key for merging/comparing.
                                          If None, compares all columns. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame showing the differences between the two datasets.  Uses a merge strategy
                          and adds suffixes to differentiate columns with the same name.

        Raises:
            TypeError: If other_data is not a DataManager or pd.DataFrame.
        """

        if isinstance(other_data, DataManager):
            other_df = other_data.data
        elif isinstance(other_data, pd.DataFrame):
            other_df = other_data
        else:
            raise TypeError("other_data must be a DataManager or a Pandas DataFrame.")

        # Use merge to find differences
        comparison = pd.merge(self.data, other_df, on=on, how='outer', indicator=True, suffixes=('_self', '_other'))
        return comparison


    def sort_data(self, by: Union[str, List[str]], ascending: bool = True) -> 'DataManager':
        """
        Sorts the data by one or more columns.

        Args:
            by (str or list): Column(s) to sort by.
            ascending (bool, optional): Whether to sort in ascending order. Defaults to True.

        Returns:
            DataManager: A new DataManager instance with the sorted data.
        """
        new_data = self.data.sort_values(by=by, ascending=ascending).copy()
        return DataManager(data=new_data)


    def filter_data(self, condition: str) -> 'DataManager':
        """
        Filters the data based on a condition.  The condition should be a string that can be evaluated
        as a boolean expression using Pandas' query() method.  For example:  "Age > 25 and City == 'New York'"

        Args:
            condition (str): The filtering condition (e.g., "Age > 25").

        Returns:
            DataManager: A new DataManager instance with the filtered data.
        Raises:
            ValueError: If the condition is invalid or causes an error during evaluation.
        """
        try:
            new_data = self.data.query(condition).copy()
            return DataManager(data=new_data)
        except Exception as e:
            raise ValueError(f"Invalid filter condition: {e}")


    def create_new_data(self, column_name: str, operation: str) -> 'DataManager':
        """
        Creates a new column based on an operation. The operation should be a string that can be
        evaluated using Pandas' `eval()` method.  This allows you to perform calculations using existing columns.

        Args:
            column_name (str): The name of the new column.
            operation (str): The operation to perform (e.g., "Price * Quantity").

        Returns:
            DataManager: A new DataManager instance with the new column added.

        Raises:
            ValueError: If the operation is invalid or causes an error during evaluation.
        """
        try:
            new_data = self.data.copy()  # Always copy before modifying

            # Check if operation involves calculations and use numpy if applicable
            if any(op in operation for op in ['+', '-', '*', '/', '**']):
                try:
                    new_data[column_name] = new_data.eval(operation).astype(np.float64)
                except Exception as e:
                    raise ValueError(f"Error during calculation with numpy: {e}")
            else:
                new_data[column_name] = new_data.eval(operation)


            return DataManager(data=new_data)
        except Exception as e:
            raise ValueError(f"Invalid operation: {e}")

    def clean_data(self, column: str, method: str = "remove_na", fill_value: Optional[Any] = None) -> 'DataManager':
        """
        Cleans data in a specific column using various methods.

        Args:
            column (str): The name of the column to clean.
            method (str, optional): The cleaning method to use:
                - "remove_na": Removes rows with missing values (NaN) in the specified column.
                - "fill_na": Fills missing values (NaN) with a specified value.
                Defaults to "remove_na".
            fill_value (any, optional): The value to fill missing values with (used with "fill_na").
                                        Defaults to None.

        Returns:
            DataManager: A new DataManager instance with the cleaned data.

        Raises:
            ValueError: If an invalid cleaning method is specified.
            KeyError: If the specified column does not exist.
        """

        try:
            new_data = self.data.copy()  # Create a copy to avoid modifying the original DataFrame

            if method == "remove_na":
                new_data = new_data.dropna(subset=[column])
            elif method == "fill_na":
                if fill_value is None:
                    raise ValueError("fill_value must be specified when using the 'fill_na' method.")

                # Use numpy to fill NaN values if the column is numeric
                if pd.api.types.is_numeric_dtype(new_data[column]):
                    new_data[column] = new_data[column].fillna(np.nan_to_num(fill_value))
                else:
                    new_data[column] = new_data[column].fillna(fill_value)

            else:
                raise ValueError("Invalid cleaning method. Must be 'remove_na' or 'fill_na'.")

            return DataManager(data=new_data)

        except KeyError:
            raise KeyError(f"Column '{column}' not found.")
        except ValueError as e:
            raise ValueError(f"Error during data cleaning: {e}")


    def save_data(self, output_path: str, file_type: str = "csv", index: bool = False) -> None:
        """
        Saves the data to a file.

        Args:
            output_path (str): The path to save the file to (e.g., 'output.csv', 'output.xlsx').
            file_type (str, optional): The file type to save as ('csv', 'excel', 'json'). Defaults to "csv".
            index (bool, optional): Whether to include the DataFrame index in the output file. Defaults to False.
        Raises:
            ValueError: If an unsupported file type is specified.
        """
        try:
            if file_type == "csv":
                self.data.to_csv(output_path, index=index)
            elif file_type == "excel":
                self.data.to_excel(output_path, index=index)
            elif file_type == "json":
                self.data.to_json(output_path, orient="records", indent=4)  # Use orient='records' for list of dictionaries
            else:
                raise ValueError("Unsupported file type.  Must be 'csv', 'excel', or 'json'.")
        except Exception as e:
            print(f"Error saving data: {e}")


    def display_head(self, n: int = 5) -> None:
        """
        Displays the first n rows of the data.  Useful for quickly inspecting the data.

        Args:
            n (int, optional): The number of rows to display. Defaults to 5.
        """
        print(self.data.head(n))


    def describe_data(self) -> None:
        """
        Provides descriptive statistics of the data (count, mean, std, min, max, etc.).  Useful for understanding
        the distribution and characteristics of numerical columns.
        """
        print(self.data.describe())


    def get_column_names(self) -> List[str]:
        """
        Returns a list of column names in the DataFrame.  Useful when you need to know the available columns
        for other operations.

        Returns:
            list: A list of column names.
        """
        return list(self.data.columns)

    def convert_file(self, input_path: str, input_type: str, output_path: str, output_type: str) -> None:
        """
        Converts a file from one type (csv, json, excel) to another.

        Args:
            input_path (str): Path to the input file.
            input_type (str): Type of the input file ('csv', 'json', 'excel').
            output_path (str): Path to save the converted file.
            output_type (str): Type of the output file ('csv', 'json', 'excel').

        Raises:
            ValueError: If an unsupported file type is specified or if there's an error during conversion.
        """
        try:
            data_manager = DataManager(file_path=input_path, file_type=input_type)
            data_manager.save_data(output_path=output_path, file_type=output_type)
            print(f"Successfully converted {input_type} to {output_type}")
        except Exception as e:
            raise ValueError(f"Error converting file: {e}")

if __name__ == "__main__":
    # Example Usage:

    # 1. Load data from a CSV file
    try:
        data_manager = DataManager(file_path="gst_data.csv")  # Replace with your actual CSV file
        print("Data loaded successfully from CSV.")
    except ValueError as e:
        print(f"Error loading CSV data: {e}")
        exit() # Stop execution if the initial file loading fails.

    # 2. Display the first few rows
    data_manager.display_head()

    # 3. Select specific columns
    try:
        selected_data = data_manager.select_columns(["GSTIN", "Taxable Value", "Tax Rate"])
        print("\nSelected columns:")
        selected_data.display_head()
    except KeyError as e:
        print(f"Error selecting columns: {e}")


    # 4. Filter data
    try:
        filtered_data = data_manager.filter_data("`Taxable Value` > 100000")  # Use backticks for column names with spaces
        print("\nFiltered data (Taxable Value > 100000):")
        filtered_data.display_head()
    except ValueError as e:
        print(f"Error filtering data: {e}")


    # 5. Create a new column (e.g., calculate total tax)
    try:
        new_data = data_manager.create_new_data("Total Tax", "`Taxable Value` * (`Tax Rate` / 100)")
        print("\nData with new 'Total Tax' column:")
        new_data.display_head()
    except ValueError as e:
        print(f"Error creating new column: {e}")

    # 6. Clean the data (remove rows with missing values in a column)
    try:
        cleaned_data = data_manager.clean_data("GSTIN", method="remove_na")
        print("\nCleaned data (removed rows with missing GSTIN):")
        cleaned_data.display_head()
    except KeyError as e:
        print(f"Error cleaning data: {e}")


    #7. Sort the data
    try:
        sorted_data = data_manager.sort_data(by="Taxable Value", ascending=False)
        print("\nSorted data (by Taxable Value, descending):")
        sorted_data.display_head()
    except Exception as e:
        print(f"Error sorting data: {e}")


    # 8. Load a second data file (e.g., for comparison)
    try:
        data_manager2 = DataManager(file_path="gst_data_updated.csv") # Replace with your second file
        print("Second data file loaded successfully.")
    except ValueError as e:
        print(f"Error loading second data file: {e}")
        exit()

    # 9. Compare the data with another file
    try:
        comparison_result = data_manager.compare_data(data_manager2, on="GSTIN")
        print("\nComparison result:")
        print(comparison_result.head()) # Display the first few rows of the comparison
    except TypeError as e:
        print(f"Error comparing data: {e}")

    # 10. Save the processed data to a new file
    try:
        data_manager.save_data("gst_data_processed.xlsx", file_type="excel")  # Save as an Excel file
        print("\nData saved to gst_data_processed.xlsx")
    except ValueError as e:
        print(f"Error saving data: {e}")

    # 11. Example using an existing DataFrame:
    data = pd.DataFrame({'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 28]})
    data_manager3 = DataManager(data=data)
    data_manager3.display_head()

    # 12. Example of file conversion:
    try:
        data_manager.convert_file(input_path="gst_data.csv", input_type="csv", output_path="gst_data.json", output_type="json")
    except ValueError as e:
        print(f"Error during file conversion: {e}")