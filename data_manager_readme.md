# DataManager: A Simple Tool for Managing Your Data

This tool helps you work with your data files (like CSV, Excel, and JSON) in a simple and easy way. You don't need to be a programmer to use the basic features. It's designed to help you clean, organize, and analyze your data.

## What it Can Do

The `DataManager` can:

* **Load data from different file types:** It can read data from CSV, Excel (both `.xlsx` and `.xls` formats), and JSON files.
* **Select specific columns:**  You can pick only the columns you need to work with.
* **Compare data from different files:**  It can compare two datasets and show you the differences.
* **Sort your data:**  You can sort your data by one or more columns in ascending or descending order.
* **Filter your data:** You can select only the rows that meet specific criteria (e.g., "Show me all entries where the 'Sales' column is greater than 1000").
* **Create new columns:** You can create new columns by performing calculations on existing ones (e.g., calculate "Total Price" by multiplying "Price" and "Quantity").
* **Clean your data:**  It can remove rows with missing information or fill in missing values.
* **Convert files:** Convert files between different formats such as converting a csv file to json file
* **Save data to different file types:**  You can save your processed data as CSV, Excel, or JSON.

## How to Use It (Simplified Instructions)

**Note:**  These instructions assume someone with very little technical knowledge. A slightly more technical user can skip some of the setup instructions.

1. **Installation**
    * Install python if it's not installed on your device.
    * Install packages like pandas, numpy using pip.

    ```bash
    pip install pandas numpy
    ```

2. **Get the `data_manager.py` file:**  Download the `data_manager.py` file from wherever it's stored (e.g., a shared drive, a website). Save it to a folder on your computer.

3. **Prepare your data file:**  Make sure your data file (CSV, Excel, or JSON) is in the same folder as the `data_manager.py` file, or know the full path to its location on your computer.

4. **Basic Operations:**

    *To load and view data:*
        1. Open the file data_manager.py in a text editor
        2. Uncomment the example usages at the end of the file, change the `file_path` variable to your filename or the full path of the file and then run the file by typing `python data_manager.py` in the terminal.

    *Selecting Columns:*
        1. Open the file data_manager.py in a text editor
        2. Uncomment the example usages at the end of the file, change the `file_path` variable to your filename or the full path of the file.
        3. Also, change the columns name in line `selected_data = data_manager.select_columns(["GSTIN", "Taxable Value", "Tax Rate"])` to the column name that you need to be selected. The run the file by typing `python data_manager.py` in the terminal.

    *Filtering Columns:*
        1. Open the file data_manager.py in a text editor
        2. Uncomment the example usages at the end of the file, change the `file_path` variable to your filename or the full path of the file.
        3. Also, change the condition in line `filtered_data = data_manager.filter_data("`Taxable Value`> 100000")` to the filtering condition that you need. The run the file by typing `python data_manager.py` in the terminal.

    *Save the processed data:*
        1. Open the file data_manager.py in a text editor
        2. Uncomment the example usages at the end of the file, change the `file_path` variable to your filename or the full path of the file.
        3. Change the location of where you need to save the file in this line `data_manager.save_data("gst_data_processed.xlsx", file_type="excel")`. The run the file by typing `python data_manager.py` in the terminal.
5. **File conversions:**
    * Change the necessary files names in this lines `data_manager.convert_file(input_path="gst_data.csv", input_type="csv", output_path="gst_data.json", output_type="json")` and then run the file in terminal using command `python data_manager.py`.

## Detailed Function Explanations

These explanations are for users who want to understand a bit more about how the tool works.

* **`DataManager(file_path=..., file_type=..., data=...)`:**

  * This is how you create a `DataManager` object. Think of it as "opening" your data file with this tool.
  * `file_path`:  The location of your data file (e.g., `"C:/MyData/sales.csv"`).  Make sure to use forward slashes `/` in the path, even on Windows.
  * `file_type`: The type of file you're opening.  It can be `"csv"`, `"excel"`, or `"json"`.  If you provide the file path, the tool can usually figure out the file type automatically.
  * `data`: You usually won't use this directly. It's for more advanced users who already have their data loaded in a special format.

    *Example:*

    ```python
    #To open csv
    data_manager = DataManager(file_path="my_data.csv") #if data_manager.py and file are in same folder.
    data_manager = DataManager(file_path="C:/users/abc/Documents/my_data.csv") #Provide full path
    #To open excel
    data_manager = DataManager(file_path="my_data.xlsx") #if data_manager.py and file are in same folder.
    data_manager = DataManager(file_path="C:/users/abc/Documents/my_data.xlsx") #Provide full path
    #To open json
    data_manager = DataManager(file_path="my_data.json") #if data_manager.py and file are in same folder.
    data_manager = DataManager(file_path="C:/users/abc/Documents/my_data.json") #Provide full path
    ```

* **`select_columns(columns=...)`:**

  * This lets you choose which columns from your data you want to keep.
  * `columns`: A list of the names of the columns you want.  Make sure to type the column names exactly as they appear in your data file.

    *Example:*

    ```python
    # Select only the "Name" and "Email" columns
    selected_data = data_manager.select_columns(columns=["Name", "Email"])
    ```

* **`compare_data(other_data=..., on=...)`:**

  * Compares the data with another DataFrame or DataManager.
  * `other_data`: The data to compare with. It can be another DataManager, or Dataframe.
  * `on`: Column(s) to use as a key for merging/comparing.
    *Example:*

    ```python
    comparison_result = data_manager.compare_data(other_data=data_manager2, on="GSTIN")
    ```

* **`sort_data(by=..., ascending=...)`:**

  * Sorts your data based on the values in one or more columns.
  * `by`: The name of the column (or a list of column names) you want to sort by.
  * `ascending`:  `True` to sort in ascending order (A to Z, smallest to largest).  `False` to sort in descending order (Z to A, largest to smallest).

    *Example:*

    ```python
    # Sort by "Date" in ascending order
    sorted_data = data_manager.sort_data(by="Date", ascending=True)

    # Sort by "Sales" in descending order
    sorted_data = data_manager.sort_data(by="Sales", ascending=False)
    ```

* **`filter_data(condition=...)`:**

  * Selects rows based on a condition you specify.
  * `condition`: A text string that describes the condition. You can use comparison operators like `>`, `<`, `=`, `>=`, `<=`, and `==`.  You can also use logical operators like `and`, `or`, and `not`. If your column name has space, then put the column name in between `` ` ``.

    *Example:*

    ```python
    # Show all rows where "Age" is greater than 25
    filtered_data = data_manager.filter_data(condition="Age > 25")

    # Show all rows where "City" is "New York" AND "Sales" is greater than 1000
    filtered_data = data_manager.filter_data(condition="City == 'New York' and Sales > 1000")
    ```

* **`create_new_data(column_name=..., operation=...)`:**

  * Creates a new column by performing a calculation or operation.
  * `column_name`: The name you want to give to the new column.
  * `operation`: A text string that describes the calculation. You can use column names in the calculation. If your column name has space, then put the column name in between `` ` ``.

    *Example:*

    ```python
    # Create a "Total Revenue" column by multiplying "Price" and "Quantity"
    new_data = data_manager.create_new_data(column_name="Total Revenue", operation="`Price` * `Quantity`")

    # Create a "Tax" column equal to 10% of the "Price" column
    new_data = data_manager.create_new_data(column_name="Tax", operation="`Price` * 0.10")
    ```

* **`clean_data(column=..., method=..., fill_value=...)`:**

  * Cleans up missing or invalid data in a column.
  * `column`: The name of the column you want to clean.
  * `method`: How you want to clean the data:
    * `"remove_na"`:  Removes rows where the specified column has missing values (represented as "NA" or blank).
    * `"fill_na"`:  Fills in missing values with a specific value.
  * `fill_value`:  The value to use to fill in missing values (only used with `method="fill_na"`).

    *Example:*

    ```python
    # Remove rows with missing values in the "Email" column
    cleaned_data = data_manager.clean_data(column="Email", method="remove_na")

    # Fill missing values in the "Age" column with the value 0
    cleaned_data = data_manager.clean_data(column="Age", method="fill_na", fill_value=0)
    ```

* **`save_data(output_path=..., file_type=..., index=...)`:**

  * Saves your processed data to a new file.
  * `output_path`: The location and name of the file you want to create (e.g., `"C:/MyData/processed_data.csv"`).
  * `file_type`: The type of file you want to save as. It can be `"csv"`, `"excel"`, or `"json"`.
  * `index`:  `False` to exclude the row numbers (index) from the saved file.  `True` to include them.  Generally, you want to set this to `False`.

    *Example:*

    ```python
    # Save the data as a CSV file without the index
    data_manager.save_data(output_path="processed_data.csv", file_type="csv", index=False)

    # Save the data as an Excel file without the index
    data_manager.save_data(output_path="processed_data.xlsx", file_type="excel", index=False)
    ```

* **`convert_file(input_path=..., input_type=..., output_path=..., output_type=...)`:**

  * Converts a file from one type (csv, json, excel) to another.
  * `input_path`: Path to the input file.
  * `input_type`: Type of the input file ('csv', 'json', 'excel').
  * `output_path`: Path to save the converted file.
  * `output_type`: Type of the output file ('csv', 'json', 'excel').

    *Example:*

    ```python
    #convert a csv file to a json file
    data_manager.convert_file(input_path="data.csv",input_type="csv",output_path="data.json", output_type="json")
    ```

* **`display_head(n=...)`:**

  * Shows the first few rows of your data, so you can quickly see what it looks like.
  * `n`: The number of rows to display (default is 5).

    *Example:*

    ```python
    # Show the first 10 rows of the data
    data_manager.display_head(n=10)
    ```

* **`describe_data()`:**

  * Provides summary statistics about your data (count, mean, standard deviation, minimum, maximum, etc.).  Useful for understanding the distribution of your data.

    *Example:*

    ```python
    data_manager.describe_data()
    ```

* **`get_column_names()`:**

  * Returns a list of the names of all the columns in your data.

    *Example:*

    ```python
    column_names = data_manager.get_column_names()
    print(column_names)
    ```

## Troubleshooting

* **File Not Found:** Make sure the `file_path` is correct and that the file exists in that location.
* **Column Not Found:** Double-check that you've typed the column names exactly as they appear in your data file (including capitalization).
* **Invalid Condition:** Make sure your filtering conditions are written correctly (e.g., use `==` for equality, not `=`).
* **Errors:** If you get an error message, try to read it carefully. It often provides clues about what went wrong.
* **Help:** Ask for help from someone with more technical experience.

## Important Notes

* **Case Sensitivity:** Column names are case-sensitive.  `"Name"` is different from `"name"`.
* **Spaces in Column Names:** If your column names contain spaces, you may need to enclose them in backticks (`` ` ``) when using them in filtering conditions or calculations (e.g., `` `Total Sales` > 1000 ``).
* **Backup Your Data:** Always make a backup of your original data file before processing it with this tool.  That way, if something goes wrong, you can always start over with the original data.
* **Paths:** Make sure you use the correct type of slashes on different OS, prefer using forward slashes `/` instead of backslashes `\` on Windows.
