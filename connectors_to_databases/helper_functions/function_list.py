def list_values_in_str_with_double_quotes(list_columns: list = None) -> str:
    """
    **Function: list_values_in_str_with_double_quotes.**

    This function takes a list of values as the `list_columns` parameter and returns a string where each
    value is enclosed in double quotes.

    **Parameters:**
    - `list_columns` (list): The list of values to be enclosed in double quotes.
    If not specified, defaults to `None`.

    **Return:**
    - `str`: A string containing values enclosed in double quotes and separated by commas.

    **Example Usage:**

    ```python
    columns = ['column1', 'column2', 'column3']
    result = list_values_in_str_with_double_quotes(columns)
    print(result)
    ```

    **Output:**

    ```
    "column1", "column2", "column3"
    ```

    In this example, we pass the `columns` list of columns to the `list_values_in_str_with_double_quotes`
    function and store the result in the `result` variable. Then, we print the value of `result`, which will
    contain the strings from the `columns` list enclosed in double quotes and separated by commas.


    @param list_columns: The list of columns to be enclosed in double quotes. If not specified, defaults
        to `None`.; default 'None'
    @return: A string containing values enclosed in double quotes and separated by commas.
    """ # noqa D415

    return ", ".join([f'"{value}"' for value in list_columns])


def list_values_in_str_with_single_quotes(list_columns: list = None) -> str:
    """
    **Function: list_values_in_str_with_single_quotes.**

    This function takes a list of values as the `list_columns` parameter and returns a string where each
    value is enclosed in single quotes.

    **Parameters:**
    - `list_columns` (list): The list of values to be enclosed in single quotes.
    If not specified, defaults to `None`.

    **Return:**
    - `str`: A string containing values enclosed in single quotes and separated by commas.

    **Example Usage:**

    ```python
    columns = ['column1', 'column2', 'column3']
    result = list_values_in_str_with_single_quotes(columns)
    print(result)
    ```

    **Output:**

    ```
    "column1", "column2", "column3"
    ```

    In this example, we pass the `columns` list of columns to the `list_values_in_str_with_single_quotes`
    function and store the result in the `result` variable. Then, we print the value of `result`, which will
    contain the strings from the `columns` list enclosed in single quotes and separated by commas.


    @param list_columns: The list of columns to be enclosed in single quotes. If not specified, defaults
        to `None`.; default 'None'.
    @return: A string containing values enclosed in single quotes and separated by commas.
    """ # noqa D415

    return ", ".join([f"'{value}'" for value in list_columns])
