def list_values_in_str_with_double_quotes(list_columns: list = None) -> str:  # noqa: RUF013
    """
    **Function: list_values_in_str_with_double_quotes**

    This static function takes a list of columns as the `list_columns` parameter and returns a string where each
    column value is enclosed in double quotes.

    **Parameters:**
    - `list_columns` (list, optional): The list of columns to be enclosed in double quotes.
    If not specified, defaults to `None`.

    **Return:**
    - `str`: A string containing column values enclosed in double quotes and separated by commas.

    **Example Usage:**

    ```python
    columns = ['column1', 'column2', 'column3']
    result = MyClass.list_columns_in_str_with_double_quotes(columns)
    print(result)
    ```

    **Output:**

    ```
    "column1", "column2", "column3"
    ```

    In this example, we pass the `columns` list of columns to the `list_columns_in_str_with_double_quotes`
    function and store the result in the `result` variable. Then, we print the value of `result`, which will
    contain the strings from the `columns` list enclosed in double quotes and separated by commas.


    @param list_columns: The list of columns to be enclosed in double quotes. If not specified, defaults
        to `None`.; default 'None'
    @return: A string containing column values enclosed in double quotes and separated by commas.
    """

    return ', '.join([f"\"{value}\"" for value in list_columns])