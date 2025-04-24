def read_sql_file(filename:str,sub_query: bool = False) -> str:
    """
    Reads and returns the content of a SQL file as a string

    @params
        filename : str
            The name of the SQL file to read (without the .sql extension).
        sub_query : bool, optional
            If True, the file is read from the 'sql_files/sub_query/' directory.
            If False, the file is read from the 'sql_files/' directory.
            Default is False.

    @returns:
        str
            The content of the SQL file as a string.
    """
    if sub_query:
        with open('sql_files/'
                  'sub_query/'
                  f'{filename}.sql','r') as file:
            return file.read()

    else:
        with open('sql_files/'
                  f'{filename}.sql','r') as file:
            return file.read()