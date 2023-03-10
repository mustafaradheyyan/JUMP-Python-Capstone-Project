TABLE_NAME = "seismic_event"

def get_rows(num_of_rows):
    sql_statement = f'''SELECT * FROM {TABLE_NAME}
    LIMIT {num_of_rows};
    '''
    return sql_statement