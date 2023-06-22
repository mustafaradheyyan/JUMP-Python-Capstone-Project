TABLE_NAME = "seismic_event"

def get_rows(num_of_rows):

    sql_statement = f'''
    SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY type) AS n
            FROM {TABLE_NAME}
             ) AS x
        WHERE n <= {num_of_rows};
    '''
    
    return sql_statement
