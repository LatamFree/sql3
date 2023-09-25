from bq import bq 

tables = bq.list_tables('bigquery-public-data.london_bicycles')
print('---------')
for table in tables:
    print(table.table_id)
    print(table.created)
    print(table.expires)
    print(table.table_type)
    print(table.num_rows)
    print('---------')



