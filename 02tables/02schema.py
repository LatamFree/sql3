from bq import bq

table = bq.get_table('bigquery-public-data.london_bicycles.cycle_hire')
print(table.schema)

for field in table.schema:
    print(field.name)
    print(field.field_type)
    print(field.description)
    print(field.mode)
    print('---------')
