from bq import bq

table = bq.get_table('bigquery-public-data.london_bicycles.cycle_hire')
print(table.num_rows)
