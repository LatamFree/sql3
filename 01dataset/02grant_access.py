from bq import bq, bigquery

dataset = bq.get_dataset("ado-boolean.scrioting_druminot")
entry = bigquery.AccessEntry(
    role="READER",
    entity_type="userByEmail",
    entity_id="sebastian@boolean.cl",
)
if entry not in dataset.access_entries:
  entries = list(dataset.access_entries)
  entries.append(entry)
  dataset.access_entries = entries
  dataset = bq.update_dataset(dataset, ["access_entries"])  # API request
else:
  print('{} already has access'.format(entry.entity_id))

print(dataset.access_entries)
