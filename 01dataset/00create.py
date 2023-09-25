from bq import bq
dataset_id = 'scrioting_druminot'

dataset = bq.dataset(dataset_id)
dataset.location = 'southamerica-west-1'
bq.create_dataset(dataset, exists_ok=True)
