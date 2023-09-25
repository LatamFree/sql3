from bq import bq
dataset = bq.get_dataset('scrioting_druminot')

print (dataset.description)
dataset.description = 'new_description'
bq.update_dataset(dataset, ['description'])
print (dataset.description)

