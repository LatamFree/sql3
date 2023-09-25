from google.cloud import bigquery

PROYECT='ado-boolean'
bq = bigquery.Client(project=PROYECT)

if __name__ == "__main__":
    dataset = bq.get_dataset('vuelos_druminot')

    print(dataset.dataset_id)
    print('---------')
    print(dataset.created)

    for access in dataset.access_entries:
        print(access.role)
        print(access.entity_id)
        print(access.entity_type)
        print('---------')


