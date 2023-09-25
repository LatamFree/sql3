from bq import bq ,bigquery

sql = """

WITH
  normalized_fileds AS (
  SELECT *,
    `Fecha-I` AS dateI,
    `Fecha-O` AS dateO
  FROM
    `ado-boolean.vuelos_scl.vuelos` )

SELECT
  * EXCEPT(`Fecha-I`,`Fecha-O`),
  CASE
    WHEN (dateI > DATETIME(2017,12,14,12,59,00) OR dateI < DATETIME(2017,03,03,12,59,00)) THEN 1
    WHEN (dateI > DATETIME(2017,07,14,12,59,00)
    OR dateI < DATETIME(2017,07,31,12,59,00)) THEN 1
    WHEN (dateI > DATETIME(2017,09,10,12,59,00) OR dateI < DATETIME(2017,09,30,12,59,00)) THEN 1
  ELSE
  0
END
  AS temporada_alta,
TIMESTAMP_DIFF(dateO, dateI, MINUTE) AS dif_min,
 CASE 
    WHEN TIMESTAMP_DIFF(dateO, dateI, MINUTE) > 15 THEN 1
    ELSE 0
  END AS atraso_15,
CASE 
    WHEN EXTRACT(HOUR FROM dateI) BETWEEN 5 AND 11 THEN 'mañana'
    WHEN EXTRACT(HOUR FROM dateI) BETWEEN 12 AND 18 THEN 'tarde'
    WHEN EXTRACT(HOUR FROM dateI) >= 19 OR EXTRACT(HOUR FROM dateI) <= 4 THEN 'noche'
    ELSE NULL  -- Este caso nunca debería suceder, pero es bueno tenerlo por si acaso
  END AS periodo_dia
FROM
  normalized_fileds
"""

dataset_ref = bq.dataset('vuelos_druminot')
table_ref = dataset_ref.table('vuelos_view')
job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.allow_large_results = True
job_config.use_legacy_sql = False
query_job = bq.query(sql, job_config=job_config)
query_job.result()

#y ahora crea la misma materialized view en el dataset de prueba

view_ref = dataset_ref.table('vuelos_view_test')
view = bigquery.Table(view_ref)
view.mview_query = sql
view = bq.create_table(view)  # API request
print("Se creó la vista materializada {}".format(view.table_id))
