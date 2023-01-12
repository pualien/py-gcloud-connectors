





import os
from gcloud_connectors.bigquery import BigQueryConnector







project_id = os.environ['PROJECT_ID']
bq_service = BigQueryConnector(project_id=project_id)




df = bq_service.pd_execute(query='''
SELECT COUNT(*) AS num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = 'gcloud-connectors'
  AND details.installer.name = 'pip'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND CURRENT_DATE()
''')


...