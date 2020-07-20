gcloud dataproc jobs submit pyspark bigquery.py \
    --cluster=c-bc3b7f56-10nodes-1core-2000mb-149-exec-75heap \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar