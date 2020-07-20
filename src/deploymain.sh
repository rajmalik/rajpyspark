gcloud dataproc jobs submit pyspark main.py  \
    --cluster=c-bc3b7f56-10nodes-1core-2000mb-149-exec-75heap \
    --py-files jobs.zip \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    -- --job wordcount