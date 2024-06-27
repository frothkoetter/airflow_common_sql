# Create a resource
./cde resource delete --name demo-dag 
./cde resource create --name demo-dag 
./cde resource upload --name demo-dag --local-path demo-dag.py
./cde resource describe --name demo-dag


# Create Job of “airflow” type and reference the DAG
./cde job delete --name demo-dag-job
./cde job create --name demo-dag-job --type airflow --dag-file demo-dag.py  --mount-1-resource demo-dag 

#Trigger Airflow job to run
./cde job run --name demo-dag-job 
