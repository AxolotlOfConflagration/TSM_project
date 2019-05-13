Run Apache Cassandra Cluster
```shell
sudo docker run --name tsm_cass -d -p 9042:9042 --rm cassandra:3
sudo docker cp queries.cql tsm_cass:/queries.cql
sudo docker exec -it tsm_cass cqlsh
SOURCE 'queries.cql'
```
[Setup Redash](https://computingforgeeks.com/how-to-install-redash-data-visualization-dashboard-on-ubuntu-18-04-lts/)
