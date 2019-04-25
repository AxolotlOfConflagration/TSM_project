Run Apache Cassandra Cluster
```shell
sudo docker run --name main_cass -d -p 9042:9042 --rm cassandra:3
sudo docker cp queries.cql main_cass:/queries.cql
sudo docker exec -it main_cass cqlsh
SOURCE 'queries.cql'
```
