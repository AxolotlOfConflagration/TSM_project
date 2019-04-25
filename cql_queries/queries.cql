CREATE KEYSPACE IF NOT EXISTS tsm_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS tsm_keyspace.rec(user_id int, ratings float, PRIMARY KEY(user_id));
