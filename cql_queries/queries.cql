CREATE KEYSPACE IF NOT EXISTS tsm_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS tsm_keyspace.shoprecs(shop_id int, recommendations text, PRIMARY KEY(shop_id));
CREATE TABLE IF NOT EXISTS tsm_keyspace.itemrecs(product_id int, recommendations text, PRIMARY KEY(product_id));
CREATE TABLE IF NOT EXISTS tsm_keyspace.top10products(product_id varchar, amount int, description varchar, PRIMARY KEY(product_id));
CREATE TABLE IF NOT EXISTS tsm_keyspace.busiestHourOfDay(hour varchar, amount int, PRIMARY KEY(hour));
CREATE TABLE IF NOT EXISTS tsm_keyspace.top3PopularCategoryProduct(category varchar, PRIMARY KEY(category));
CREATE TABLE IF NOT EXISTS tsm_keyspace.mostPopularItemInABasket(id int,items list<text>, freq int, PRIMARY KEY(id));
CREATE TABLE IF NOT EXISTS tsm_keyspace.ifThen(id int, antecedent list<text>, consequent list<text>, confidence double, lift double, PRIMARY KEY(id));