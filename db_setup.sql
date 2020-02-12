
DROP TABLE IF EXISTS connections;
CREATE TABLE connections (
  connection_id serial PRIMARY KEY,
  account_number varchar(10) NOT NULL,
  node_id varchar(255) NOT NULL,
  route_table varchar(255) NOT NULL,
  pod_name varchar(255) NOT NULL
);
