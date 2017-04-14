#!/bin/bash -xe

docker pull postgres
docker run --name neo4j-etl-postgres -e POSTGRES_USER=neo4j -e POSTGRES_PASSWORD=neo4j -d -p 5433:5432 postgres

#
# docker run -it --rm --link neo4j-etl-postgres:postgres postgres psql -h postgres -U neo4j
#
# DROP DATABASE IF EXISTS exclusion;
# CREATE DATABASE exclusion WITH OWNER 'neo4j' ENCODING 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';
#
# DROP DATABASE IF EXISTS northwind;
# CREATE DATABASE northwind WITH OWNER 'neo4j' ENCODING 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';
#
# neo4j=# \q
#
