#!/bin/bash -xe
#
# https://hub.docker.com/r/wnameless/oracle-xe-11g/
#
# docker pull wnameless/oracle-xe-11g
# docker run --name neo4j-etl-oracle -d -p 49160:22 -p 49161:1521 wnameless/oracle-xe-11g
#
# ssh root@localhost -p 49160
# password: admin
#
# sqlplus system/oracle
#
# CREATE USER neo4j IDENTIFIED BY neo4j;
# GRANT DBA TO neo4j;
#
# CREATE USER exclusion IDENTIFIED BY exclusion;
# GRANT DBA TO exclusion;
#
#
#
#
#
