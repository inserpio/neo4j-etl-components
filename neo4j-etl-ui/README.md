# neo4j-etl-ui


A web tool to inspect and consolidate neo4j-etl generated mapping files to import data into Neo4j super easy and in your preferred format.

**NOTE: This is a prototype version meant to provide a basic level of functionality useful for gathering initial feedback.**

![](public/img/neo4j-etl-ui.gif)

## Dependencies

`neo4j-etl-ui` is a node.js web application, therefore node.js (and npm, the node package manager which is bundled with node) is required. Installation instructions are available [here](http://nodejs.org)

## Installation

1. `git clone git@github.com:neo4j-contrib/neo4j-etl-components.git`
2. `cd neo4j-etl/neo4j-etl-ui`
3. `npm install`
4. `npm start`
5. Open web browser at url `http://localhost:3000`

## Overview

### Using the web based tool

This tool will parse your RDBMS to GRAPH mapping file and guide you through the process of consolidating your property graph data model to import your data into Neo4j in your desired format.

In 3 steps, you can easily inspect and consolidate your relational to graph database mapping:
1. Generate the mapping file via command line
```
./bin/neo4j-etl postgresql generate-metadata-mapping \
--rdbms:url jdbc:postgresql://localhost:5433/northwind?ssl=false \
--rdbms:user neo4j --rdbms:password neo4j > /tmp/northwind/mapping.json
```
2. Inspect and configure the graph data model with this visual tool
3. Save the consolidated mapping file to be used to import your data into Neo4j
```
./bin/neo4j-etl postgresql export \
--rdbms:url jdbc:postgresql://localhost:5433/northwind?ssl=false \
--rdbms:user neo4j --rdbms:password neo4j \
--using bulk:neo4j-import \
--mapping-file /Users/lorenzo/Downloads/mapping-ui.json \
--import-tool $NEO4J_HOME/bin \
--destination $NEO4J_HOME/data/databases/graph.db \
--csv-directory /tmp/northwind --options-file  /tmp/northwind/options.json \
--quote '"' --force
```