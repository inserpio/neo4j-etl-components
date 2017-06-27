package org.neo4j.etl.commands;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;
import org.neo4j.etl.sql.metadata.*;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class DatabaseInspector {
    private final DatabaseClient databaseClient;
    private final Formatting formatting;
    private final FilterOptions filterOptions;

    public DatabaseInspector(DatabaseClient databaseClient) throws SQLException {
        this(databaseClient, Formatting.DEFAULT);
    }

    public DatabaseInspector(DatabaseClient databaseClient, Formatting formatting) throws SQLException {
        this.databaseClient = databaseClient;
        this.filterOptions = FilterOptions.DEFAULT;
        this.formatting = formatting;
    }

    public DatabaseInspector(DatabaseClient databaseClient, Formatting formatting, FilterOptions filterOptions) throws SQLException {
        this.databaseClient = databaseClient;
        this.filterOptions = filterOptions;
        this.formatting = formatting;
    }

    public SchemaExport buildSchemaExport(Schema schema) throws Exception {
        final TableInfoAssembler tableInfoAssembler = new TableInfoAssembler(databaseClient, formatting, filterOptions, schema);

        HashSet<Join> joins = new HashSet<>();
        HashSet<Table> tables = new HashSet<>();
        HashSet<JoinTable> joinTables = new HashSet<>();

        Collection<TableInfo> tableInfo = tableInfoAssembler.createTableInfo(schema);
        for (TableInfo info : tableInfo) {
            if (info.representsJoinTable()) {
                joinTables.add(info.createJoinTable());
            } else {
                tables.add(info.createTable());
                joins.addAll(info.createJoins());
            }
        }

        return new SchemaExport(tables, joins, joinTables);
    }
}
