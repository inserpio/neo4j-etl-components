package org.neo4j.etl.commands;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.metadata.Join;
import org.neo4j.etl.sql.metadata.JoinTable;
import org.neo4j.etl.sql.metadata.Table;
import org.neo4j.etl.sql.metadata.TableInfo;
import org.neo4j.etl.sql.metadata.TableInfoAssembler;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.ArrayUtils;

public class DatabaseInspector
{
    private final List<String> tablesToExclude;
    private final DatabaseClient databaseClient;
    private final TableInfoAssembler tableInfoAssembler;

    public DatabaseInspector( DatabaseClient databaseClient, List<String> tablesToExclude )
    {
        this(databaseClient, Formatting.DEFAULT, tablesToExclude);
    }

    public DatabaseInspector(DatabaseClient databaseClient, Formatting formatting, List<String> tablesToExclude) {
        this.databaseClient = databaseClient;
        this.tablesToExclude = tablesToExclude;
        this.tableInfoAssembler = new TableInfoAssembler( databaseClient, formatting, tablesToExclude );
    }

    public SchemaExport buildSchemaExport() throws Exception
    {
        HashSet<Join> joins = new HashSet<>();
        HashSet<Table> tables = new HashSet<>();
        HashSet<JoinTable> joinTables = new HashSet<>();

        for ( TableName tableName : databaseClient.tableNames() )
        {
            if ( !ArrayUtils.containsIgnoreCase( tablesToExclude, tableName.simpleName() ) )
            {
                buildSchema( tableName, tables, joins, joinTables );
            }
        }

        return new SchemaExport( tables, joins, joinTables );
    }

    private void buildSchema( TableName tableName,
                              Collection<Table> tables,
                              Collection<Join> joins,
                              Collection<JoinTable> joinTables ) throws Exception
    {
        TableInfo tableInfo = tableInfoAssembler.createTableInfo( tableName );

        if ( tableInfo.representsJoinTable() )
        {
            joinTables.add( tableInfo.createJoinTable() );
        }
        else
        {
            tables.add( tableInfo.createTable() );
            joins.addAll( tableInfo.createJoins() );
        }
    }
}
