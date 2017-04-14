package org.neo4j.etl.sql.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.util.ArrayUtils;

import static java.lang.String.format;

public class TableInfoAssembler
{
    static final String SYNTHETIC_PRIMARY_KEY_NAME = "_ROW_INDEX_";

    private final DatabaseClient databaseClient;
    private final List<String> tablesToExclude;
    private final Formatting formatting;

    public TableInfoAssembler( DatabaseClient databaseClient, List<String> tablesToExclude )
    {
        this(databaseClient, Formatting.DEFAULT, tablesToExclude);
    }

    public TableInfoAssembler(DatabaseClient databaseClient, Formatting formatting, List<String> tablesToExclude) {
        this.databaseClient = databaseClient;
        this.formatting = formatting;
        this.tablesToExclude = tablesToExclude;
    }

    public TableInfo createTableInfo( TableName tableName ) throws Exception
    {
        Collection<Column> keyColumns = new HashSet<>();

        Map<String, Column> allColumns = createColumnsMap( tableName );
        Collection<JoinKey> foreignKeys = createForeignKeys( tableName, allColumns, keyColumns );
        Optional<Column> primaryKey = createPrimaryKey( tableName, allColumns, keyColumns, foreignKeys );

        return new TableInfo(
                tableName,
                primaryKey,
                foreignKeys,
                columnsLessKeyColumns( allColumns, keyColumns ) );
    }

    private Map<String, Column> createColumnsMap( TableName tableName ) throws Exception
    {
        try ( QueryResults columnsResults = databaseClient.columns( tableName ) )
        {
            return columnsResults.stream()
                    .map( row -> new String[]{row.get( "COLUMN_NAME" ), row.get( "TYPE_NAME" )} )
                    .collect( Collectors.toMap( v -> v[0], v -> v[1] ) )
                    .entrySet().stream()
                    .map( e -> new SimpleColumn(
                            tableName,
                            e.getKey(),
                            ColumnRole.Data,
                            SqlDataType.parse( e.getValue() ),
                            ColumnValueSelectionStrategy.SelectColumnValue,
                            formatting))
                    .filter( c -> !c.sqlDataType().skipImport() )
                    .collect( Collectors.toMap( Column::name, c -> c ) );
        }
    }

    private Collection<JoinKey> createForeignKeys( TableName table,
                                                   Map<String, Column> columns,
                                                   Collection<Column> keyColumns ) throws Exception
    {
        try ( QueryResults foreignKeysResults = databaseClient.foreignKeys( table ) )
        {
            Map<String, List<Map<String, String>>> foreignKeyGroups = foreignKeysResults.stream()
                    .collect( Collectors.groupingBy( row -> row.get( "FK_NAME" ) ) );

            Collection<JoinKey> keys = new ArrayList<>();

            for ( List<Map<String, String>> foreignKeyGroup : foreignKeyGroups.values() )
            {
                List<Column> sourceColumns = new ArrayList<>();
                List<Column> targetColumns = new ArrayList<>();

                foreignKeyGroup.forEach( fkRow ->
                {
                    if ( !ArrayUtils.containsIgnoreCase( tablesToExclude, fkRow.get( "PKTABLE_NAME" ) ) )
                    {
                        Column sourceColumn = columns.get( table.fullyQualifiedColumnName( fkRow.get( "FKCOLUMN_NAME" ) ) );

                        // We assume the key's target column data type is the same as the source column's data type
                        SqlDataType sqlDataType = sourceColumn.sqlDataType();

                        sourceColumns.add( sourceColumn );
                        targetColumns.add( createForeignKeyTargetColumn( fkRow, sqlDataType ) );
                    }
                } );

                if( targetColumns.size() > 0 )
                {
                    keyColumns.addAll( sourceColumns );

                    // We assume that for a composite foreign key, all parts of the key refer to the same target table
                    if ( targetColumns.stream().map( Column::table ).collect( Collectors.toSet() ).size() > 1 )
                    {
                        throw new IllegalStateException(
                                format( "Composite foreign key refers to more than one target table: %s",
                                        foreignKeyGroup ) );
                    }

                    TableName targetTable = targetColumns.get( 0 ).table();
                    keys.add( new JoinKey(
                            new CompositeColumn( table, sourceColumns, ColumnRole.ForeignKey ),
                            new CompositeColumn( targetTable, targetColumns, ColumnRole.PrimaryKey ) ) );
                }
            }

            return keys;
        }
    }

    private Optional<Column> createPrimaryKey( TableName table,
                                               Map<String, Column> columns,
                                               Collection<Column> keyColumns,
                                               Collection<JoinKey> foreignKeys ) throws Exception
    {
        try ( QueryResults primaryKeyResults = databaseClient.primaryKeys( table ) )
        {
            List<Column> primaryKeyColumns = primaryKeyResults.stream()
                    .map( pk -> columns.get( table.fullyQualifiedColumnName( pk.get( "COLUMN_NAME" ) ) ) )
                    .collect( Collectors.toList() );

            keyColumns.addAll( primaryKeyColumns );

            if ( primaryKeyColumns.isEmpty() )
            {
                if ( notJoinTable( foreignKeys ) )
                {
                    return Optional.of( createRowIndexBasedPrimaryKey( table ) );
                }
                else
                {
                    return Optional.empty();
                }
            }
            else
            {
                return Optional.of( new CompositeColumn( table, primaryKeyColumns, ColumnRole.PrimaryKey ) );
            }
        }
    }

    private boolean notJoinTable( Collection<JoinKey> foreignKeys )
    {
        return foreignKeys.size() != 2;
    }

    private SimpleColumn createRowIndexBasedPrimaryKey( TableName table )
    {
        return new SimpleColumn(
                table,
                SYNTHETIC_PRIMARY_KEY_NAME,
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectRowIndex );
    }

    private Column createForeignKeyTargetColumn( Map<String, String> fkRow, SqlDataType sqlDataType )
    {
        TableName targetTableName = new TableName(
                firstNonNullOrEmpty( fkRow.get( "PKTABLE_CAT" ), fkRow.get( "PKTABLE_SCHEM" ) ),
                fkRow.get( "PKTABLE_NAME" ) );

        return new SimpleColumn(
                targetTableName,
                fkRow.get( "PKCOLUMN_NAME" ),
                ColumnRole.Data,
                sqlDataType, ColumnValueSelectionStrategy.SelectColumnValue );
    }

    private List<Column> columnsLessKeyColumns( Map<String, Column> allColumns, Collection<Column> keyColumns )
    {
        return allColumns.values().stream().filter( c -> !keyColumns.contains( c ) ).collect( Collectors.toList() );
    }

    private String firstNonNullOrEmpty( String a, String b )
    {
        return StringUtils.isNotEmpty( a ) ? a : b;
    }
}
