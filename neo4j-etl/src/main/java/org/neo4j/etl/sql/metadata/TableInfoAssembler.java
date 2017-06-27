package org.neo4j.etl.sql.metadata;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.DatabaseCatalog;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TableInfoAssembler {
    static final String SYNTHETIC_PRIMARY_KEY_NAME = "_ROW_INDEX_";

    private final Formatting formatting;
    private final DatabaseCatalog databaseCatalog;

    /**
     * Used mostly for test purposes
     *
     * @param databaseCatalog
     */
    public TableInfoAssembler(DatabaseCatalog databaseCatalog) {
        this.formatting = Formatting.DEFAULT;
        this.databaseCatalog = databaseCatalog;
    }

    public TableInfoAssembler(DatabaseClient databaseClient, Formatting formatting, FilterOptions filterOptions, Schema schema) throws SQLException {
        this.formatting = formatting;
        this.databaseCatalog = new DatabaseCatalog(databaseClient, formatting, filterOptions, schema);
    }

    public TableInfo createTableInfo(TableName tableName) throws Exception {
        Collection<Column> keyColumns = new HashSet<>();

        Map<String, Column> allColumns = createColumnsMapFromTableName(tableName);
        Collection<JoinKey> foreignKeys = createForeignKeysFromTableName(tableName);
        Optional<Column> primaryKey = createPrimaryKeysFromTableName(tableName, foreignKeys);

        return new TableInfo(
                tableName,
                primaryKey,
                foreignKeys,
                columnsLessKeyColumns(allColumns, keyColumns));
    }

    /**
     * Collects info for all the tables
     *
     * @return
     * @throws Exception
     */
    public Collection<TableInfo> createTableInfo(Schema schema) throws Exception {
        Collection<TableName> tableNames = this.databaseCatalog.getTables(schema);
        List<TableInfo> tableInfo = new ArrayList<>();

        for (TableName tableName : tableNames) {
            tableInfo.add(createTableInfo(tableName));
        }

        return tableInfo;
    }

    /**
     * Use this instead of createColumnsMap
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    protected Map<String, Column> createColumnsMapFromTableName(TableName tableName) throws Exception {
        return databaseCatalog.getColumns(tableName).stream().collect(Collectors.toMap(Column::name, c -> c));
    }

    /**
     * Use this instead of createForeignKeys
     *
     * @param tableName
     * @return
     */
    protected Collection<JoinKey> createForeignKeysFromTableName(TableName tableName) {
        return databaseCatalog.getForeignKeys(tableName);
    }

    /**
     * Use this instead of createPrimaryKey
     *
     * @param tableName
     * @return
     */
    protected Optional<Column> createPrimaryKeysFromTableName(TableName tableName, Collection<JoinKey> foreignKeys) {
        Collection<Column> primaryKeys = databaseCatalog.getPrimaryKey(tableName);

        if (primaryKeys.isEmpty()) {
            if (notJoinTable(foreignKeys)) {
                return Optional.of(createRowIndexBasedPrimaryKey(tableName));
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.of(new CompositeColumn(tableName, primaryKeys, ColumnRole.PrimaryKey));
        }
    }


    private boolean notJoinTable(Collection<JoinKey> foreignKeys) {
        return foreignKeys.size() != 2;
    }

    private SimpleColumn createRowIndexBasedPrimaryKey(TableName table) {
        return new SimpleColumn(
                table,
                SYNTHETIC_PRIMARY_KEY_NAME,
                ColumnRole.PrimaryKey,
                SqlDataType.INT,
                ColumnValueSelectionStrategy.SelectRowIndex,
                formatting);
    }

    private List<Column> columnsLessKeyColumns(Map<String, Column> allColumns, Collection<Column> keyColumns) {
        return allColumns.values().stream().filter(c -> !keyColumns.contains(c)).collect(Collectors.toList());
    }
}
