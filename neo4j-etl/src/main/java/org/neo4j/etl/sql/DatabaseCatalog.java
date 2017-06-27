package org.neo4j.etl.sql;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.exportcsv.mapping.ExclusionMode;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.*;
import org.neo4j.etl.sql.metadata.Schema;
import schemacrawler.schema.*;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by alberto.delazzari on 07/06/17.
 */
public class DatabaseCatalog {

    /**
     * Default column types to exclude:
     * <ul>
     * <li>binary type</li>
     * <li>large objects</li>
     * <li>objects</li>
     * </ul>
     * These types include common BLOB and CLOB
     */
    static final Set<JavaSqlType.JavaSqlTypeGroup> DEFAULT_COLUMN_TYPES_TO_EXCLUDE = Arrays.asList(
            JavaSqlType.JavaSqlTypeGroup.binary,
            JavaSqlType.JavaSqlTypeGroup.large_object,
            JavaSqlType.JavaSqlTypeGroup.object).stream().collect(Collectors.toSet());

    private final Catalog catalog;
    private final Schema schema;
    private final Formatting formatting;

    private final Set<JavaSqlType.JavaSqlTypeGroup> columnTypesToExclude;

    /**
     * Used only for tests
     * Uses a default {@link Formatting} and a default {@link FilterOptions}
     *
     * @param databaseClient
     * @param schema
     * @throws SQLException
     */
    public DatabaseCatalog(DatabaseClient databaseClient, Schema schema) throws SQLException {
        this(databaseClient, Formatting.DEFAULT, FilterOptions.DEFAULT, schema);
    }

    /**
     * The DatabaseCatalog uses {@link FilterOptions} in order to apply many filters.
     * Filters can involve tables, columns (types or names) or any other database object.
     *
     * @param databaseClient
     * @param formatting
     * @param filterOptions
     * @param schema
     * @throws SQLException
     */
    public DatabaseCatalog(DatabaseClient databaseClient, Formatting formatting, FilterOptions filterOptions, Schema schema) throws SQLException {
        this.schema = (schema != null && schema.name() != null) ? schema : databaseClient.getSchema();
        this.catalog = databaseClient.getCatalog(createSchemaCrawlerOptions(filterOptions, this.schema));
        this.formatting = formatting;

        // If the column types are null than we use the default one
        // If the list is empty instead than we consider as a "no filter"
        this.columnTypesToExclude = filterOptions.getColumnTypesToExclude() == null ? DEFAULT_COLUMN_TYPES_TO_EXCLUDE : getSqlTypesFromNames(filterOptions.getColumnTypesToExclude());
    }

    /**
     * Converts a list of strings to JavaSqlTypeGroup
     *
     * @param columnTypeNames
     * @return
     */
    private Set<JavaSqlType.JavaSqlTypeGroup> getSqlTypesFromNames(List<String> columnTypeNames) {
        return columnTypeNames.stream()
                .map(name -> JavaSqlType.JavaSqlTypeGroup.valueOf(name))
                .collect(Collectors.toSet());
    }

    private SchemaCrawlerOptions createSchemaCrawlerOptions(FilterOptions filterOptions, Schema schema) {
        final SchemaCrawlerOptions schemaCrawlerOptions = new SchemaCrawlerOptions();

        // Tables to exclude but if the ExclusionMode is INCLUDE we have to "invert" the filter
        List<String> tablesToExclude = filterOptions.tablesToExclude();
        final ExclusionMode exclusionMode = filterOptions.exclusionMode();

        schemaCrawlerOptions.setSchemaInfoLevel(SchemaInfoLevelBuilder.standard());

        // Filter by the schema (with Oracle it takes too much time to crawl all the db instance)
        final String schemaName = schema != null ? schema.name().toUpperCase() : "";
        schemaCrawlerOptions.setSchemaInclusionRule((final String text) -> text.toUpperCase().contains(schemaName));

        final List<String> normalizedTablesToExclude = tablesToExclude.stream()
                .map(this::normalizeTableName)
                .collect(Collectors.toList());

        // TODO: tablesToExclude can be "inverted" so we need a (boolean) parameter to convert tables to exclude in tables to include
        schemaCrawlerOptions.setTableInclusionRule((final String table) ->
                normalizedTablesToExclude.stream()
                        // Ignore case equality in order to avoid case sensitive table names when a user defines tablesToExclude parameter
                        .filter(tableToExclude -> exclusionMode.equals(ExclusionMode.EXCLUDE)
                                ? tableToExclude.equalsIgnoreCase(table)
                                : !tableToExclude.equalsIgnoreCase(table))
                        .count() == 0
        );

        return schemaCrawlerOptions;
    }

    private String normalizeTableName(String tableName) {
        return this.schema.name() + "." + tableName;
    }

    /**
     * Get a {@link Table} from a {@link TableName}
     *
     * @param tableName
     * @return
     */
    public Table lookupTableByTableName(TableName tableName) {
        return catalog.lookupTable(catalog.lookupSchema(schema.name()).get(), tableName.simpleName()).get();
    }

    /**
     * Get all the tables for the given schema
     *
     * @param schema
     * @return
     */
    public Collection<TableName> getTables(Schema schema) {
        final String schemaName = (schema == null || schema.name() == null) ? this.schema.name() : schema.name();

        return catalog.lookupSchema(schemaName).isPresent() ? catalog.getTables(catalog.lookupSchema(schemaName).get()).stream()
                .map(table -> new TableName(schemaName, table.getName()))
                .collect(Collectors.toList()) : Collections.emptyList();
    }

    /**
     * Get all the columns for the table
     *
     * @param tableName
     * @return
     */
    public Collection<Column> getColumns(TableName tableName) {
        Table table = lookupTableByTableName(tableName);

        return table.getColumns().stream()
                .filter(column -> !this.skipColumn(column))
                .map(column -> new SimpleColumn(tableName,
                        column.getName(),
                        ColumnRole.Data,
                        new SqlDataType(column.getColumnDataType().getJavaSqlType()),
                        ColumnValueSelectionStrategy.SelectColumnValue,
                        formatting)
                ).collect(Collectors.toList());
    }

    /**
     * Get the primary key for the table.
     * N.B. it's an iterator because of the key could be a composite key including more than one column
     *
     * @param tableName
     * @return
     */
    public Collection<Column> getPrimaryKey(TableName tableName) {
        Table table = lookupTableByTableName(tableName);

        return table.getPrimaryKey() != null ?
                table.getPrimaryKey().getColumns().stream()
                        .map(indexColumn -> new SimpleColumn(tableName,
                                        indexColumn.getName(),
                                        ColumnRole.Data,
                                        new SqlDataType(indexColumn.getColumnDataType().getJavaSqlType()),
                                        ColumnValueSelectionStrategy.SelectColumnValue,
                                        formatting
                                )
                        ).collect(Collectors.toList()) : Collections.emptyList();
    }

    /**
     * Get all the foreign keys
     *
     * @param tableName
     * @return
     */
    public Collection<JoinKey> getForeignKeys(TableName tableName) {
        Table table = lookupTableByTableName(tableName);

        List<JoinKey> joinKeys = new ArrayList<>();

        // For each foreign key
        for (ForeignKey foreignKey : table.getForeignKeys()) {
            List<ForeignKeyColumnReference> foreignKeyColumnReferences = foreignKey.getColumnReferences();


            List<Column> foreignKeyColumns = new ArrayList<>();
            List<Column> primaryKeyColumns = new ArrayList<>();
            // Get the column references for each foreign key (it's a list, we can have more than one column)
            for (ForeignKeyColumnReference foreignKeyColumnReference : foreignKeyColumnReferences) {
                if (foreignKeyColumnReference.getForeignKeyColumn().getParent().getFullName().equals(tableName.fullName())) {
                    foreignKeyColumns.add(getSimpleColumnFromInnerColumn(foreignKeyColumnReference.getForeignKeyColumn()));

                    // TODO: try to find a smarter solution!
                    try {
                        primaryKeyColumns.add(getSimpleColumnFromInnerColumn(foreignKeyColumnReference.getPrimaryKeyColumn()));
                    } catch (Exception e) {
                        // This error will be raised when excluding tables
                    }
                }
            }

            if (foreignKeyColumns.size() > 0 && primaryKeyColumns.size() > 0) {
                joinKeys.add(new JoinKey(
                        new CompositeColumn(tableName, foreignKeyColumns, ColumnRole.ForeignKey),
                        new CompositeColumn(primaryKeyColumns.get(0).table(), primaryKeyColumns, ColumnRole.PrimaryKey))
                );
            }
        }

        return joinKeys;
    }

    /**
     * Converts a SchemaCrawler's (internal) representation of a column in a {@link Column} object
     *
     * @param innerColumn
     * @return
     */
    private Column getSimpleColumnFromInnerColumn(schemacrawler.schema.Column innerColumn) {
        return new SimpleColumn(new TableName(schema.name(), innerColumn.getParent().getName()),
                innerColumn.getName(),
                ColumnRole.Data,
                new SqlDataType(innerColumn.getColumnDataType().getJavaSqlType()),
                ColumnValueSelectionStrategy.SelectColumnValue,
                formatting);
    }

    /**
     * If the column has a type that is contained in the list of types to exclude than it will be excluded.
     * The list of types to exclude is passed when the {@link DatabaseCatalog} is constructed
     *
     * @param column
     * @return true if the column must be excluded according to its type
     */
    private boolean skipColumn(schemacrawler.schema.Column column) {
        JavaSqlType.JavaSqlTypeGroup javaSqlTypeGroup = column.getColumnDataType().getJavaSqlType().getJavaSqlTypeGroup();

        return columnTypesToExclude.contains(javaSqlTypeGroup);
    }
}
