package org.neo4j.etl.sql;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.etl.io.AwaitHandle;
import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;
import org.neo4j.etl.sql.exportcsv.formatting.SqlQuotes;
import org.neo4j.etl.sql.metadata.Schema;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.FutureUtils;
import org.neo4j.etl.util.Loggers;
import schemacrawler.schema.Catalog;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.utility.SchemaCrawlerUtility;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

public class DatabaseClient implements AutoCloseable {
    private final Connection connection;
    private final DatabaseMetaData metaData;
    private final StatementFactory statementFactory;

    private final String schema;

    private final SqlQuotes databaseQuotes;

    public DatabaseClient(ConnectionConfig connectionConfig) throws SQLException, ClassNotFoundException {
        Loggers.Sql.log().fine("Connecting to database...");

        // For recent JDBC driver this piece of code is useless
        // For now it remains as is for back compatibility

        // We iterate through all available drivers and we check if one of them is suitable for the specified connection
        boolean driverExistsForConnectionUrl = false;
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();

            if (driver.acceptsURL(connectionConfig.uri().toString())) {
                driverExistsForConnectionUrl = true;
                break;
            }
        }

        if (!driverExistsForConnectionUrl) {
            String message = "There is no suitable driver for the connection URL: " + connectionConfig.uri().toString();
            Loggers.Sql.log(Level.SEVERE, message);
            throw new ClassNotFoundException(message);
        }

        try {
            connection = DriverManager.getConnection(
                    connectionConfig.uri().toString(),
                    connectionConfig.credentials().username(),
                    connectionConfig.credentials().password());
        } catch (SQLException e) {
            Loggers.Sql.log(Level.SEVERE, "Could not connect to the host database. Please check your credentials ", e);
            throw e;
        }

        metaData = connection.getMetaData();
        statementFactory = connectionConfig.statementFactory();

        //schema = hasSchemas ? connection.getSchema() : connection.getCatalog();
        // TODO: maybe we can use other info from Metadata to "normalize" the concept of schema (or catalog)
        schema = !StringUtils.isEmpty(connection.getSchema()) ? connection.getSchema() : connection.getCatalog();

        // Retrieve quotes from metadata
        databaseQuotes = getDatabaseSpecificQuotes(metaData);

        Loggers.Sql.log().fine("Connected to database");
    }

    /**
     * Creates a {@link SqlQuotes} object representing the database quote character and the catalog separator character
     *
     * @param metaData database metadata object according to JDBC standards
     * @return
     * @throws SQLException
     */
    private SqlQuotes getDatabaseSpecificQuotes(DatabaseMetaData metaData) throws SQLException {
        QuoteChar databaseObjectQuoteChar = new QuoteChar(metaData.getIdentifierQuoteString(), metaData.getIdentifierQuoteString());
        String separator = StringUtils.defaultIfBlank(metaData.getCatalogSeparator(), ".");
        QuoteChar catalogSeparator = new QuoteChar(separator, separator);

        return new SqlQuotes(databaseObjectQuoteChar, catalogSeparator);
    }

    /**
     * Gets the {@link SqlQuotes} object
     *
     * @return
     */
    // TODO: public?
    public SqlQuotes getQuotes() {
        return this.databaseQuotes;
    }

    /**
     * Get the Catalog
     * This method accept {@link SchemaCrawlerOptions} to set up the schema crawler
     * It was designed as is in order to not expose the connection object externally
     *
     * @return
     */
    Catalog getCatalog(final SchemaCrawlerOptions schemaCrawlerOptions) throws SQLException {
        try {
            return SchemaCrawlerUtility.getCatalog(connection, schemaCrawlerOptions);
        } catch (SchemaCrawlerException e) {
            // Wrapping the internal exception as an SQLException
            throw new SQLException(e.getMessage(), e);
        }
    }

    /**
     * Get the current schema to be analyzed
     * Schema and Catalog are concept strictly related to the database we are using so they stay here in the DB client
     *
     * @return
     */
    Schema getSchema() {
        return new Schema(this.schema);
    }

    @Deprecated
    public QueryResults primaryKeys(TableName tableName) throws SQLException {
        return new SqlQueryResults(metaData.getPrimaryKeys("", tableName.schema(), tableName.simpleName()));
    }

    @Deprecated
    public QueryResults foreignKeys(TableName tableName) throws SQLException {
        return new SqlQueryResults(metaData.getImportedKeys("", tableName.schema(), tableName.simpleName()));
    }

    @Deprecated
    public QueryResults columns(TableName tableName) throws SQLException {
        return new SqlQueryResults(metaData.getColumns("", tableName.schema(), tableName.simpleName(), null));
    }

    @Deprecated
    public Collection<TableName> tables(Schema schema) throws SQLException {
        Collection<TableName> tableNames = new ArrayList<>();

        String tableSchema;

        if (schema != null && schema != Schema.UNDEFINED) {
            tableSchema = StringUtils.upperCase(schema.name());
        } else {
            tableSchema = this.schema;
        }

        try (ResultSet results = connection.getMetaData().getTables(null, tableSchema, null, new String[]{"TABLE"})) {
            while (results.next()) {
                tableNames.add(new TableName(tableSchema, results.getString("TABLE_NAME")));
            }
        }

        return tableNames;
    }

    public AwaitHandle<QueryResults> executeQuery(String sql) {
        return new DatabaseClientAwaitHandle<>(
                FutureUtils.exceptionableFuture(() ->
                {
                    Loggers.Sql.log().finest(sql);
                    connection.setAutoCommit(false);
                    return new SqlQueryResults(statementFactory.createStatement(connection).executeQuery(sql));

                }, r -> new Thread(r).start()));
    }

    public AwaitHandle<Boolean> execute(String sql) {
        return new DatabaseClientAwaitHandle<>(
                FutureUtils.exceptionableFuture(() ->
                {
                    Loggers.Sql.log().finest(sql);
                    connection.setAutoCommit(true);
                    return connection.prepareStatement(sql).execute();

                }, r -> new Thread(r).start()));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    interface StatementFactory {
        Statement createStatement(Connection connection) throws SQLException;
    }

    private static class DatabaseClientAwaitHandle<T> implements AwaitHandle<T> {
        private final CompletableFuture<T> future;

        private DatabaseClientAwaitHandle(CompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public T await() throws Exception {
            return future.get();
        }

        @Override
        public T await(long timeout, TimeUnit unit) throws Exception {
            return future.get(timeout, unit);
        }

        @Override
        public CompletableFuture<T> toFuture() {
            return future;
        }
    }

    private static class SqlQueryResults implements QueryResults {
        private final ResultSet results;

        SqlQueryResults(ResultSet results) {
            this.results = results;
        }

        @Override
        public boolean next() throws Exception {
            return results.next();
        }

        @Override
        public Stream<Map<String, String>> stream() {
            Collection<String> columnLabels = new ArrayList<>();

            try {
                ResultSetMetaData metaData = results.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    columnLabels.add(metaData.getColumnLabel(i));
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Error while getting column labels from SQL result set", e);
            }

            return StreamSupport.stream(new ResultSetSpliterator(results, columnLabels), false);
        }

        @Override
        public String getString(String columnLabel) {
            try {
                return results.getString(columnLabel);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            results.close();
        }

        private static class ResultSetSpliterator implements Spliterator<Map<String, String>> {
            private final ResultSet results;
            private final Collection<String> columnLabels;

            ResultSetSpliterator(ResultSet results, Collection<String> columnLabels) {
                this.results = results;
                this.columnLabels = columnLabels;
            }

            @Override
            public boolean tryAdvance(Consumer<? super Map<String, String>> action) {
                boolean hasNext;
                try {
                    hasNext = results.next();
                } catch (SQLException e) {
                    throw new IllegalStateException("Error while iterating SQL result set", e);
                }
                if (hasNext) {
                    Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (String columnLabel : columnLabels) {
                        try {
                            map.put(columnLabel, results.getString(columnLabel));
                        } catch (SQLException e) {
                            throw new IllegalStateException(
                                    format("Error while accessing '%s' in SQL result set", columnLabel), e);
                        }

                    }
                    action.accept(map);
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public Spliterator<Map<String, String>> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return 0;
            }

            @Override
            public int characteristics() {
                return 0;
            }
        }
    }
}
