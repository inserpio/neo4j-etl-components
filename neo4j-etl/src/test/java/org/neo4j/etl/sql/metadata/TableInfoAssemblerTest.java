package org.neo4j.etl.sql.metadata;

import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.DatabaseCatalog;
import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.StubQueryResults;
import schemacrawler.schema.*;
import schemacrawler.schema.Table;
import schemacrawler.utility.JavaSqlTypes;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TableInfoAssemblerTest {

    private final JavaSqlType varcharDataType = new JavaSqlTypes().getFromJavaSqlTypeName("VARCHAR");

    @Test
    public void shouldReturnKeyCollectionWithPrimaryKey() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder().setPrimaryKey("id").build();
        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase.Example")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);
        PrimaryKey primaryKey = mock(PrimaryKey.class);
        IndexColumn indexColumn = mock(IndexColumn.class);
        ColumnDataType columnDataType = mock(ColumnDataType.class);

        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);
        when(indexColumn.getName()).thenReturn("id");
        when(indexColumn.getColumnDataType()).thenReturn(columnDataType);
        when(primaryKey.getColumns()).thenReturn(Arrays.asList(indexColumn));
        when(table.getPrimaryKey()).thenReturn(primaryKey);

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertTrue(tableInfo.primaryKey().isPresent());
        assertTrue(tableInfo.foreignKeys().isEmpty());
        assertFalse(tableInfo.representsJoinTable());

        assertEquals("javabase.Example.id", tableInfo.primaryKey().orElseGet(() -> null).name());
    }

    @Test
    public void shouldReturnKeyCollectionWithCompositePrimaryKey() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder().setPrimaryKey("first_name", "last_name").build();
        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase.Example")));

        TableName tableName = new TableName("javabase.Example");

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        // mock schema crawler objects
        Table table = mock(Table.class);
        PrimaryKey primaryKey = mock(PrimaryKey.class);
        IndexColumn firstColumn = mock(IndexColumn.class);
        IndexColumn secondColumn = mock(IndexColumn.class);
        ColumnDataType columnDataType = mock(ColumnDataType.class);

        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        when(firstColumn.getName()).thenReturn("first_name");
        when(firstColumn.getColumnDataType()).thenReturn(columnDataType);

        when(secondColumn.getName()).thenReturn("last_name");
        when(secondColumn.getColumnDataType()).thenReturn(columnDataType);

        when(primaryKey.getColumns()).thenReturn(Arrays.asList(firstColumn, secondColumn));
        when(table.getPrimaryKey()).thenReturn(primaryKey);

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertTrue(tableInfo.primaryKey().isPresent());
        assertTrue(tableInfo.foreignKeys().isEmpty());
        assertFalse(tableInfo.representsJoinTable());

        assertEquals(join("javabase.Example.first_name", "javabase.Example.last_name"),
                tableInfo.primaryKey().orElseGet(() -> null).name());
    }

    @Test
    public void shouldReturnKeyCollectionWithTwoForeignKeys() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("author_id")
                .addForeignKey("book_id")
                .build();
        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableName tableName = new TableName("javabase.Example");

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        ForeignKey foreignKeyAuthor = mock(ForeignKey.class);
        ForeignKey foreignKeyBook = mock(ForeignKey.class);

        ForeignKeyColumnReference foreignKeyColumnReferenceAuthor = mock(ForeignKeyColumnReference.class);
        IndexColumn authorColumn = mock(IndexColumn.class);
        when(authorColumn.getName()).thenReturn("author_id");
        when(authorColumn.getColumnDataType()).thenReturn(columnDataType);
        when(authorColumn.getParent()).thenReturn(table);

        when(foreignKeyColumnReferenceAuthor.getForeignKeyColumn()).thenReturn(authorColumn);
        when(foreignKeyColumnReferenceAuthor.getPrimaryKeyColumn()).thenReturn(authorColumn);

        when(foreignKeyAuthor.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReferenceAuthor));

        ForeignKeyColumnReference foreignKeyColumnReferenceBook = mock(ForeignKeyColumnReference.class);
        IndexColumn bookColumn = mock(IndexColumn.class);
        when(bookColumn.getName()).thenReturn("book_id");
        when(bookColumn.getColumnDataType()).thenReturn(columnDataType);
        when(bookColumn.getParent()).thenReturn(table);

        when(foreignKeyColumnReferenceBook.getForeignKeyColumn()).thenReturn(bookColumn);
        when(foreignKeyColumnReferenceBook.getPrimaryKeyColumn()).thenReturn(bookColumn);

        when(foreignKeyBook.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReferenceBook));

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKeyAuthor, foreignKeyBook));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertFalse(tableInfo.primaryKey().isPresent());
        assertEquals(2, tableInfo.foreignKeys().size());
        assertTrue(tableInfo.representsJoinTable());

        assertThat(
                tableInfo.foreignKeys().stream().map(k -> k.sourceColumn().name()).collect(Collectors.toList()),
                hasItems("javabase.Example.author_id", "javabase.Example.book_id"));
    }

    @Test
    public void shouldReturnKeyCollectionWithTwoCompositeForeignKeys() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("column_1", "column_2")
                .addForeignKey("column_3", "column_4")
                .build();
        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey1 = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference1 = mock(ForeignKeyColumnReference.class);
        ForeignKeyColumnReference foreignKeyColumnReference2 = mock(ForeignKeyColumnReference.class);

        IndexColumn column1 = mock(IndexColumn.class);
        when(column1.getName()).thenReturn("column_1");
        when(column1.getColumnDataType()).thenReturn(columnDataType);
        when(column1.getParent()).thenReturn(table);

        IndexColumn column2 = mock(IndexColumn.class);
        when(column2.getName()).thenReturn("column_2");
        when(column2.getColumnDataType()).thenReturn(columnDataType);
        when(column2.getParent()).thenReturn(table);

        when(foreignKeyColumnReference1.getPrimaryKeyColumn()).thenReturn(column1);
        when(foreignKeyColumnReference1.getForeignKeyColumn()).thenReturn(column1);

        when(foreignKeyColumnReference2.getPrimaryKeyColumn()).thenReturn(column2);
        when(foreignKeyColumnReference2.getForeignKeyColumn()).thenReturn(column2);

        when(foreignKey1.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference1, foreignKeyColumnReference2));

        // Second foreign key
        ForeignKey foreignKey2 = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference3 = mock(ForeignKeyColumnReference.class);
        ForeignKeyColumnReference foreignKeyColumnReference4 = mock(ForeignKeyColumnReference.class);

        IndexColumn column3 = mock(IndexColumn.class);
        when(column3.getName()).thenReturn("column_3");
        when(column3.getColumnDataType()).thenReturn(columnDataType);
        when(column3.getParent()).thenReturn(table);

        IndexColumn column4 = mock(IndexColumn.class);
        when(column4.getName()).thenReturn("column_4");
        when(column4.getColumnDataType()).thenReturn(columnDataType);
        when(column4.getParent()).thenReturn(table);

        when(foreignKeyColumnReference3.getPrimaryKeyColumn()).thenReturn(column3);
        when(foreignKeyColumnReference3.getForeignKeyColumn()).thenReturn(column3);

        when(foreignKeyColumnReference4.getPrimaryKeyColumn()).thenReturn(column4);
        when(foreignKeyColumnReference4.getForeignKeyColumn()).thenReturn(column4);

        when(foreignKey2.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference3, foreignKeyColumnReference4));

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey1, foreignKey2));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertFalse(tableInfo.primaryKey().isPresent());
        assertEquals(2, tableInfo.foreignKeys().size());
        assertTrue(tableInfo.representsJoinTable());

        assertThat(
                tableInfo.foreignKeys().stream().map(k -> k.sourceColumn().name()).collect(Collectors.toList()),
                hasItems(join("javabase.Example.column_1", "javabase.Example.column_2"),
                        join("javabase.Example.column_3", "javabase.Example.column_4")));
    }

    @Test
    public void shouldCreateSyntheticPrimaryKeyForTableWithLessThanTwoForeignKeysAndNoPrimaryKey() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("fk")
                .build();

        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference = mock(ForeignKeyColumnReference.class);

        IndexColumn column1 = mock(IndexColumn.class);
        when(column1.getName()).thenReturn("fk");
        when(column1.getColumnDataType()).thenReturn(columnDataType);
        when(column1.getParent()).thenReturn(table);

        when(foreignKeyColumnReference.getPrimaryKeyColumn()).thenReturn(column1);
        when(foreignKeyColumnReference.getForeignKeyColumn()).thenReturn(column1);
        when(foreignKey.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference));

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertTrue(tableInfo.primaryKey().isPresent());
        assertEquals(1, tableInfo.foreignKeys().size());
        assertFalse(tableInfo.representsJoinTable());

        assertEquals(TableInfoAssembler.SYNTHETIC_PRIMARY_KEY_NAME, tableInfo.primaryKey().get().alias());
        assertFalse(tableInfo.primaryKey().get().allowAddToSelectStatement());
    }

    @Test
    public void shouldCreateSyntheticPrimaryKeyForTableWithMoreThanTwoForeignKeysAndNoPrimaryKey() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("fk_1")
                .addForeignKey("fk_2")
                .addForeignKey("fk_3")
                .build();

        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey1 = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference1 = mock(ForeignKeyColumnReference.class);

        IndexColumn column1 = mock(IndexColumn.class);
        when(column1.getName()).thenReturn("fk_1");
        when(column1.getColumnDataType()).thenReturn(columnDataType);
        when(column1.getParent()).thenReturn(table);

        when(foreignKeyColumnReference1.getPrimaryKeyColumn()).thenReturn(column1);
        when(foreignKeyColumnReference1.getForeignKeyColumn()).thenReturn(column1);
        when(foreignKey1.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference1));

        // Second foreign key
        ForeignKey foreignKey2 = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference2 = mock(ForeignKeyColumnReference.class);

        IndexColumn column2 = mock(IndexColumn.class);
        when(column2.getName()).thenReturn("fk_2");
        when(column2.getColumnDataType()).thenReturn(columnDataType);
        when(column2.getParent()).thenReturn(table);

        when(foreignKeyColumnReference2.getPrimaryKeyColumn()).thenReturn(column2);
        when(foreignKeyColumnReference2.getForeignKeyColumn()).thenReturn(column2);
        when(foreignKey2.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference2));

        // Third foreign key
        ForeignKey foreignKey3 = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference3 = mock(ForeignKeyColumnReference.class);

        IndexColumn column3 = mock(IndexColumn.class);
        when(column3.getName()).thenReturn("fk_3");
        when(column3.getColumnDataType()).thenReturn(columnDataType);
        when(column3.getParent()).thenReturn(table);

        when(foreignKeyColumnReference3.getPrimaryKeyColumn()).thenReturn(column3);
        when(foreignKeyColumnReference3.getForeignKeyColumn()).thenReturn(column3);
        when(foreignKey3.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference3));

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey1, foreignKey2, foreignKey3));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);


        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertTrue(tableInfo.primaryKey().isPresent());
        assertEquals(3, tableInfo.foreignKeys().size());
        assertFalse(tableInfo.representsJoinTable());

        assertEquals(TableInfoAssembler.SYNTHETIC_PRIMARY_KEY_NAME, tableInfo.primaryKey().get().alias());
        assertFalse(tableInfo.primaryKey().get().allowAddToSelectStatement());
    }

    @Test
    public void shouldNotCreateSyntheticPrimaryKeyForJoinTableWithTwoForeignKeysAndNoPrimaryKey() throws Exception {
        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("fk_1")
                .addForeignKey("fk_2")
                .build();

        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey1 = createForeignKeyMock(table, 1);

        // Second foreign key
        ForeignKey foreignKey2 = createForeignKeyMock(table, 2);

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey1, foreignKey2));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);


        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertFalse(tableInfo.primaryKey().isPresent());
        assertEquals(2, tableInfo.foreignKeys().size());
        assertTrue(tableInfo.representsJoinTable());
    }

    @Test
    public void shouldCreateCompositePrimaryKeyFromForeignKeysEvenWhenTargetTableIsExcluded() throws Exception {
        List<String> tablesToExclude = new ArrayList<String>();
        tablesToExclude.add("Example1");

        // given
        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("fk_1")
                .addForeignKey("fk_2")
                .setPrimaryKey("fk_1, fk_2")
                .build();

        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey1 = createForeignKeyMock(table, 1);
        // Second foreign key
        ForeignKey foreignKey2 = createForeignKeyMock(table, 2);

        PrimaryKey primaryKey = mock(PrimaryKey.class);
        IndexColumn primaryKeyColumn = mock(IndexColumn.class);
        when(primaryKeyColumn.getName()).thenReturn("fk_1, fk_2");
        when(primaryKeyColumn.getColumnDataType()).thenReturn(columnDataType);

        when(primaryKey.getColumns()).thenReturn(Arrays.asList(primaryKeyColumn));
        when(table.getPrimaryKey()).thenReturn(primaryKey);

        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey1, foreignKey2));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);


        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertTrue(tableInfo.primaryKey().isPresent());
        assertEquals("javabase.Example.fk_1, fk_2", tableInfo.primaryKey().get().name());
    }

    // TODO fix table exclusion correctly with unit tests (due to schema crawler)
    @Test
    @Ignore
    public void shouldNotAddThirdForeignKeyAndRepresentJoinTableWhenTargetTableIsExcluded() throws Exception {
        // given
        List<String> tablesToExclude = new ArrayList<String>();
        tablesToExclude.add("Example1");

        DatabaseClient databaseClient = new DatabaseClientBuilder()
                .addForeignKey("fk_1")
                .addForeignKey("fk_2")
                .addForeignKey("fk_3")
                .build();

        DatabaseCatalog databaseCatalog = spy(new DatabaseCatalog(databaseClient, new Schema("javabase")));

        TableInfoAssembler assembler = new TableInfoAssembler(databaseCatalog);

        TableName tableName = new TableName("javabase.Example");

        // mock schema crawler objects
        Table table = mock(Table.class);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        // First foreign key
        ForeignKey foreignKey1 = createForeignKeyMock(table, 1);
        // Second foreign key
        ForeignKey foreignKey2 = createForeignKeyMock(table, 2);
        // Third foreign key
        ForeignKey foreignKey3 = createForeignKeyMock(table, 3);

        when(table.getPrimaryKey()).thenReturn(null);
        when(table.getFullName()).thenReturn(tableName.fullName());
        when(table.getName()).thenReturn("Example");
        when(table.getForeignKeys()).thenReturn(Arrays.asList(foreignKey1, foreignKey2, foreignKey3));

        doReturn(table).when(databaseCatalog).lookupTableByTableName(tableName);
        doCallRealMethod().when(databaseCatalog).getPrimaryKey(tableName);

        // when
        TableInfo tableInfo = assembler.createTableInfo(tableName);

        // then
        assertEquals(2, tableInfo.foreignKeys().size());
        assertTrue(tableInfo.representsJoinTable());
    }

    private ForeignKey createForeignKeyMock(Table table, int i) {
        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getJavaSqlType()).thenReturn(varcharDataType);

        ForeignKey foreignKey = mock(ForeignKey.class);
        ForeignKeyColumnReference foreignKeyColumnReference = mock(ForeignKeyColumnReference.class);

        IndexColumn targetColumn = mock(IndexColumn.class);
        when(targetColumn.getName()).thenReturn("fk_" + i);
        when(targetColumn.getColumnDataType()).thenReturn(columnDataType);

        Table targetTable = mock(Table.class);
        when(targetTable.getName()).thenReturn("Example");
        when(targetTable.getFullName()).thenReturn("javabase.Example");
        when(targetColumn.getParent()).thenReturn(targetTable);

        IndexColumn primaryColumn = mock(IndexColumn.class);
        when(primaryColumn.getName()).thenReturn("fk_" + i);
        when(primaryColumn.getColumnDataType()).thenReturn(columnDataType);
        when(primaryColumn.getParent()).thenReturn(table);

        when(foreignKeyColumnReference.getPrimaryKeyColumn()).thenReturn(primaryColumn);
        when(foreignKeyColumnReference.getForeignKeyColumn()).thenReturn(targetColumn);
        when(foreignKey.getColumnReferences()).thenReturn(Arrays.asList(foreignKeyColumnReference));

        return foreignKey;
    }

    private String join(String... columns) {
        return StringUtils.join(columns, CompositeColumn.SEPARATOR);
    }

    private static class DatabaseClientBuilder {
        private final DatabaseClient databaseClient = mock(DatabaseClient.class);
        private final Collection<String> columns = new HashSet<>();
        private final List<String> primaryKey = new ArrayList<>();
        private final List<List<String>> foreignKeys = new ArrayList<>();

        DatabaseClientBuilder setPrimaryKey(String... primaryKeyElement) {
            List<String> elements = asList(primaryKeyElement);

            this.columns.addAll(elements);
            this.primaryKey.addAll(elements);

            return this;
        }

        DatabaseClientBuilder addForeignKey(String... foreignKeyElement) {
            List<String> elements = asList(foreignKeyElement);

            this.columns.addAll(elements);
            this.foreignKeys.add(elements);

            return this;
        }

        public DatabaseClient build() throws Exception {
            StubQueryResults.Builder columnsResults = StubQueryResults.builder().columns("COLUMN_NAME", "TYPE_NAME");
            columns.forEach((c) -> columnsResults.addRow(c, "INT"));

            StubQueryResults.Builder primaryKeyResults = StubQueryResults.builder().columns("COLUMN_NAME");
            primaryKey.forEach((rows) -> primaryKeyResults.addRow(rows));

            StubQueryResults.Builder foreignKeyResults = StubQueryResults.builder()
                    .columns("FK_NAME", "FKCOLUMN_NAME", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME");

            for (int i = 0; i < foreignKeys.size(); i++) {
                final int elementNumber = i + 1;
                String fkName = format("fk_%s", elementNumber);
                List<String> foreignKeyElements = foreignKeys.get(i);
                foreignKeyElements.forEach(fk -> foreignKeyResults.addRow(fkName, fk, "javabase", "Example" + elementNumber, "id"));
            }

            //when(databaseClient.columns(any())).thenReturn(columnsResults.build());
            //when(databaseClient.primaryKeys(any())).thenReturn(primaryKeyResults.build());
            //when(databaseClient.foreignKeys(any())).thenReturn(foreignKeyResults.build());

            return databaseClient;
        }
    }
}
