package org.neo4j.etl.sql.metadata;

import org.junit.Test;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SqlDataTypeTest {
    @Test
    public void parseShouldUpperCaseDataTypesToMapToRdbmsDataType() throws Exception {
        // given
        SqlDataType anInt = SqlDataType.parse("INTEGER");

        // then
        assertThat(anInt, is(SqlDataType.INT));
    }

    @Test
    public void toNeo4jDataTypeMappingOfNumericTypes() throws Exception {
        assertThat(SqlDataType.INT.toNeo4jDataType(), is(Neo4jDataType.Long));
        assertThat(SqlDataType.TINYINT.toNeo4jDataType(), is(Neo4jDataType.Long));
        assertThat(SqlDataType.parse("SMALLINT").toNeo4jDataType(), is(Neo4jDataType.Long));
        assertThat(SqlDataType.parse("BIGINT").toNeo4jDataType(), is(Neo4jDataType.Long));
        assertThat(SqlDataType.parse("DOUBLE").toNeo4jDataType(), is(Neo4jDataType.Double));
        assertThat(SqlDataType.parse("FLOAT").toNeo4jDataType(), is(Neo4jDataType.Double));
        assertThat(SqlDataType.parse("DECIMAL").toNeo4jDataType(), is(Neo4jDataType.Double));
    }

    @Test
    public void toNeo4jDataTypeMappingOfStringTypes() throws Exception {
        assertThat(SqlDataType.parse("CHAR").toNeo4jDataType(), is(Neo4jDataType.String));
        assertThat(SqlDataType.parse("VARCHAR").toNeo4jDataType(), is(Neo4jDataType.String));
    }

    @Test
    public void toNeo4jDataTypeMappingOfBlobTypesShoulReturnNull() throws Exception {
        assertNull(SqlDataType.parse("BLOB").toNeo4jDataType());
        assertNull(SqlDataType.parse("TINYBLOB").toNeo4jDataType());
        assertNull(SqlDataType.parse("MEDIUMBLOB").toNeo4jDataType());
        assertNull(SqlDataType.parse("LONGBLOB").toNeo4jDataType());
    }

    @Test
    public void toNeo4jDataTypeMappingOfDateTypes() throws Exception {
        assertThat(SqlDataType.parse("DATE").toNeo4jDataType(), is(Neo4jDataType.String));
        assertThat(SqlDataType.parse("TIME").toNeo4jDataType(), is(Neo4jDataType.String));
    }

    @Test
    public void toNeo4jDataTypeMappingOfBit() throws Exception {
        assertThat(SqlDataType.parse("BIT").toNeo4jDataType(), is(Neo4jDataType.Byte));
    }

    @Test
    public void testParse() throws Exception {
        SqlDataType sqlDataType = SqlDataType.parse("INTEGER");
        assertThat(sqlDataType, is(SqlDataType.INT));
    }
}
