package org.neo4j.etl.sql.metadata;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SqlDataTypeTest
{
    @Test
    public void parseShouldUpperCaseDataTypesToMapToMySqlDataType() throws Exception
    {
        // given
        SqlDataType anInt = SqlDataType.parse( "int" );

        // then
        assertThat( anInt, is( SqlDataType.INT ) );
    }

    @Test
    public void toNeo4jDataTypeMappingOfNumericTypes() throws Exception
    {
        assertThat( SqlDataType.INT.toNeo4jDataType(), is( Neo4jDataType.Int ) );
        assertThat( SqlDataType.INT_UNSIGNED.toNeo4jDataType(), is( Neo4jDataType.Int ) );
        assertThat( SqlDataType.TINYINT.toNeo4jDataType(), is( Neo4jDataType.Byte ) );
        assertThat( SqlDataType.TINYINT_UNSIGNED.toNeo4jDataType(), is( Neo4jDataType.Byte ) );
        assertThat( SqlDataType.SMALLINT.toNeo4jDataType(), is( Neo4jDataType.Short ) );
        assertThat( SqlDataType.SMALLINT_UNSIGNED.toNeo4jDataType(), is( Neo4jDataType.Short ) );
        assertThat( SqlDataType.MEDIUMINT.toNeo4jDataType(), is( Neo4jDataType.Int ) );
        assertThat( SqlDataType.MEDIUMINT_UNSIGNED.toNeo4jDataType(), is( Neo4jDataType.Int ) );
        assertThat( SqlDataType.BIGINT.toNeo4jDataType(), is( Neo4jDataType.Long ) );
        assertThat( SqlDataType.BIGINT_UNSIGNED.toNeo4jDataType(), is( Neo4jDataType.Long ) );
        assertThat( SqlDataType.FLOAT.toNeo4jDataType(), is( Neo4jDataType.Float ) );
        assertThat( SqlDataType.DOUBLE.toNeo4jDataType(), is( Neo4jDataType.Double ) );
        assertThat( SqlDataType.DECIMAL.toNeo4jDataType(), is( Neo4jDataType.Float ) );
    }

    @Test
    public void toNeo4jDataTypeMappingOfStringTypes() throws Exception
    {
        assertThat( SqlDataType.CHAR.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.VARCHAR.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.MEDIUMTEXT.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.LONGTEXT.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.ENUM.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.TINYTEXT.toNeo4jDataType(), is( Neo4jDataType.String ) );
    }

    @Test
    public void toNeo4jDataTypeMappingOfBlobTypesShoulReturnNull() throws Exception
    {
        assertNull( SqlDataType.BLOB.toNeo4jDataType() );
        assertNull( SqlDataType.TINYBLOB.toNeo4jDataType() );
        assertNull( SqlDataType.MEDIUMBLOB.toNeo4jDataType() );
        assertNull( SqlDataType.LONGBLOB.toNeo4jDataType() );
    }

    @Test
    public void blobDataTypesShouldReturnTrueIfAskedAboutSupportedData() throws Exception
    {
        assertTrue( SqlDataType.BLOB.skipImport() );
        assertTrue( SqlDataType.TINYBLOB.skipImport() );
        assertTrue( SqlDataType.MEDIUMBLOB.skipImport() );
        assertTrue( SqlDataType.LONGBLOB.skipImport() );

        assertFalse( SqlDataType.INT.skipImport() );
    }

    @Test
    public void toNeo4jDataTypeMappingOfDateTypes() throws Exception
    {
        assertThat( SqlDataType.DATE.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.DATETIME.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.TIMESTAMP.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.TIME.toNeo4jDataType(), is( Neo4jDataType.String ) );
        assertThat( SqlDataType.YEAR.toNeo4jDataType(), is( Neo4jDataType.String ) );
    }

    @Test
    public void toNeo4jDataTypeMappingOfBit() throws Exception
    {
        assertThat( SqlDataType.BIT.toNeo4jDataType(), is( Neo4jDataType.Byte ) );
    }

    @Test
    public void testParse() throws Exception
    {
        SqlDataType sqlDataType = SqlDataType.parse( "float" );
        assertThat( sqlDataType, is( sqlDataType.FLOAT ) );
    }
}
