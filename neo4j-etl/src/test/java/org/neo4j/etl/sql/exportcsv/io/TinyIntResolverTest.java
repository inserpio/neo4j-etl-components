package org.neo4j.etl.sql.exportcsv.io;

import org.junit.Test;

import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import org.neo4j.etl.sql.exportcsv.mapping.TinyIntAs;
import org.neo4j.etl.sql.metadata.SqlDataType;

import static org.junit.Assert.assertEquals;

public class TinyIntResolverTest
{
    private static final TinyIntResolver byteResolver = new TinyIntResolver( TinyIntAs.BYTE );
    private static final TinyIntResolver booleanResolver = new TinyIntResolver( TinyIntAs.BOOLEAN );

    @Test
    public void byteResolverShouldNotTransformToBoolean() throws Exception
    {
        assertEquals( "1", byteResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.INT ) );
        assertEquals( "1", byteResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.TEXT ) );
        assertEquals( "1", byteResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.TINYINT ) );
        assertEquals( "0", byteResolver.handleSpecialCaseForTinyInt( "0", SqlDataType.TINYINT ) );
    }

    @Test
    public void booleanResolverShouldTransformTinyIntToBoolean() throws Exception
    {
        assertEquals( "1", booleanResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.INT ) );
        assertEquals( "1", booleanResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.TEXT ) );
        assertEquals( "true", booleanResolver.handleSpecialCaseForTinyInt( "1", SqlDataType.TINYINT ) );
        assertEquals( "false", booleanResolver.handleSpecialCaseForTinyInt( "0", SqlDataType.TINYINT ) );
    }

    @Test
    public void byteResolverShouldReturnTargetDataTypeAsIs() throws Exception
    {
        assertEquals( Neo4jDataType.Long, byteResolver.targetDataType( SqlDataType.INT ) );
        assertEquals( Neo4jDataType.String, byteResolver.targetDataType( SqlDataType.TEXT ) );
        assertEquals( Neo4jDataType.Long, byteResolver.targetDataType( SqlDataType.TINYINT ) );
    }

    @Test
    public void booleanResolverShouldReturnTargetDataTypeAsBooleanForTinyInt() throws Exception
    {
        assertEquals( Neo4jDataType.Long, booleanResolver.targetDataType( SqlDataType.INT ) );
        assertEquals( Neo4jDataType.String, booleanResolver.targetDataType( SqlDataType.TEXT ) );
        assertEquals( Neo4jDataType.Boolean, booleanResolver.targetDataType( SqlDataType.TINYINT ) );
    }
}
