package org.neo4j.etl.sql.metadata;

import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;

import static java.lang.String.format;

public enum SqlDataType
{
    BIT( Neo4jDataType.Byte ),
    INT( Neo4jDataType.Int ),
    INT_UNSIGNED( Neo4jDataType.Int ),
    TINYINT( Neo4jDataType.Byte ),
    TINYINT_UNSIGNED( Neo4jDataType.Byte ),
    SMALLINT( Neo4jDataType.Short ),
    SMALLINT_UNSIGNED( Neo4jDataType.Short ),
    BIGINT( Neo4jDataType.Long ),
    BIGINT_UNSIGNED( Neo4jDataType.Long ),
    FLOAT( Neo4jDataType.Float ),
    DOUBLE( Neo4jDataType.Double ),
    DECIMAL( Neo4jDataType.Float ),
    MEDIUMINT( Neo4jDataType.Int ),
    MEDIUMINT_UNSIGNED( Neo4jDataType.Int ),

    CHAR( Neo4jDataType.String ),
    VARCHAR( Neo4jDataType.String ),
    TEXT( Neo4jDataType.String ),
    TINYTEXT( Neo4jDataType.String ),
    MEDIUMTEXT( Neo4jDataType.String ),
    LONGTEXT( Neo4jDataType.String ),
    ENUM( Neo4jDataType.String ),

    DATE( Neo4jDataType.String ),
    DATETIME( Neo4jDataType.String ),
    TIMESTAMP( Neo4jDataType.String ),
    TIME( Neo4jDataType.String ),
    YEAR( Neo4jDataType.String ),

    BLOB( null ),
    TINYBLOB( null ),
    MEDIUMBLOB( null ),
    LONGBLOB( null );

    public static final SqlDataType COMPOSITE_KEY_TYPE = TEXT;
    public static final SqlDataType LABEL_DATA_TYPE = TEXT;
    public static final SqlDataType RELATIONSHIP_TYPE_DATA_TYPE = TEXT;
    public static final SqlDataType KEY_DATA_TYPE = TEXT;

    public static SqlDataType parse( String dataType )
    {
        try
        {
            return SqlDataType.valueOf( dataType.replaceAll(" ", "_").toUpperCase() );
        }
        catch ( NullPointerException e )
        {
            throw new IllegalArgumentException( format( "Unrecognized SQL data type: %s", dataType ) );
        }
    }

    private Neo4jDataType neo4jDataType;

    SqlDataType( Neo4jDataType neo4jDataType )
    {
        this.neo4jDataType = neo4jDataType;
    }

    /*You need to handle the tinyInt scenario always transform from TinyIntResolver*/
    public Neo4jDataType toNeo4jDataType()
    {
        return neo4jDataType;
    }

    public boolean skipImport()
    {
        return BLOB == this || TINYBLOB == this || MEDIUMBLOB == this || LONGBLOB == this;
    }
}
