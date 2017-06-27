package org.neo4j.etl.sql.metadata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import schemacrawler.schema.JavaSqlType;
import schemacrawler.utility.JavaSqlTypes;

import static java.lang.String.format;

public class SqlDataType {
    /*BLOB(null),
    TINYBLOB(null),
    MEDIUMBLOB(null),
    LONGBLOB(null),
    BYTEA(null),*/

    public static final SqlDataType TEXT = SqlDataType.parse("VARCHAR");

    public static final SqlDataType INT = SqlDataType.parse("INTEGER");

    public static final SqlDataType TINYINT = SqlDataType.parse("TINYINT");

    private Neo4jDataType neo4jDataType;

    private JavaSqlType javaSqlType;

    /**
     * Creates a SQL data type from a SQL java type.
     * It automatically infers Neo4j data type
     *
     * @param javaSqlType
     */
    public SqlDataType(JavaSqlType javaSqlType) {
        this.javaSqlType = javaSqlType;
        this.neo4jDataType = inferNeo4jDataType(javaSqlType);
    }

    public static SqlDataType parse(String dataType) {
        try {
            return new SqlDataType(new JavaSqlTypes().getFromJavaSqlTypeName(dataType.toUpperCase()));
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(format("Unrecognized SQL data type: %s", dataType));
        }
    }

    public String name() {
        return this.javaSqlType.getJavaSqlTypeName();
    }

    /**
     * Retrieves Neo4j data type from the Java Sql type
     *
     * @param javaSqlType
     * @return a data type supported by Neo4j
     */
    private Neo4jDataType inferNeo4jDataType(JavaSqlType javaSqlType) {
        JavaSqlType.JavaSqlTypeGroup javaSqlTypeGroup = javaSqlType.getJavaSqlTypeGroup();

        switch (javaSqlTypeGroup) {
            case integer:
                // TODO: this should be converted according to TinyIntResolver but TinyInt is not valid for all DBs
                /*if (javaSqlType.getJavaSqlTypeName().equals("TINYINT")) {
                    return Neo4jDataType.Byte;
                }*/
                return Neo4jDataType.Long;
            case bit:
                // TODO: probably this should be converted to a string
                return Neo4jDataType.Byte;
            case real:
                return Neo4jDataType.Double;
            case character:
            case temporal:
            case url:
            case xml:
                return Neo4jDataType.String;
            // TODO: probably this should be skipped or translated into a string representation
            case binary:
                return Neo4jDataType.Boolean;
            default:
                return null;
            // TODO: add "id" and "reference" Java SQL type

        }
    }

    /*You need to handle the tinyInt scenario always transform from TinyIntResolver*/
    public Neo4jDataType toNeo4jDataType() {
        return neo4jDataType;
    }

    /**
     * Check if SqlTypeData is a binary data, for example a BLOB
     *
     * @return a boolean, true if it's a byte data
     */
    public boolean isBinaryObject() {
        JavaSqlType.JavaSqlTypeGroup javaSqlTypeGroup = this.javaSqlType.getJavaSqlTypeGroup();

        return javaSqlTypeGroup.equals(JavaSqlType.JavaSqlTypeGroup.object) ||
                javaSqlTypeGroup.equals(JavaSqlType.JavaSqlTypeGroup.large_object);
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }
}
