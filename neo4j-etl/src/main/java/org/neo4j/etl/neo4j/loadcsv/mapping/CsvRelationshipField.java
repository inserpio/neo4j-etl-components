package org.neo4j.etl.neo4j.loadcsv.mapping;

import org.neo4j.etl.neo4j.importcsv.config.formatting.DefaultPropertyFormatter;
import org.neo4j.etl.neo4j.importcsv.fields.Neo4jDataType;
import org.neo4j.etl.sql.metadata.ColumnRole;

public class CsvRelationshipField
{
    private String name;
    private String columnType;
    private Neo4jDataType type;
    private boolean isArray;

    private String column;
    private int position;
    private ColumnRole role;
    private String space;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = new DefaultPropertyFormatter().format( name );
    }

    public String getColumnType()
    {
        return columnType;
    }

    public void setColumnType(String columnType)
    {
        this.columnType = columnType;
    }

    public Neo4jDataType getType() {
        return type;
    }

    public void setType(Neo4jDataType type) {
        this.type = type;
    }

    public boolean isArray() {
        return isArray;
    }

    public void setArray(boolean array) {
        isArray = array;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public ColumnRole getRole() {
        return role;
    }

    public void setRole(ColumnRole role) {
        this.role = role;
    }

    public String getSpace() {
        return space;
    }

    public void setSpace(String space) {
        this.space = space;
    }

    @Override
    public String toString() {
        return "CsvRelationshipField{" +
                "name='" + name + '\'' +
                ", columnType='" + columnType + '\'' +
                ", type=" + type +
                ", isArray=" + isArray +
                ", column='" + column + '\'' +
                ", position=" + position +
                ", role=" + role +
                ", space='" + space + '\'' +
                '}';
    }
}
