package org.neo4j.etl.sql.metadata;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * @author inserpio
 * @since 28/04/17
 */
public class Schema {
    public static final Schema UNDEFINED = new Schema();

    private final String name;

    protected Schema() {
        this.name = null;
    }

    ;

    public Schema(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    @Override
    public boolean equals( Object o ) {
        return EqualsBuilder.reflectionEquals( this, o );
    }
}
