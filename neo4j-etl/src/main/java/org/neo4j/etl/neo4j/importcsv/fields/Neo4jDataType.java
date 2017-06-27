package org.neo4j.etl.neo4j.importcsv.fields;

public enum Neo4jDataType
{
    Boolean( false ),
    Long( false ),
    Double( false ),
    Byte( false ),
    String( true );

    private boolean useQuotes;

    Neo4jDataType( boolean useQuotes )
    {
        this.useQuotes = useQuotes;
    }

    public String value()
    {
        return name().toLowerCase();
    }

    @Override
    public String toString()
    {
        return value();
    }

    public boolean shouldUseQuotes()
    {
        return useQuotes;
    }
}
