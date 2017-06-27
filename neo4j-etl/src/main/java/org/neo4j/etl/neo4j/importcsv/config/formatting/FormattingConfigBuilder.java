package org.neo4j.etl.neo4j.importcsv.config.formatting;

import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.exportcsv.formatting.SqlQuotes;

class FormattingConfigBuilder implements Formatting.Builder
{
    Delimiter delimiter = Delimiter.COMMA;
    Delimiter arrayDelimiter = Delimiter.SEMICOLON;
    QuoteChar quote = QuoteChar.DOUBLE_QUOTES;
    Formatter labelFormatter = new DefaultLabelFormatter();
    Formatter relationshipFormatter = new DefaultRelationshipFormatter();
    Formatter propertyFormatter = new DefaultPropertyFormatter();
    SqlQuotes sqlQuotes = SqlQuotes.DEFAULT;

    @Override
    public Formatting.Builder delimiter( Delimiter delimiter )
    {
        this.delimiter = delimiter;
        return this;
    }

    @Override
    public Formatting.Builder arrayDelimiter( Delimiter delimiter )
    {
        this.arrayDelimiter = delimiter;
        return this;
    }

    @Override
    public Formatting.Builder quote( QuoteChar quote )
    {
        this.quote = quote;
        return this;
    }

    @Override
    public Formatting.Builder labelFormatter( Formatter labelFormatter )
    {
        this.labelFormatter = labelFormatter;
        return this;
    }

    @Override
    public Formatting.Builder relationshipFormatter( Formatter relationshipFormatter )
    {
        this.relationshipFormatter = relationshipFormatter;
        return this;
    }

    @Override
    public Formatting.Builder propertyFormatter( Formatter propertyFormatter )
    {
        this.propertyFormatter = propertyFormatter;
        return this;
    }

    @Override
    public Formatting.Builder databaseClient(DatabaseClient databaseClient){
        this.sqlQuotes = databaseClient.getQuotes();
        return this;
    }

    @Override
    public Formatting build()
    {
        return new Formatting( this );
    }
}
