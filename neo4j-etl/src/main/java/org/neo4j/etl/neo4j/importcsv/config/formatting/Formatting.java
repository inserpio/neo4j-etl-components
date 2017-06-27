package org.neo4j.etl.neo4j.importcsv.config.formatting;

import org.neo4j.etl.sql.DatabaseClient;
import org.neo4j.etl.sql.exportcsv.formatting.SqlQuotes;
import org.neo4j.etl.util.Preconditions;

public class Formatting
{
    public static final Delimiter DEFAULT_DELIMITER = new Delimiter( "," );
    public static final QuoteChar DEFAULT_QUOTE_CHAR = QuoteChar.DOUBLE_QUOTES;
    public static final Formatting DEFAULT = builder().build();

    public static Builder builder()
    {
        return new FormattingConfigBuilder();
    }

    private final Delimiter delimiter;
    private final Delimiter arrayDelimiter;
    private final QuoteChar quote;
    private final Formatter labelFormatter;
    private final Formatter relationshipFormatter;
    private final Formatter propertyFormatter;
    private final SqlQuotes sqlQuotes;

    Formatting( FormattingConfigBuilder builder )
    {
        this.delimiter = Preconditions.requireNonNull( builder.delimiter, "Delimiter" );
        this.arrayDelimiter = Preconditions.requireNonNull( builder.arrayDelimiter, "ArrayDelimiter" );
        this.quote = Preconditions.requireNonNull( builder.quote, "Quote" );
        this.labelFormatter = Preconditions.requireNonNull( builder.labelFormatter, "LabelFormatter" );
        this.relationshipFormatter = Preconditions.requireNonNull( builder.relationshipFormatter,
                "RelationshipFormatter" );
        this.propertyFormatter = Preconditions.requireNonNull( builder.propertyFormatter,
                "PropertyFormatter" );
        this.sqlQuotes = Preconditions.requireNonNull( builder.sqlQuotes, "SqlQuotes" );
    }

    public Delimiter delimiter()
    {
        return delimiter;
    }

    public Delimiter arrayDelimiter()
    {
        return arrayDelimiter;
    }

    public QuoteChar quote()
    {
        return quote;
    }

    public Formatter labelFormatter()
    {
        return labelFormatter;
    }

    public Formatter relationshipFormatter()
    {
        return relationshipFormatter;
    }

    public Formatter propertyFormatter()
    {
        return propertyFormatter;
    }

    public SqlQuotes sqlQuotes()
    {
        return sqlQuotes;
    }

    public interface Builder
    {
        Builder delimiter( Delimiter delimiter );

        Builder arrayDelimiter( Delimiter delimiter );

        Builder quote( QuoteChar quote );

        Builder labelFormatter( Formatter labelFormatter );

        Builder relationshipFormatter( Formatter relationshipFormatter );

        Builder propertyFormatter( Formatter propertyFormatter );

        Builder databaseClient(DatabaseClient databaseClient);

        Formatting build();
    }
}
