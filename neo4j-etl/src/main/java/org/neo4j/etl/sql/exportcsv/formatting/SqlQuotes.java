package org.neo4j.etl.sql.exportcsv.formatting;

import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;

public class SqlQuotes
{
    public static final SqlQuotes DEFAULT = new SqlQuotes( QuoteChar.TICK_QUOTES, QuoteChar.TICK_QUOTES, QuoteChar.TICK_QUOTES, QuoteChar.DOUBLE_QUOTES );

    private final QuoteChar forSchema;
    private final QuoteChar forTable;
    private final QuoteChar forColumn;
    private final QuoteChar forConstant;

    public SqlQuotes( QuoteChar forSchema, QuoteChar forTable, QuoteChar forColumn, QuoteChar forConstant )
    {
        this.forSchema = forSchema;
        this.forTable = forTable;
        this.forColumn = forColumn;
        this.forConstant = forConstant;
    }

    public QuoteChar forSchema()
    {
        return this.forSchema;
    }

    public QuoteChar forTable()
    {
        return this.forTable;
    }

    public QuoteChar forColumn()
    {
        return this.forColumn;
    }

    public QuoteChar forConstant()
    {
        return forConstant;
    }
}
