package org.neo4j.etl.sql.exportcsv.formatting;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.etl.neo4j.importcsv.config.formatting.QuoteChar;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.ColumnRole;
import org.neo4j.etl.sql.metadata.TableName;

public class SqlQuotes {
    public static final SqlQuotes DEFAULT = new SqlQuotes(QuoteChar.TICK_QUOTES, new QuoteChar(".", "."));

    private final QuoteChar databaseObjectQuoteChar;

    private final QuoteChar catalogSeparator;

    private final QuoteChar constantObjectQuoteChar;

    /**
     * Default constructor from database default character and catalog separator character.
     * It also automatically infers the constant default character.
     *
     * @param databaseObjectQuoteChar the default character that is used to quote tables, columns, etc
     * @param catalogSeparator the default character that is used to separate database objects
     */
    public SqlQuotes(QuoteChar databaseObjectQuoteChar, QuoteChar catalogSeparator) {
        this.databaseObjectQuoteChar = databaseObjectQuoteChar;
        this.catalogSeparator = catalogSeparator;

        // If the schema/table/columns default quote character is the double quote
        // than we can use the single quote otherwise we can leave it as is
        this.constantObjectQuoteChar = databaseObjectQuoteChar.value().equals(QuoteChar.DOUBLE_QUOTES.value()) ?
                QuoteChar.SINGLE_QUOTES :
                QuoteChar.DOUBLE_QUOTES;
    }

    /**
     * Quotes a table name in order to deal with case sensitive names
     *
     * @param tableName
     * @return
     */
    public String quoteTable(TableName tableName) {
        return quoteObjectName(tableName.fullName());
    }

    /**
     * Quotes a column name in order to deal with case sensitive names
     *
     * @param column
     * @return
     */
    public String quoteColumn(Column column) {
        String quote = this.databaseObjectQuoteChar.value();

        if (ColumnRole.Literal.equals(column.role())) {
            return String.format("%s AS %s",
                    this.constantObjectQuoteChar.value() + normalizeToUnquoted(column.name()) + this.constantObjectQuoteChar.value(),
                    quote + column.alias() + quote);
        } else {
            return String.format("%s AS %s",
                    quoteObjectName(column.name()),
                    quote + column.alias() + quote);
        }
    }

    /**
     * Quote a generic db object: schema, table or column
     *
     * @param dbObjectNameToQuote
     * @return
     */
    private String quoteObjectName(String dbObjectNameToQuote) {
        String quote = this.databaseObjectQuoteChar.value();
        String separator = this.catalogSeparator.value();

        return quote + StringUtils.join(StringUtils.split(normalizeToUnquoted(dbObjectNameToQuote), separator), quote + separator + quote) + quote;
    }

    /**
     * This method is used to avoid double quoted db object's name.
     * If the quote character is not the same quote character used by Java then it will remains in the object's name itself.
     * For example, MySql uses the tick character as the quote character,
     * so if we have a table `MyTable` then the Java string representation will include the ticks.
     * When we quote the db object's full name than we need to avoid this case: `schema`.``MyTable``
     *
     * @param dbObjectNameToQuote
     * @return
     */
    private String normalizeToUnquoted(String dbObjectNameToQuote) {
        return dbObjectNameToQuote.replaceAll(this.databaseObjectQuoteChar.value(), "");
    }
}
