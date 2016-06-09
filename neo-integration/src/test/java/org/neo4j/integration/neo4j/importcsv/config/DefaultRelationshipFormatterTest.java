package org.neo4j.integration.neo4j.importcsv.config;

import org.junit.Test;

import org.neo4j.integration.neo4j.importcsv.config.formatting.DefaultRelationshipFormatter;

import static org.junit.Assert.assertEquals;

public class DefaultRelationshipFormatterTest
{
    @Test
    public void shouldFormatAsUpperCaseWithUnderscores()
    {
        // given
        DefaultRelationshipFormatter formatter = new DefaultRelationshipFormatter();

        // then
        assertEquals( "AUTHOR_BOOK", formatter.format( "AuthorBook" ) );
        assertEquals( "AUTHOR_BOOK", formatter.format( "authors_books" ) );
        assertEquals( "AUTHOR_FIRST_NAME_LAST_NAME", formatter.format( "AUTHOR_FIRST_NAME\0LAST_NAME" ) );
    }
}
