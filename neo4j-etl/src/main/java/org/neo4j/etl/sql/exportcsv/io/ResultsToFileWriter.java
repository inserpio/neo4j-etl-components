package org.neo4j.etl.sql.exportcsv.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.QueryResults;
import org.neo4j.etl.sql.exportcsv.mapping.ColumnToCsvFieldMappings;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMapping;
import org.neo4j.etl.sql.metadata.Column;

class ResultsToFileWriter
{
    private final TinyIntResolver tinyIntResolver;
    private Formatting formatting;

    ResultsToFileWriter( Formatting formatting, TinyIntResolver tinyIntResolver )
    {
        this.formatting = formatting;
        this.tinyIntResolver = tinyIntResolver;
    }

    public void write( QueryResults results, Path file, MetadataMapping resource ) throws Exception
    {
        RowStrategy rowStrategy = RowStrategy.select( resource.graphObjectType() );

        ColumnToCsvFieldMappings mappings = resource.mappings();
        Column[] columns = mappings.columns().toArray( new Column[mappings.columns().size()] );

        int maxIndex = columns.length - 1;
        int rowIndex = 0;

        try ( BufferedWriter writer = Files.newBufferedWriter( file, Charset.forName( "UTF8" ) ) )
        {
            while ( results.next() )
            {
                rowIndex += 1;

                if ( rowStrategy.isWriteableRow( results, rowIndex, columns ) )
                {
                    for ( int i = 0; i < maxIndex; i++ )
                    {
                        writeFieldValueAndDelimiter(
                                tinyIntResolver.handleSpecialCaseForTinyInt( columns[i].selectFrom( results, rowIndex ),
                                        columns[i].sqlDataType() ),
                                writer,
                                columns[i].useQuotes() );
                    }

                    writeFieldValueAndNewLine(
                            tinyIntResolver.handleSpecialCaseForTinyInt( columns[maxIndex].selectFrom( results,
                                    rowIndex ),
                                    columns[maxIndex].sqlDataType() ),
                            writer,
                            columns[maxIndex].useQuotes() );
                }
            }
        }
    }

    private void writeFieldValueAndDelimiter( String value,
                                              BufferedWriter writer,
                                              boolean useQuotes ) throws IOException
    {
        sanitiseAndWriteData( value, writer, useQuotes );
        writer.write( this.formatting.delimiter().value() );
    }

    private void writeFieldValueAndNewLine( String value,
                                            BufferedWriter writer,
                                            boolean useQuotes ) throws IOException
    {
        sanitiseAndWriteData( value, writer, useQuotes );
        writer.newLine();
    }

    private void sanitiseAndWriteData( String value,
                                       BufferedWriter writer,
                                       boolean useQuotes ) throws IOException
    {

        if ( StringUtils.isNotEmpty( value ) )
        {
            if ( useQuotes )
            {
                // TODO: check if "writeEnquoted" can be removed
                this.formatting.quote().writeEnquoted( value, writer );
            }
            else
            {
                writer.write( value );
            }
        }
    }
}
