package org.neo4j.etl.sql.exportcsv.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.lang3.StringUtils;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.fields.CsvField;
import org.neo4j.etl.sql.metadata.Column;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.Preconditions;

public class ColumnToCsvFieldMappings
{
    public static ColumnToCsvFieldMappings fromJson( JsonNode root, Formatting formatting )
    {
        ArrayNode mappingArray = (ArrayNode) root;
        Collection<ColumnToCsvFieldMapping> mappings = new ArrayList<>();
        for ( JsonNode jsonNode : mappingArray )
        {
            mappings.add( ColumnToCsvFieldMapping.fromJson( jsonNode ) );
        }
        return new ColumnToCsvFieldMappings( mappings, formatting );
    }

    public static Builder builder()
    {
        return new ColumnToCsvFieldMappingsBuilder();
    }

    private final Collection<ColumnToCsvFieldMapping> mappings;
    private final Formatting formatting;

    ColumnToCsvFieldMappings( Collection<ColumnToCsvFieldMapping> mappings, Formatting formatting )
    {
        this.mappings = Collections.unmodifiableCollection(
                Preconditions.requireNonEmptyCollection( mappings, "Mappings" ) );
        this.formatting = Preconditions.requireNonNull( formatting, "Formatting" );
    }

    public Collection<CsvField> fields()
    {
        return mappings.stream().map( ColumnToCsvFieldMapping::field ).collect( Collectors.toList() );
    }

    public Collection<Column> columns()
    {
        return mappings.stream().map( ColumnToCsvFieldMapping::column ).collect( Collectors.toList() );
    }

    public Collection<String> aliasedColumns()
    {
        return columns().stream()
                .filter( Column::allowAddToSelectStatement )
                .map( Column::aliasedColumn )
                .collect( Collectors.toList() );
    }

    public Collection<String> tableNames()
    {
        // TODO replace formatting.sqlQuotes().forTable().value() + StringUtils.join( name.split( "\\." ), formatting.sqlQuotes().forTable().value() + "." + formatting.sqlQuotes().forTable().value() ) + formatting.sqlQuotes().forTable().value())
        // TODO with formatting.sqlQuotes().forTable(name) moving the logic inside SqlQuote class
        return columns().stream()
                .map( Column::table )
                .distinct()
                .map( TableName::fullName )
                .map( name -> formatting.sqlQuotes().forTable().value() + StringUtils.join( name.split( "\\." ), formatting.sqlQuotes().forTable().value() + "." + formatting.sqlQuotes().forTable().value() ) + formatting.sqlQuotes().forTable().value())
                .collect( Collectors.toCollection( LinkedHashSet::new ) );
    }

    public JsonNode toJson()
    {
        ArrayNode root = JsonNodeFactory.instance.arrayNode();

        for ( ColumnToCsvFieldMapping mapping : mappings )
        {
            root.add( mapping.toJson() );
        }

        return root;
    }

    public interface Builder
    {
        Builder add( ColumnToCsvFieldMapping columnToCsvFieldMapping );

        Builder withFormatting(Formatting formatting );

        ColumnToCsvFieldMappings build();
    }
}
