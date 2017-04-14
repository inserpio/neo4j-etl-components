package org.neo4j.etl.commands;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.sql.exportcsv.DatabaseExportSqlSupplier;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappingProvider;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;
import org.neo4j.etl.sql.exportcsv.mapping.RelationshipNameResolver;
import org.neo4j.etl.sql.metadata.Join;
import org.neo4j.etl.sql.metadata.JoinTable;
import org.neo4j.etl.sql.metadata.Table;
import org.neo4j.etl.sql.metadata.TableName;
import org.neo4j.etl.util.ArrayUtils;

import static java.lang.String.format;

public class SchemaExport
{
    private final Collection<Join> joins;
    private final Collection<Table> tables;
    private final Collection<JoinTable> joinTables;

    public SchemaExport( Collection<Table> tables, Collection<Join> joins, Collection<JoinTable> joinTables )
    {
        this.joins = joins;
        this.tables = tables;
        this.joinTables = joinTables;
    }

    public MetadataMappings generateMetadataMappings( Formatting formatting,
                                                      DatabaseExportSqlSupplier sqlSupplier,
                                                      RelationshipNameResolver relationshipNameResolver,
                                                      TinyIntResolver tinyIntResolver )
    {
        validate();

        MetadataMappingProvider metadataMappingProvider =
                new MetadataMappingProvider( formatting, sqlSupplier, relationshipNameResolver, tinyIntResolver );
        MetadataMappings metadataMappings = new MetadataMappings();

        tables.forEach( o -> metadataMappings.add( o.invoke( metadataMappingProvider ) ) );
        joins.forEach( o -> metadataMappings.add( o.invoke( metadataMappingProvider ) ) );
        joinTables.forEach( o -> metadataMappings.add( o.invoke( metadataMappingProvider ) ) );

        return metadataMappings;
    }

    Collection<Table> tables()
    {
        return tables;
    }

    Collection<Join> joins()
    {
        return joins;
    }

    Collection<JoinTable> joinTables()
    {
        return joinTables;
    }

    private void validate()
    {
        List<String> allTableNames = tables.stream().map( Table::descriptor ).collect( Collectors.toList() );

        joins.forEach(
                join -> join.tableNames().forEach(
                        tableName ->
                        {
                            if ( !ArrayUtils.containsIgnoreCase( allTableNames, tableName.fullName() ) )
                            {
                                throw new IllegalStateException(
                                        format( "Config is missing table definition '%s' for join [%s]",
                                                tableName.fullName(),
                                                join.tableNames().stream()
                                                        .map( TableName::fullName )
                                                        .collect( Collectors.joining( " -> " ) ) ) );
                            }
                        } ) );

    }
}
