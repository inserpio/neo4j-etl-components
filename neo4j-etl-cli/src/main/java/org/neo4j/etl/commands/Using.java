package org.neo4j.etl.commands;

import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.commands.rdbms.importer.BoltDriverImportFromRdbms;
import org.neo4j.etl.commands.rdbms.importer.CypherShellImportFromRdbms;
import org.neo4j.etl.commands.rdbms.importer.Neo4jImportImportFromRdbms;
import org.neo4j.etl.commands.rdbms.importer.Neo4jShellImportFromRdbms;

import static java.lang.String.format;

public enum Using
{
    NEO4J_IMPORT (
            "bulk:neo4j-import",
            "", "", "", false,
            new Neo4jImportImportFromRdbms()
    ),
    NEO4J_SHELL (
            "cypher:neo4j-shell",
            format( "commit%n" ), format( "begin%n" ), format( "schema await%n" ), true,
            new Neo4jShellImportFromRdbms()
    ),
    CYPHER_SHELL (
            "cypher:shell",
            format( ":commit%n" ), format( ":begin%n" ), "", false,
            new CypherShellImportFromRdbms()
    ),
    BOLT_DRIVER (
            "cypher:direct",
            format( ":commit%n" ), format( ":begin%n" ), "", true,
            new BoltDriverImportFromRdbms()
    );

    private final String using;
    private final String commit;
    private final String begin;
    private final String schemaAwait;
    private boolean periodicCommit;
    private final ImportFromRdbms importer;

    Using(String using, String commit, String begin, String schemaAwait, boolean periodicCommit, ImportFromRdbms importer )
    {
        this.using = using;
        this.begin = begin;
        this.commit = commit;
        this.schemaAwait = schemaAwait;
        this.periodicCommit = periodicCommit;
        this.importer = importer;
    }

    public static Using fromString( String using )
    {
        for ( Using importer : Using.values() )
        {
            if ( using != null && using.equalsIgnoreCase( importer.using ) )
                return importer;
        }

        return Using.NEO4J_IMPORT;
    }

    public String using()
    {
        return this.using;
    }

    public String begin()
    {
        return this.begin;
    }

    public String commit()
    {
        return this.commit;
    }

    public String schemaAwait()
    {
        return this.schemaAwait;
    }

    public boolean periodicCommit()
    {
        return periodicCommit;
    }

    public ImportFromRdbms importer()
    {
        return importer;
    }
}