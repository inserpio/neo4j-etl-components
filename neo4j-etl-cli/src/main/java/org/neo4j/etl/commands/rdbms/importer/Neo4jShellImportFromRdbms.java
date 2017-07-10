package org.neo4j.etl.commands.rdbms.importer;

import org.neo4j.etl.commands.Using;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.loadcsv.Neo4jShellCommand;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jShellConfig;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvNode;
import org.neo4j.etl.neo4j.loadcsv.mapping.CsvRelationship;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.String.format;

public class Neo4jShellImportFromRdbms extends AbstractLoadCsvImportFromRdbms implements ImportFromRdbms
{
    protected void doLoadCsv( Neo4jConnectionConfig neo4jConnectionConfig,
                              Environment environment,
                              Formatting formatting,
                              MetadataMappings metadataMappings ) throws Exception
    {
        List<CsvNode> nodes = getCsvNodes( environment, formatting, metadataMappings );
        List<CsvRelationship> relationships = getCsvRelationships( environment, formatting, metadataMappings, nodes );

        Neo4jShellConfig.Builder builder = Neo4jShellConfig.builder()
                .importToolDirectory( environment.importToolDirectory() )
                .destination( environment.destinationDirectory() )
                //.config( environment.importToolDirectory().resolve( "conf/neo4j.conf" ) )
                .config( createConfigFile( environment ) )
                .file( createLoadCsvFile( environment, formatting, nodes, relationships ) );

        new Neo4jShellCommand( builder.build() ).execute();
    }

    @Override
    protected String begin()
    {
        return Using.NEO4J_SHELL.begin();
}

    @Override
    protected String commit()
    {
        return Using.NEO4J_SHELL.commit();
    }

    @Override
    protected String schemaAwait()
    {
        return Using.NEO4J_SHELL.schemaAwait();
    }

    @Override
    protected boolean periodicCommit()
    {
        return Using.NEO4J_SHELL.periodicCommit();
    }

    private Path createConfigFile( Environment environment ) throws IOException
    {
        Path config = Files.createFile(
                Paths.get( environment.csvDirectory().toAbsolutePath().toString(), "load-cvs-neo4j.conf" ) );

        BufferedWriter writer = Files.newBufferedWriter(config, Charset.forName("UTF8") );
        writer.write( format( "dbms.security.allow_csv_import_from_file_urls=true%n" ) );
        writer.write( format( "dbms.directories.import=" + environment.csvDirectory().getParent().toAbsolutePath().toString() + "%n" ) );
        writer.flush();
        writer.close();

        return config;
    }
}
