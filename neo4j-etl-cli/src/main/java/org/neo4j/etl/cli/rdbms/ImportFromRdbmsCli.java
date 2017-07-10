package org.neo4j.etl.cli.rdbms;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.RequiredOnlyIf;
import com.github.rvesse.airline.model.CommandGroupMetadata;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.etl.commands.Using;
import org.neo4j.etl.commands.rdbms.ImportFromRdbms;
import org.neo4j.etl.commands.rdbms.GenerateMetadataMapping;
import org.neo4j.etl.environment.CsvDirectorySupplier;
import org.neo4j.etl.environment.DestinationDirectorySupplier;
import org.neo4j.etl.environment.Environment;
import org.neo4j.etl.environment.ImportToolDirectorySupplier;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.config.formatting.ImportToolOptions;
import org.neo4j.etl.neo4j.loadcsv.config.Neo4jConnectionConfig;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.DatabaseType;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;
import org.neo4j.etl.sql.exportcsv.mapping.TinyIntAs;
import org.neo4j.etl.sql.exportcsv.supplier.DefaultExportSqlSupplier;
import org.neo4j.etl.sql.metadata.Schema;
import org.neo4j.etl.util.CliRunner;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "export", description = "Export from RDBMS and import into NEO4J via CSV files.")
public class ImportFromRdbmsCli implements Runnable
{
    @Inject
    protected CommandGroupMetadata commandGroupMetadata;

    /*
     * RDBMS connection parameters
     */

    @RequireOnlyOne(tag = "database")
    @Option(type = OptionType.COMMAND,
            name = { "--rdbms:url", "--url" },
            description = "Url to use for connection to RDBMS.",
            title = "RDBMS url")
    protected String rdbmsUrl;

    @Required
    @Option(type = OptionType.COMMAND,
            name = { "--rdbms:user", "-u", "--user" },
            description = "User for login to RDBMS.",
            title = "RDBMS user")
    protected String rdbmsUser;

    @Required
    @Option(type = OptionType.COMMAND,
            name = { "--rdbms:password", "--password" },
            description = "Password for login to RDBMS.",
            title = "RDBMS password")
    protected String rdbmsPassword;

    @Option(type = OptionType.COMMAND,
            name = { "--rdbms:schema", "-s", "--schema" },
            description = "RDBMS schema.",
            title = "schema")
    protected String rdbmsSchema;

    /*
     * NEO4J connection parameters
     */

    @Option(type = OptionType.COMMAND,
            name = { "--neo4j:url", "--graph:url", "--graph:neo4j:url" },
            description = "Url to use for connection to Neo4j.",
            title = "neo4j url")
    protected String neo4jUrl = "bolt://localhost:7687";

    @Option(type = OptionType.COMMAND,
            name = {"--neo4j:user", "--graph:user", "--graph:neo4j:user"},
            description = "User for login to Neo4j.",
            title = "neo4j user")
    protected String neo4jUser;

    @Option(type = OptionType.COMMAND,
            name = {"--neo4j:password", "--graph:password", "--graph:neo4j:password"},
            description = "Password for login to Neo4j.",
            title = "neo4j password")
    protected String neo4jPassword;

    /*
     * Mandatory parameters
     */

    @Required
    @Option(type = OptionType.COMMAND,
            name = {"--import-tool"},
            description = "Path to directory containing Neo4j import tool.",
            title = "import tool path")
    protected String importToolDirectory;

    @Required
    @Option(type = OptionType.COMMAND,
            name = {"--csv-directory"},
            description = "Path to directory for intermediate CSV files.",
            title = "csv directory")
    protected String csvRootDirectory;

    @Required
    @Option(type = OptionType.COMMAND,
            name = {"--destination"},
            description = "Path to destination store directory.",
            title = "directory")
    protected String destinationDirectory;

    /*
     * Optional parameters
     */

    @Option(type = OptionType.COMMAND,
            name = {"--using"},
            description = "Import tool that will be used to load data into neo4j.",
            title = "import tool")
    protected String using = Using.NEO4J_IMPORT.toString();

    @Option(type = OptionType.COMMAND,
            name = {"--options-file"},
            description = "Path to file containing Neo4j import tool options.",
            title = "options file")
    protected String importToolOptionsFile = "";

    @Option(type = OptionType.COMMAND,
            name = {"--force"},
            description = "Force delete destination store directory if it already exists.")
    protected boolean force = false;

    @Option(type = OptionType.COMMAND,
            name = {"--delimiter"},
            description = "Delimiter to separate fields in CSV.",
            title = "character")
    protected String delimiter;

    @Option(type = OptionType.COMMAND,
            name = {"--quote"},
            description = "Character to treat as quotation character for values in CSV data.",
            title = "character")
    protected String quote;
    @SuppressWarnings("FieldCanBeLocal")

    @Option(type = OptionType.COMMAND,
            name = {"--debug"},
            description = "Print detailed diagnostic output.")
    protected boolean debug = false;

    @Option(type = OptionType.COMMAND,
            name = {"--mapping-file"},
            description = "Path to an existing metadata mapping file. " +
                    "The name 'stdin' will cause the CSV resources definitions to be read from standard input.",
            title = "file|stdin")
    protected String mappingFile;

    @Option(type = OptionType.COMMAND,
            name = {"--relationship-name", "--rel-name"},
            description = "Specifies whether to get the name for relationships from table names or column names",
            title = "table(default)|column")
    protected String relationshipNameFrom = "table";

    @Option(type = OptionType.COMMAND,
            name = {"--tiny-int", "--tiny"},
            description = "Specifies whether to get the convert TinyInt to byte or boolean",
            title = "byte(default)|boolean")
    protected String tinyIntAs = "byte";

    @Option(type = OptionType.COMMAND,
            name = {"--exclusion-mode", "--exc"},
            description = "Specifies how to handle table exclusion. Options are mutually exclusive." +
                    "exclude: Excludes specified tables from the process. All other tables will be included." +
                    "include: Includes specified tables only. All other tables will be excluded." +
                    "none: All tables are included in the process.",
            title = "exclude|include|none(default)")
    protected String exclusionMode = "none";

    @Arguments(description = "Tables to be excluded/included",
            title = "table1 table2 ...")
    protected List<String> tables = new ArrayList<String>();

    /*
     * Deprecated parameters (to be removed in future releases, still maintained for retro-compatibility)
     */

    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @RequireOnlyOne(tag = "database")
    @Option(type = OptionType.COMMAND,
            name = {"-h", "--host"},
            description = "Host to use for connection to RDBMS.",
            title = "RDBMS host")
    protected String rdbmsHost;

    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @Option(type = OptionType.COMMAND,
            name = {"-p", "--port"},
            description = "Port number to use for connection to RDBMS.",
            title = "RDBMS port")
    protected Integer rdbmsPort;

    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @RequiredOnlyIf( names = { "host" } )
    @Option(type = OptionType.COMMAND,
            name = {"-d", "--database"},
            description = "RDBMS database.",
            title = "RDBMS database")
    protected String rdbmsDatabase;

    @Override
    public void run()
    {
        try
        {
            DatabaseType databaseType = DatabaseType.fromString( this.commandGroupMetadata.getName() );

            ConnectionConfig rdbmsConnectionConfig = this.rdbmsUrl == null ?
                    ConnectionConfig.forDatabaseFromHostAndPort(databaseType)
                            .host(rdbmsHost)
                            .port( rdbmsPort != null ? rdbmsPort : databaseType.defaultPort() )
                            .database(rdbmsDatabase)
                            .username(rdbmsUser)
                            .password(rdbmsPassword)
                            .build()
                    :
                    ConnectionConfig.forDatabaseFromUrl(databaseType)
                            .url(rdbmsUrl)
                            .username(rdbmsUser)
                            .password(rdbmsPassword)
                            .build();

            Neo4jConnectionConfig neo4jConnectionConfig =
                    new Neo4jConnectionConfig(
                            new URI( neo4jUrl ), neo4jUser, neo4jPassword );

            Environment environment = new Environment(
                    new ImportToolDirectorySupplier( Paths.get( importToolDirectory ) ).supply(),
                    new DestinationDirectorySupplier( Paths.get( destinationDirectory ), force ).supply(),
                    new CsvDirectorySupplier( Paths.get( csvRootDirectory ) ).supply(),
                    ImportToolOptions.initialiseFromFile( Paths.get( importToolOptionsFile ) ) );

            ImportToolOptions importToolOptions = environment.importToolOptions();

            ImportFromRdbms exporter = Using.fromString( using ).importer();

            Formatting formatting = Formatting.builder()
                    .delimiter( importToolOptions.getDelimiter( delimiter ) )
                    .quote( importToolOptions.getQuoteCharacter( exporter.quote( quote ) ) )
                    .build();

            TinyIntResolver tinyIntResolver = new TinyIntResolver( TinyIntAs.parse( tinyIntAs ) );

            MetadataMappings metadataMappings = createMetadataMappings(
                    rdbmsConnectionConfig, formatting, tinyIntResolver );

            exporter.extractAndLoad(
                    rdbmsConnectionConfig,
                    neo4jConnectionConfig,
                    environment,
                    formatting,
                    tinyIntResolver,
                    metadataMappings,
                    new ImportFromRdbmsEventHandler() );
        }
        catch ( Exception e )
        {
            CliRunner.handleException( e, debug );
        }
    }


    private MetadataMappings createMetadataMappings( ConnectionConfig connectionConfig,
                                                     Formatting formatting,
                                                     TinyIntResolver tinyIntResolver ) throws Exception
    {
        Callable<MetadataMappings> generateMetadataMappings;

        if ( StringUtils.isNotEmpty( mappingFile ) )
        {
            generateMetadataMappings = GenerateMetadataMappingCli.metadataMappingsFromFile( mappingFile, formatting );
        }
        else
        {
            final FilterOptions filterOptions = new FilterOptions( tinyIntAs, relationshipNameFrom, exclusionMode, tables, false );

            generateMetadataMappings = new GenerateMetadataMapping(
                    new GenerateMetadataMappingEventHandler(),
                    emptyOutputStream(),
                    connectionConfig,
                    formatting,
                    new DefaultExportSqlSupplier(),
                    filterOptions,
                    tinyIntResolver );

            Schema rdbmsSchema = this.rdbmsSchema != null ? new Schema( this.rdbmsSchema ) : Schema.UNDEFINED;

            ((GenerateMetadataMapping) generateMetadataMappings).forSchema( rdbmsSchema );
        }

        return generateMetadataMappings.call();
    }

    private OutputStream emptyOutputStream()
    {
        return new OutputStream()
        {
            @Override
            public void write( int b ) throws IOException
            {
                // Do nothing
            }
        };
    }
}
