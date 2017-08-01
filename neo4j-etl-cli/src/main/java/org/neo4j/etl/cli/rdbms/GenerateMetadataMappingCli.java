package org.neo4j.etl.cli.rdbms;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.RequiredOnlyIf;
import com.github.rvesse.airline.model.CommandGroupMetadata;
import org.neo4j.etl.commands.rdbms.GenerateMetadataMapping;
import org.neo4j.etl.neo4j.importcsv.config.formatting.Formatting;
import org.neo4j.etl.neo4j.importcsv.config.formatting.ImportToolOptions;
import org.neo4j.etl.sql.ConnectionConfig;
import org.neo4j.etl.sql.exportcsv.io.TinyIntResolver;
import org.neo4j.etl.sql.exportcsv.mapping.FilterOptions;
import org.neo4j.etl.sql.exportcsv.mapping.MetadataMappings;
import org.neo4j.etl.sql.exportcsv.supplier.DefaultExportSqlSupplier;
import org.neo4j.etl.sql.metadata.Schema;
import org.neo4j.etl.util.CliRunner;
import org.neo4j.etl.util.Loggers;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;

@Command(name = "generate-metadata-mapping", description = "Create RDBMS to Neo4j metadata mapping Json.")
public class GenerateMetadataMappingCli implements Runnable {
    @RequireOnlyOne(tag = "database")
    @Option(type = OptionType.COMMAND,
            name = {"--rdbms:url", "--url"},
            description = "Url to use for connection to RDBMS.",
            title = "RDBMS url")
    protected String rdbmsUrl;

    /*
     * RDBMS connection parameters
     *
     * - MySQL --> jdbc:mysql://{hostname}:{port}/{database}?autoReconnect=true&useSSL=false
     * - PostgreSQL --> jdbc:postgresql://{hostname}:{port}/{database}?ssl=false
     * - Oracle --> jdbc:oracle:thin:@{hostname}:{port}:{database}
     * - MicrosoftSQL --> jdbc:sqlserver://{hostname}:{port};databaseName={database}
     */
    @Required
    @Option(type = OptionType.COMMAND,
            name = {"--rdbms:user", "-u", "--user"},
            description = "User for login to RDBMS.",
            title = "RDBMS user")
    protected String rdbmsUser;
    @Required
    @Option(type = OptionType.COMMAND,
            name = {"--rdbms:password", "--password"},
            description = "Password for login to RDBMS.",
            title = "RDBMS password")
    protected String rdbmsPassword;
    @Option(type = OptionType.COMMAND,
            name = {"--rdbms:schema", "-s", "--schema"},
            description = "RDBMS schema.",
            title = "schema")
    protected String rdbmsSchema;
    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @RequireOnlyOne(tag = "database")
    @Option(type = OptionType.COMMAND,
            name = {"-h", "--host"},
            description = "Host to use for connection to RDBMS.",
            title = "RDBMS host")
    protected String rdbmsHost;

    /*
     * Optional parameters
     */
    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @Option(type = OptionType.COMMAND,
            name = {"-p", "--port"},
            description = "Port number to use for connection to RDBMS.",
            title = "RDBMS port")
    protected Integer rdbmsPort;
    // deprecated in favour of "--rdbms:url"
    @Deprecated
    @RequiredOnlyIf(names = {"host"})
    @Option(type = OptionType.COMMAND,
            name = {"-d", "--database"},
            description = "RDBMS database.",
            title = "RDBMS database")
    protected String rdbmsDatabase;
    @Inject
    private CommandGroupMetadata commandGroupMetadata;
    @Option(type = OptionType.COMMAND,
            name = {"--delimiter"},
            description = "Delimiter to separate fields in CSV.",
            title = "delimiter")
    private String delimiter;
    @Option(type = OptionType.COMMAND,
            name = {"--quote"},
            description = "Character to treat as quotation character for values in CSV data.",
            title = "quote")
    private String quote;
    @Option(type = OptionType.COMMAND,
            name = {"--options-file"},
            description = "Path to file containing Neo4j import tool options.",
            title = "option file")
    private String importToolOptionsFile = "";
    @Option(type = OptionType.COMMAND,
            name = {"--debug"},
            description = "Print detailed diagnostic output.")
    private boolean debug = false;
    @Option(type = OptionType.COMMAND,
            name = {"--relationship-name", "--rel-name"},
            description = "Specifies whether to get the name for relationships from table names or column names.",
            title = "table(default)|column")
    private String relationshipNameFrom = "table";

    /*
     * Deprecated parameters (to be removed in future releases, still maintained for retro-compatibility)
     */
    @Option(type = OptionType.COMMAND,
            name = {"--tiny-int"},
            description = "Specifies whether to convert TinyInt to byte or boolean",
            title = "byte(default)|boolean")
    private String tinyIntAs = "byte";
    @Option(type = OptionType.COMMAND,
            name = {"--exclusion-mode", "--exc"},
            description = "Specifies how to handle table exclusion. Options are mutually exclusive." +
                    "exclude: Excludes specified tables from the process. All other tables will be included." +
                    "include: Includes specified tables only. All other tables will be excluded." +
                    "none: All tables are included in the process.",
            title = "exclude|include|none(default)")
    private String exclusionMode = "none";
    @Arguments(description = "Tables to be excluded/included",
            title = "table1 table2 ...")
    private List<String> tables = new ArrayList<String>();

    public static Callable<MetadataMappings> metadataMappingsFromFile(String mappingsFile, Formatting formatting) throws IOException {
        Callable<MetadataMappings> metadataMappings;
        if (mappingsFile.equalsIgnoreCase("stdin")) {
            Loggers.Default.log(Level.INFO, "Reading metadata mapping from stdin");
            try (Reader reader = new InputStreamReader(System.in);
                 BufferedReader buffer = new BufferedReader(reader)) {
                metadataMappings = GenerateMetadataMapping.load(buffer, formatting);
            }
        } else {
            Loggers.Default.log(Level.INFO, "Reading metadata mapping from file: " + mappingsFile);
            metadataMappings = GenerateMetadataMapping.load(mappingsFile, formatting);
        }
        return metadataMappings;
    }

    @Override
    public void run() {
        try {
            // The connection config is built only from the connection url
            ConnectionConfig connectionConfig = ConnectionConfig.forDatabaseFromUrl(this.rdbmsUrl)
                    .username(rdbmsUser)
                    .password(rdbmsPassword)
                    .build();

            ImportToolOptions importToolOptions =
                    ImportToolOptions.initialiseFromFile(Paths.get(importToolOptionsFile));

            Formatting formatting = Formatting.builder()
                    .delimiter(importToolOptions.getDelimiter(this.delimiter))
                    .quote(importToolOptions.getQuoteCharacter(this.quote))
                    .build();

            final FilterOptions filterOptions = new FilterOptions(tinyIntAs, relationshipNameFrom, exclusionMode,
                    tables, false);

            Schema schema = this.rdbmsSchema != null ? new Schema(this.rdbmsSchema) : Schema.UNDEFINED;

            new GenerateMetadataMapping(
                    new GenerateMetadataMappingEventHandler(),
                    System.out,
                    connectionConfig,
                    formatting,
                    new DefaultExportSqlSupplier(),
                    filterOptions, new TinyIntResolver(filterOptions.tinyIntAs())).forSchema(schema).call();
        } catch (Exception e) {
            CliRunner.handleException(e, debug);
        }
    }
}
