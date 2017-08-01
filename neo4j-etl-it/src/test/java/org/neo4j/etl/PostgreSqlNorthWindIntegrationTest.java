package org.neo4j.etl;

import com.jayway.jsonpath.JsonPath;
import org.neo4j.etl.neo4j.Neo4j;
import org.neo4j.etl.provisioning.Server;
import org.neo4j.etl.provisioning.scripts.RdbmsScripts;
import org.neo4j.etl.rdbms.RdbmsClient;
import org.neo4j.etl.util.ResourceRule;

import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.neo4j.etl.neo4j.Neo4j.NEO_TX_URI;

public class PostgreSqlNorthWindIntegrationTest {
    protected static void setUp(ResourceRule<Server> postgresql, ResourceRule<Neo4j> neo4j, String[] args) throws Exception {
        String adminUrl = String.format("jdbc:postgresql://%s:%s/%s?ssl=false", postgresql.get().ipAddress(), 5433, "postgres");
        RdbmsClient postgres = new RdbmsClient(
                adminUrl,
                RdbmsClient.Parameters.DBUser.value(),
                RdbmsClient.Parameters.DBPassword.value());

        postgres.execute(RdbmsScripts.northwindStartupScript("PostgreSQL").value());

        String url = String.format("jdbc:postgresql://%s:%s/%s?ssl=false", postgresql.get().ipAddress(), 5433, "northwind");
        RdbmsClient northwind = new RdbmsClient(
                url,
                RdbmsClient.Parameters.DBUser.value(),
                RdbmsClient.Parameters.DBPassword.value());

        northwind.execute(RdbmsScripts.northwindScript("PostgreSQL").value());

        NeoIntegrationCli.executeMainReturnSysOut(args);
    }

    protected static void tearDown(ResourceRule<Neo4j> neo4j) throws Exception {
        neo4j.get().stop();
    }

    protected static void shouldExportFromPostgreSqlAndImportIntoGraph(ResourceRule<Neo4j> neo4j) throws Exception {
        assertFalse(neo4j.get().containsImportErrorLog(Neo4j.DEFAULT_DATABASE));

        String customersJson = neo4j.get().executeHttp(NEO_TX_URI, "MATCH (c:Customer) RETURN c");
        String customersWithOrdersJson = neo4j.get().executeHttp(NEO_TX_URI,
                "MATCH (c)--(o) " +
                        "WHERE (c:Customer)<-[:CUSTOMER]-(o:Order) RETURN DISTINCT c");
        List<String> customers = JsonPath.read(customersJson, "$.results[*].data[*].row[0]");
        List<String> customersWithOrders = JsonPath.read(customersWithOrdersJson, "$.results[*].data[*].row[0]");
        assertThat(customers.size(), is(91));
        assertThat(customersWithOrders.size(), is(89));

        String newOrdersJson = neo4j.get().executeHttp(NEO_TX_URI,
                "MATCH (c)--(o) " +
                        "WHERE (c:Customer)--(o:Order{shipCity:'Lyon'}) RETURN DISTINCT c");
        List<String> customersWithOrdersToLyon = JsonPath.read(newOrdersJson, "$.results[*].data[*].row[0]");

        assertThat(customersWithOrdersToLyon.size(), is(1));

        String territoriesWithEmployeesResponse = neo4j.get().executeHttp(NEO_TX_URI,
                "MATCH (e:Employee)-->(t:Territory) RETURN e,t;");
        List<String> territories = JsonPath.read(territoriesWithEmployeesResponse, "$.results[*].data[*].row[1]");

        assertThat(territories.size(), is(49));

        String managers = neo4j.get().executeHttp(NEO_TX_URI,
                "MATCH (e:Employee)--(r:Employee) WHERE NOT (e:Employee)-->(r:Employee) RETURN e;");
        List<String> lastNames = JsonPath.read(managers, "$.results[*].data[*].row[0].lastName");
        assertThat(lastNames, hasItems("Fuller", "Buchanan"));
    }
}
