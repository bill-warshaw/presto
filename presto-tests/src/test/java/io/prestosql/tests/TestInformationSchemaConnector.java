/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.CountingMockConnector;
import io.prestosql.testing.CountingMockConnector.MetadataCallsCount;
import io.prestosql.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestInformationSchemaConnector
        extends AbstractTestQueryFramework
{
    private final CountingMockConnector countingMockConnector = new CountingMockConnector();

    @Test
    public void testBasic()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata", "VALUES 10");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables", "VALUES 80");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns", "VALUES 583");
        assertQuery("SELECT * FROM tpch.information_schema.schemata ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('tpch', 'tiny')");
        assertQuery("SELECT * FROM tpch.information_schema.tables ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'BASE TABLE')");
        assertQuery("SELECT * FROM tpch.information_schema.columns ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'suppkey', 1, NULL, 'YES', 'bigint')");
        assertQuery("SELECT count(*) FROM test_catalog.information_schema.columns", "VALUES 300034");
    }

    @Test
    public void testSchemaNamePredicate()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata WHERE schema_name = 'sf1'", "VALUES 1");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema = 'sf1'", "VALUES 8");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'information_schema'", "VALUES 34");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema > 'sf100'", "VALUES 427");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema != 'sf100'", "VALUES 522");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf100'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf%'", "VALUES 488");
    }

    @Test
    public void testTableNamePredicate()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name = 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name = 'ORDERS'", "VALUES 0");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name < 'orders'", "VALUES 30");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'part'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'part%'", "VALUES 18");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name = 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name < 'orders'", "VALUES 265");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part%'", "VALUES 126");
    }

    @Test
    public void testMixedPredicate()
    {
        assertQuery("SELECT * FROM tpch.information_schema.tables WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES ('tpch', 'sf1', 'orders', 'BASE TABLE')");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema > 'sf1' and table_name < 'orders'", "VALUES 24");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema > 'sf1' and table_name < 'orders'", "VALUES 224");
    }

    @Test
    public void testProject()
    {
        assertQuery("SELECT schema_name FROM tpch.information_schema.schemata ORDER BY 1 DESC LIMIT 1", "VALUES 'tiny'");
        assertQuery("SELECT table_name, table_type FROM tpch.information_schema.tables ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('views', 'BASE TABLE')");
        assertQuery("SELECT column_name, data_type FROM tpch.information_schema.columns ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('with_hierarchy', 'varchar')");
    }

    @Test
    public void testLimit()
    {
        assertQuery("SELECT count(*) FROM (SELECT * from tpch.information_schema.columns LIMIT 1)", "VALUES 1");
        assertQuery("SELECT count(*) FROM (SELECT * FROM tpch.information_schema.columns LIMIT 100)", "VALUES 100");
        assertQuery("SELECT count(*) FROM (SELECT * FROM test_catalog.information_schema.tables LIMIT 1000)", "VALUES 1000");
    }

    @Test(timeOut = 60_000)
    public void testMetadataCalls()
    {
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1'",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1' AND schema_name IN ('test_schema1', 'test_schema2')",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables",
                "VALUES 3008",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema = 'test_schema1'",
                "VALUES 1000",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1'",
                "VALUES 1000",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1' AND table_schema IN ('test_schema1', 'test_schema2')",
                "VALUES 1000",
                new MetadataCallsCount()
                        .withListTablesCount(2));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name = 'test_table1'",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1'",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(3));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1' AND table_name IN ('test_table1', 'test_table2')",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_schema = 'test_schema1' AND table_name = 'test_table1'",
                "VALUES 100",
                new MetadataCallsCount()
                        .withListTablesCount(1)
                        .withGetColumnsCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'wrong'",
                "VALUES 0",
                new MetadataCallsCount());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'test_catalog' AND table_schema = 'wrong_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog IN ('wrong', 'test_catalog') AND table_schema = 'wrong_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 1)",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(1)
                        .withGetColumnsCount(0));
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 1000)",
                "VALUES 1000",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2)
                        .withGetColumnsCount(1000));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema = ''",
                "VALUES 0",
                new MetadataCallsCount());
    }

    @Test
    public void testInformationForEmptyNames()
    {
        assertNoTableScan("SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema = ''");
        assertNoTableScan("SELECT count(*) from test_catalog.information_schema.tables WHERE table_name = ''");
    }

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(countingMockConnector.getPlugin());
            queryRunner.createCatalog("test_catalog", "mock", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private void assertMetadataCalls(String actualSql, String expectedSql, MetadataCallsCount expectedMetadataCallsCount)
    {
        MetadataCallsCount actualMetadataCallsCount = countingMockConnector.runCounting(() -> {
            // expectedSql is run on H2, so does not affect counts.
            assertQuery(actualSql, expectedSql);
        });

        assertEquals(actualMetadataCallsCount, expectedMetadataCallsCount);
    }

    private void assertNoTableScan(String query)
    {
        assertFalse(searchFrom(getQueryRunner().createPlan(getSession(), query, WarningCollector.NOOP).getRoot())
                        .where(TableScanNode.class::isInstance)
                        .findFirst()
                        .isPresent(),
                "TableScanNode was not expected");
    }
}
