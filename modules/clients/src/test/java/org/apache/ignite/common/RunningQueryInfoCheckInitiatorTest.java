/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Tests for query originator.
 */
public class RunningQueryInfoCheckInitiatorTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setAuthenticationEnabled(true)
            .setCacheConfiguration(new CacheConfiguration()
                .setName("test")
                .setSqlSchema("TEST")
                .setSqlFunctionClasses(TestSQLFunctions.class)
                .setIndexedTypes(Integer.class, Integer.class)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);
        startClientGrid(1);

        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        TestSQLFunctions.reset();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserDefinedInitiatorId() throws Exception {
        final String initiatorId = "TestUserSpecifiedOriginator";

        GridTestUtils.runAsync(() ->
            grid(0).context().query().querySqlFields(
                new SqlFieldsQuery("SELECT test.awaitLatch()").setQueryInitiatorId(initiatorId), false).getAll());

        assertEquals(initiatorId, initiatorId(grid(0), "awaitLatch", 1000));

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 2000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStatementsUserDefinedInitiatorId() throws Exception {
        final String initiatorId = "TestUserSpecifiedOriginator";

        GridTestUtils.runAsync(() -> {
            List<FieldsQueryCursor<List<?>>> curs = grid(0).context().query().querySqlFields(
                new SqlFieldsQuery("SELECT 'qry0', test.awaitLatch(); SELECT 'qry1', test.awaitLatch()")
                    .setQueryInitiatorId(initiatorId), false, false);

            for (FieldsQueryCursor<List<?>> cur : curs)
                cur.getAll();
        });

        assertEquals(initiatorId, initiatorId(grid(0), "qry0", 1000));

        TestSQLFunctions.reset();

        assertEquals(initiatorId, initiatorId(grid(0), "qry1", 1000));

        checkRunningQueriesCount(grid(0), 1, 1000);

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 1000);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcThinInitiatorId() throws Exception {
        GridTestUtils.runAsync(() -> {
                try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?user=ignite&password=ignite")) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT test.awaitLatch()");
                    }
                }
                catch (SQLException e) {
                    log.error("Unexpected exception", e);
                }
            }
        );

        String initiatorId = initiatorId(grid(0), "awaitLatch", 1000);

        assertTrue("Invalid initiator ID: " + initiatorId,
            Pattern.compile("jdbc-thin:127\\.0\\.0\\.1:[0-9]+@ignite").matcher(initiatorId).matches());

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 1000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThinClientInitiatorId() throws Exception {
        GridTestUtils.runAsync(() -> {
                try (IgniteClient cli = Ignition.startClient(
                    new ClientConfiguration()
                        .setAddresses("127.0.0.1")
                        .setUserName("ignite")
                        .setUserPassword("ignite"))) {
                    cli.query(new SqlFieldsQuery("SELECT test.awaitLatch()")).getAll();
                }
                catch (Exception e) {
                    log.error("Unexpected exception", e);
                }
            }
        );

        String initiatorId = initiatorId(grid(0), "awaitLatch", 1000);

        assertTrue("Invalid initiator ID: " + initiatorId, Pattern.compile("cli:127\\.0\\.0\\.1:[0-9]+@ignite")
            .matcher(initiatorId).matches());

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 1000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobDefaultInitiatorId() throws Exception {
        IgniteRunnable job = new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ign;

            @Override public void run() {
                ((IgniteEx)ign).context().query().querySqlFields(
                    new SqlFieldsQuery("SELECT test.awaitLatch()"), false).getAll();
            }
        };

        grid(1).cluster().forServers().ignite().compute().runAsync(job);

        String initiatorId = initiatorId(grid(0), "awaitLatch", 1000);

        assertTrue("Invalid initiator ID: " + initiatorId,
            initiatorId.startsWith("task:" + job.getClass().getName()) &&
                initiatorId.endsWith(grid(1).context().localNodeId().toString()));

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 1000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcV2InitiatorId() throws Exception {
        final UUID grid0NodeId = grid(0).cluster().localNode().id();

        GridTestUtils.runAsync(() -> {
                try (Connection conn = DriverManager.getConnection(
                    CFG_URL_PREFIX + "nodeId=" + grid0NodeId + "@modules/clients/src/test/config/jdbc-config.xml")) {
                    try (Statement stmt = conn.createStatement()) {
                        try (ResultSet rs = stmt.executeQuery("SELECT test.awaitLatch()")) {
                            // Read results: workaround for leak cursor GG-27123
                            // Remove after fix.
                            while (rs.next())
                                ;
                        }
                    }
                }
                catch (SQLException e) {
                    log.error("Unexpected exception", e);
                }
            }
        );

        String initiatorId = initiatorId(grid(0), "awaitLatch", 5000);

        assertTrue("Invalid initiator ID: " + initiatorId,
            Pattern.compile("jdbc-v2:127\\.0\\.0\\.1").matcher(initiatorId).matches());

        TestSQLFunctions.reset();

        checkRunningQueriesCount(grid(0), 0, 2000);
    }

    /**
     * @param node Ignite target node where query must be executed.
     * @param sqlMatch string to match SQL query with specified initiator ID.
     * @param timeout Timeout.
     * @return initiator ID.
     */
    private String initiatorId(IgniteEx node, String sqlMatch, int timeout) throws Exception {
        long t0 = U.currentTimeMillis();

        while (true) {
            if (U.currentTimeMillis() - t0 > timeout)
                fail("Timeout. Cannot find query with: " + sqlMatch);

            List<List<?>> res = node.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT sql, initiator_id FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

            for (List<?> row : res) {
                if (((String)row.get(0)).toUpperCase().contains(sqlMatch.toUpperCase()))
                    return (String)row.get(1);
            }

            U.sleep(200);
        }
    }

    /**
     * @param node Noe to check running queries.
     * @param timeout Timeout to finish running queries.
     */
    private void checkRunningQueriesCount(IgniteEx node, int expectedQryCount, int timeout) {
        long t0 = U.currentTimeMillis();

        while (true) {
            List<List<?>> res = node.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

            if (res.size() == expectedQryCount + 1)
                return;

            if (U.currentTimeMillis() - t0 > timeout) {
                fail("Timeout. There are unexpected running queries: " + res);
            }
        }
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /**
         *
         */
        static volatile CountDownLatch latch = new CountDownLatch(1);

        /**
         * Recreate latches. Old latches are released.
         */
        static void reset() {
            CountDownLatch latchOld = latch;

            latch = new CountDownLatch(1);

            latchOld.countDown();
        }

        /**
         * Releases cancelLatch that leeds to sending cancel Query and waits until cancel Query is fully processed.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long awaitLatch() {
            try {
                latch.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }
    }
}
