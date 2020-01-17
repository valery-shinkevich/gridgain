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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

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
    public void testUserDefinedOriginator() throws Exception {
        final String orig = "TestUserSpecifiedOriginator";

        GridTestUtils.runAsync(() ->
            grid(0).context().query().querySqlFields(
                new SqlFieldsQuery("SELECT test.awaitLatch()").setQueryInitiatorId(orig), false).getAll());

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll().size() == 2,
            1000));

        List<List<?>> res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT initiator_id FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

        assertEquals(orig, res.get(0).get(0));
        assertNull(res.get(1).get(0));

        TestSQLFunctions.reset();

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false)
                .getAll().size() == 1,
            1000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcOriginator() throws Exception {
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

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll().size() == 2,
            1000));

        List<List<?>> res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT initiator_id FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

        String originator = (String)res.get(0).get(0);

        assertTrue(Pattern.compile("jdbc:/127\\.0\\.0\\.1:[0-9]+@ignite").matcher(originator).matches());
        assertNull(res.get(1).get(0));

        TestSQLFunctions.reset();

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false)
                .getAll().size() == 1,
            1000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThinClientOriginator() throws Exception {
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

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll().size() == 2,
            1000));

        List<List<?>> res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT initiator_id FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

        String originator = (String)res.get(0).get(0);

        assertTrue(Pattern.compile("cli:/127\\.0\\.0\\.1:[0-9]+@ignite").matcher(originator).matches());
        assertNull(res.get(1).get(0));

        TestSQLFunctions.reset();

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false)
                .getAll().size() == 1,
            1000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobDefaultOriginator() throws Exception {
        IgniteRunnable job = new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ign;

            @Override public void run() {
                System.out.println(Thread.currentThread().getName() + " +++ ");
                ((IgniteEx)ign).context().query().querySqlFields(
                    new SqlFieldsQuery("SELECT test.awaitLatch()"), false).getAll();
            }
        };

        grid(1).cluster().forServers().ignite().compute().runAsync(job);

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll().size() == 2,
            1000));

        List<List<?>> res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT initiator_id FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

        String originator = (String)res.get(0).get(0);

        assertEquals(job.getClass().getName(), originator);
        assertNull(res.get(1).get(0));

        TestSQLFunctions.reset();

        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false)
                .getAll().size() == 1,
            1000));
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
            latch.countDown();

            latch = new CountDownLatch(1);
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
