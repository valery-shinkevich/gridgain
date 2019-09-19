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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_MISSED;

/**
 * The test is checking multiple demander supplying from non-affinity owner.
 */
public class NotAffinitySupplierWithMultipalRebalanceTest extends GridCommonAbstractTest {
    /** Start cluster nodes. */
    public static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setBackups(2)
                    .setAffinity(new TestAffinity(4)));
    }

    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSupplingOldBackup() throws Exception {
        try {
            IgniteEx ignite0 = startGrids(NODES_CNT);

            ignite0.cluster().active(true);
            ignite0.cluster().baselineAutoAdjustEnabled(false);

            TestRecordingCommunicationSpi testCommunicationSpi0 = (TestRecordingCommunicationSpi)ignite0
                .configuration().getCommunicationSpi();

            laodData(ignite0, DEFAULT_CACHE_NAME);

            awaitPartitionMapExchange();

            TestRecordingCommunicationSpi testCommunicationSpi1 = startNodeWithBlockingRebalance("new_1");
            TestRecordingCommunicationSpi testCommunicationSpi2 = startNodeWithBlockingRebalance("new_2");
            TestRecordingCommunicationSpi testCommunicationSpi3 = startNodeWithBlockingRebalance("new_3");

            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

            testCommunicationSpi1.waitForBlocked();
            testCommunicationSpi2.waitForBlocked();
            testCommunicationSpi3.waitForBlocked();

            AtomicBoolean hasMissed = new AtomicBoolean();

            ignite0.events().localListen(event -> {
                info("Partition missing event: " + event);

                hasMissed.compareAndSet(false, true);

                return false;
            }, EVT_CACHE_REBALANCE_PART_MISSED);

            testCommunicationSpi0.record(GridDhtPartitionsFullMessage.class);

            testCommunicationSpi1.stopBlock();
            testCommunicationSpi2.stopBlock();

            testCommunicationSpi0.waitForRecorded();

            testCommunicationSpi3.stopBlock();

            awaitPartitionMapExchange();

            assertFalse(hasMissed.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param name Node instance name.
     * @return Test communication SPI.
     * @throws Exception If failed.
     */
    @NotNull private TestRecordingCommunicationSpi startNodeWithBlockingRebalance(String name) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(name));

        TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        communicationSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage)msg;

                if (CU.cacheId(DEFAULT_CACHE_NAME) != demandMessage.groupId())
                    return false;

                info("Message was caught: " + msg.getClass().getSimpleName()
                    + " to: " + node.consistentId()
                    + " by chache: " + DEFAULT_CACHE_NAME);

                return true;
            }

            return false;
        });

        Ignite ignite1 = startGrid(cfg);
        return communicationSpi;
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void laodData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++)
                streamer.addData(i, System.nanoTime());
        }
    }

    /** The test's affinity which mowing all partitions. */
    private static class TestAffinity extends RendezvousAffinityFunction {
        /**
         * @param parts Partitions.
         */
        public TestAffinity(int parts) {
            super(false, parts);
        }

        /** {@inheritDoc} */
        @Override public List<ClusterNode> assignPartition(int part, List<ClusterNode> nodes, int backups,
            @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
            if (backups == 2 && nodes.size() == NODES_CNT + 3) {
                ClusterNode[] list = new ClusterNode[3];

                for (ClusterNode node : nodes) {
                    if (node.consistentId().equals("new_1"))
                        list[part % 3] = node;
                    else if (node.consistentId().equals("new_2"))
                        list[part % 3] = node;
                    else if (node.consistentId().equals("new_3"))
                        list[part % 3] = node;
                }

                if (list[0] != null && list[1] != null && list[2] != null)
                    return Arrays.asList(list);

            } else if (backups == 2 && nodes.size() == NODES_CNT) {
                ClusterNode[] list = new ClusterNode[3];

                for (ClusterNode node : nodes) {
                    if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest2"))
                        list[0] = node;
                    else if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest1"))
                        list[1] = node;
                    else if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest0"))
                        list[2] = node;
                }

                if (list[0] != null && list[1] != null && list[2] != null)
                    return Arrays.asList(list);
            }

            return super.assignPartition(part, nodes, backups, neighborhoodCache);
        }
    }
}