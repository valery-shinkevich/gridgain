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

package org.apache.ignite.yardstick.cache.dml;

import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs putAll operations via SQL MERGE.
 */
public class IgniteSqlMergeAllBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Affinity mapper. */
    private Affinity<Integer> aff;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        aff = ignite().affinity("query");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Object[] vals = new Object[args.batch() * 2];

        ClusterNode node = args.collocated() ? aff.mapKeyToNode(nextRandom(args.range())) : null;

        StringBuilder qry = new StringBuilder("merge into Integer(_key, _val) values ");

        int j = 0;

        for (int i = 0; i < args.batch(); ) {
            int key = nextRandom(args.range());

            if (args.collocated() && !aff.isPrimary(node, key))
                continue;

            ++i;

            // Put two args, for key and value.
            vals[j++] = key;
            vals[j++] = key;

            qry.append("(?, ?),");
        }

        Arrays.sort(vals);

        // Trim trailing comma.
        qry = qry.deleteCharAt(qry.length() - 1);

        cache.query(new SqlFieldsQuery(qry.toString()).setArgs(vals));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }
}
