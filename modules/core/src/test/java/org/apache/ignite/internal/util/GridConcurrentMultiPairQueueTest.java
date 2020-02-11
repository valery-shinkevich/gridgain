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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * GridConcurrentMultiPairQueue test.
 **/
public class GridConcurrentMultiPairQueueTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testGridConcurrentMultiPairQueueCorrectness() throws Exception {
        Integer[] arr2 = new Integer[]{2, 4};
        Integer[] arr1 = new Integer[]{1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
        Integer[] arr4 = new Integer[]{};
        Integer[] arr5 = new Integer[]{};
        Integer[] arr3 = new Integer[]{100, 200, 300, 400, 500, 600, 600, 700};
        Integer[] arr6 = new Integer[]{};

        Collection<T2<Integer, Integer[]>> keyWithArr = new HashSet<>();

        final Map<Integer, Collection<Integer>> mapForCheck = new ConcurrentHashMap<>();

        final Map<Integer, Collection<Integer>> mapForCheck2 = new ConcurrentHashMap<>();

        keyWithArr.add(new T2<>(10, arr2));
        keyWithArr.add(new T2<>(20, arr1));
        keyWithArr.add(new T2<>(30, arr4));
        keyWithArr.add(new T2<>(40, arr5));
        keyWithArr.add(new T2<>(50, arr3));
        keyWithArr.add(new T2<>(60, arr6));

        mapForCheck.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        mapForCheck2.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck2.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck2.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        final GridConcurrentMultiPairQueue<Integer, Integer> queue = new GridConcurrentMultiPairQueue<>(keyWithArr);

        GridTestUtils.runMultiThreaded(() -> {
            GridConcurrentMultiPairQueue.Result<Integer, Integer> res =
                new GridConcurrentMultiPairQueue.Result<>();

            queue.next(res);

            while (res.getKey() != null) {
                assertTrue(mapForCheck.containsKey(res.getKey()));

                mapForCheck.get(res.getKey()).remove(res.getValue());

                Collection<Integer> coll = mapForCheck.get(res.getKey());

                if (coll != null && coll.isEmpty())
                    mapForCheck.remove(res.getKey(), coll);

                queue.next(res);
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue arr test");

        assertTrue(mapForCheck.isEmpty());

        assertTrue(queue.isEmpty());

        assertTrue(queue.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);

        Map<Integer, Collection<Integer>> keyWithColl = new HashMap<>();

        keyWithColl.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        keyWithColl.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        keyWithColl.put(30, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr4))));
        keyWithColl.put(40, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr5))));
        keyWithColl.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));
        keyWithColl.put(60, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr6))));

        final GridConcurrentMultiPairQueue<Integer, Integer> queue2 = new GridConcurrentMultiPairQueue<>(keyWithColl);

        GridTestUtils.runMultiThreaded(() -> {
            GridConcurrentMultiPairQueue.Result<Integer, Integer> res =
                new GridConcurrentMultiPairQueue.Result<>();

            queue2.next(res);

            while (res.getKey() != null) {
                assertTrue(mapForCheck2.containsKey(res.getKey()));

                mapForCheck2.get(res.getKey()).remove(res.getValue());

                Collection<Integer> coll = mapForCheck2.get(res.getKey());

                if (coll != null && coll.isEmpty())
                    mapForCheck2.remove(res.getKey(), coll);

                queue2.next(res);
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue coll test");

        assertTrue(mapForCheck2.isEmpty());

        assertTrue(queue2.isEmpty());

        assertTrue(queue2.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);
    }

    /** */
    @Test
    public void testGridConcurrentMultiPairQueueCorrectness1() throws Exception {
        Integer[] arr2 = new Integer[]{2, 4};
        Integer[] arr1 = new Integer[]{1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
        Integer[] arr4 = new Integer[]{};
        Integer[] arr5 = new Integer[]{};
        Integer[] arr3 = new Integer[]{100, 200, 300, 400, 500, 600, 600, 700};
        Integer[] arr6 = new Integer[]{};

        Collection<T2<Integer, Integer[]>> keyWithArr = new HashSet<>();

        final Map<Integer, Collection<Integer>> mapForCheck = new ConcurrentHashMap<>();

        final Map<Integer, Collection<Integer>> mapForCheck2 = new ConcurrentHashMap<>();

        keyWithArr.add(new T2<>(10, arr2));
        keyWithArr.add(new T2<>(20, arr1));
        keyWithArr.add(new T2<>(30, arr4));
        keyWithArr.add(new T2<>(40, arr5));
        keyWithArr.add(new T2<>(50, arr3));
        keyWithArr.add(new T2<>(60, arr6));

        mapForCheck.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        mapForCheck2.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck2.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck2.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        final GridConcurrentMultiPairQueue<Integer, Integer> queue = new GridConcurrentMultiPairQueue<>(keyWithArr);

        GridTestUtils.runMultiThreaded(() -> {
            T2<Integer, Integer> pair = queue.poll();

            while (pair != null) {
                assertTrue(mapForCheck.containsKey(pair.get1()));

                mapForCheck.get(pair.get1()).remove(pair.get2());

                if (mapForCheck.get(pair.get1()).isEmpty())
                    mapForCheck.remove(pair.get1());

                pair = queue.poll();
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue arr test");

        assertTrue(mapForCheck.isEmpty());

        assertTrue(queue.isEmpty());

        assertTrue(queue.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);

        Map<Integer, Collection<Integer>> keyWithColl = new HashMap<>();

        keyWithColl.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        keyWithColl.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        keyWithColl.put(30, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr4))));
        keyWithColl.put(40, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr5))));
        keyWithColl.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));
        keyWithColl.put(60, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr6))));

        final GridConcurrentMultiPairQueue<Integer, Integer> queue2 = new GridConcurrentMultiPairQueue<>(keyWithColl);

        GridTestUtils.runMultiThreaded(() -> {
            T2<Integer, Integer> pair = queue2.poll();

            while (pair != null) {
                assertTrue(mapForCheck2.containsKey(pair.get1()));

                mapForCheck2.get(pair.get1()).remove(pair.get2());

                if (mapForCheck2.get(pair.get1()).isEmpty())
                    mapForCheck2.remove(pair.get1());

                pair = queue2.poll();
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue coll test");

        assertTrue(mapForCheck2.isEmpty());

        assertTrue(queue2.isEmpty());

        assertTrue(queue2.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);
    }
}
