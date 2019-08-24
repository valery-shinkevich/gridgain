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
package org.apache.ignite.internal.sql.calcite.iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * TODO: Add class description.
 */
public abstract class PhysicalOperator extends GridFutureAdapter<List<List<?>>> {

    abstract  Iterator<List<?>> iterator(List<List<?>> ... input);

    protected void execute(List<List<?>> ... input ) {
        Iterator<List<?>> it = iterator(input);

        List<List<?>> all = new ArrayList<>();

        while (it.hasNext())
            all.add(it.next());

        onDone(all);
    }

    public abstract void init();
}