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
package org.apache.ignite.internal.sql.calcite.expressions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * TODO: Add class description.
 */
public class FieldGetter implements Expression {
    private int idx;

    public FieldGetter() {
    }

    public FieldGetter(int idx) {
        this.idx = idx;
    }

    @Override public Object evaluate(List row) {
        return row.get(idx);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(idx);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        idx = in.readInt();
    }
}