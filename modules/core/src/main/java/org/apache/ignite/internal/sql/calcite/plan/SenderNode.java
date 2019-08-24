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
package org.apache.ignite.internal.sql.calcite.plan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.sql.calcite.rels.IgnitePlanVisitor;

/**
 * TODO: Add class description.
 */
public class SenderNode implements PlanNode {
    private PlanNode input;
    private SenderType type;
    private int linkId;
    private List<Integer> distKeys;

    public SenderNode() {
    }

    public SenderNode(PlanNode input, SenderType type, int linkId, List<Integer>  distKeys) {
        this.input = input;
        this.type = type;
        this.linkId = linkId;
        this.distKeys = distKeys;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type.ordinal());
        out.writeInt(linkId);
        out.writeObject(distKeys);
        out.writeObject(input);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = SenderType.values()[in.readInt()];
        linkId = in.readInt();
        distKeys = (List<Integer>)in.readObject();
        input = (PlanNode)in.readObject();
    }


    @Override public String toString(int level) {
        String margin = String.join("", Collections.nCopies(level, "  "));

        StringBuilder sb = new StringBuilder("\n");

        sb.append(margin)
            .append("SenderNode [linkId=")
            .append(linkId)
            .append(", type=")
            .append(type)
            .append(", distKeys=")
            .append(distKeys)
            .append("]")
            .append(input.toString(level + 1));

        return sb.toString();
    }

    @Override public void accept(IgnitePlanVisitor visitor) {
        visitor.onSender(this);
    }

    @Override public List<PlanNode> inputs() {
        return Collections.singletonList(input);
    }

    public PlanNode input() {
        return input;
    }

    public SenderType type() {
        return type;
    }

    public int linkId() {
        return linkId;
    }

    public List<Integer> distKeys() {
        return distKeys;
    }

    @Override public String toString() {
        return toString(0);
    }

    public enum SenderType {
        BROADCAST,
        SINGLE,
        HASH
    }
}