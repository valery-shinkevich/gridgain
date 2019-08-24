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
package org.apache.ignite.internal.sql.calcite.rels;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * TODO: Add class description.
 */
public class FilterRel extends Filter implements IgniteRel {

    protected FilterRel(RelOptCluster cluster, RelTraitSet traits,
        RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterRel(getCluster(), traitSet, input, condition);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("dist", getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE));
    }

    @Override public void accept(IgniteRelVisitor visitor) {
        visitor.onFilter(this);
    }
}