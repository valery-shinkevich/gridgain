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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class PreparedStatementExSelfTest extends AbstractIndexingCommonTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStoringMeta() throws Exception {
        PreparedStatement stmt = stmt();

        PreparedStatementEx wrapped = stmt.unwrap(PreparedStatementEx.class);

        wrapped.putMeta(0, "0");

        assertEquals("0", wrapped.meta(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStoringMoreMetaKeepsExisting() throws Exception {
        PreparedStatement stmt = stmt();

        PreparedStatementEx wrapped = stmt.unwrap(PreparedStatementEx.class);

        wrapped.putMeta(0, "0");
        wrapped.putMeta(1, "1");

        assertEquals("0", wrapped.meta(0));
        assertEquals("1", wrapped.meta(1));
    }

    /**
     *
     */
    private static PreparedStatement stmt() {
        return new PreparedStatementExImpl(null);
    }
}