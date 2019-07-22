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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.io.IOException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC put binary type schema request.
 */
public class JdbcBinaryTypePutRequest extends JdbcRequest {
    /** Schema of binary type. */
    private BinaryMetadata meta;

    /** Default constructor for deserialization purpose. */
    JdbcBinaryTypePutRequest() {
        super(BINARY_TYPE_PUT);
    }

    /**
     * @param meta Schema of binary type.
     */
    public JdbcBinaryTypePutRequest(BinaryMetadata meta) {
        super(BINARY_TYPE_PUT);

        this.meta = meta;
    }

    /**
     * Returns schema of binary type.
     *
     * @return Schema of binary type.
     */
    public BinaryMetadata meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        try {
            meta.writeTo(writer);
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        meta = new BinaryMetadata();
        try {
            meta.readFrom(reader);
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBinaryTypePutRequest.class, this);
    }
}