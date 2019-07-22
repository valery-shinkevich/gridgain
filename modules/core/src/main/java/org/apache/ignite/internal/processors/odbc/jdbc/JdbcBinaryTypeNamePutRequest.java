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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.MarshallerPlatformIds;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC put binary type name request.
 */
public class JdbcBinaryTypeNamePutRequest extends JdbcRequest {
    /** ID of binary type. */
    private int typeId;

    /** ID of platform. */
    private byte platformId;

    /** Type name. */
    private String typeName;

    /** Default constructor for deserialization purpose. */
    JdbcBinaryTypeNamePutRequest() {
        super(BINARY_TYPE_NAME_PUT);
    }

    /**
     * @param typeId ID of binary type.
     * @param platformId ID of platform. See {@link MarshallerPlatformIds} for supported values.
     * @param typeName name of the new binary type.
     */
    public JdbcBinaryTypeNamePutRequest(int typeId, byte platformId, String typeName) {
        super(BINARY_TYPE_NAME_PUT);

        this.typeId = typeId;
        this.platformId = platformId;
        this.typeName = typeName;
    }

    /**
     * Returns ID of binary type.
     *
     * @return ID of binary type.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * Returns ID of platform.
     *
     * @return ID of platform.
     */
    public byte platformId() {
        return platformId;
    }

    /**
     * Returns type name.
     *
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }


    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeInt(typeId);
        writer.writeByte(platformId);
        writer.writeString(typeName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        typeId = reader.readInt();
        platformId = reader.readByte();
        typeName = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBinaryTypeNamePutRequest.class, this);
    }
}