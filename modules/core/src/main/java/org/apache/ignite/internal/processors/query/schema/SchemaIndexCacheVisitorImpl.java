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

package org.apache.ignite.internal.processors.query.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheStat.extraIndexBuildLogging;

/**
 * Visitor who create/rebuild indexes in parallel by partition for a given cache.
 */
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /** Future for create/rebuild index. */
    protected final GridFutureAdapter<Void> buildIdxFut;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cancel Cancellation token.
     * @param buildIdxFut Future for create/rebuild index.
     */
    public SchemaIndexCacheVisitorImpl(
        GridCacheContext cctx,
        SchemaIndexOperationCancellationToken cancel,
        GridFutureAdapter<Void> buildIdxFut
    ) {
        assert nonNull(cctx);
        assert nonNull(buildIdxFut);

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;
        this.buildIdxFut = buildIdxFut;

        this.cancel = cancel;

        this.log = cctx.logger(SchemaIndexCacheVisitorImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
        assert nonNull(clo);

        List<GridDhtLocalPartition> locParts = cctx.topology().localPartitions();

        cctx.group().metrics().setIndexBuildCountPartitionsLeft(locParts.size());

        if (locParts.isEmpty()) {
            buildIdxFut.onDone();

            return;
        }

        AtomicBoolean stop = new AtomicBoolean();

        GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat> buildIdxCompoundFut =
            new GridCompoundFuture<>();

        for (GridDhtLocalPartition locPart : locParts) {
            GridWorkerFuture<SchemaIndexCacheStat> workerFut = new GridWorkerFuture<>();

            GridWorker worker = new SchemaIndexCachePartitionWorker(cctx, locPart, stop, cancel, clo, workerFut);

            workerFut.setWorker(worker);
            buildIdxCompoundFut.add(workerFut);

            cctx.kernalContext().buildIndexExecutorService().execute(worker);
        }

        buildIdxCompoundFut.listen(fut -> {
            Throwable err = fut.error();

            if (isNull(err) && extraIndexBuildLogging() && log.isInfoEnabled()) {
                try {
                    GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat> compoundFut =
                        (GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat>)fut;

                    SchemaIndexCacheStat resStat = new SchemaIndexCacheStat();

                    compoundFut.futures().stream()
                        .map(IgniteInternalFuture::result)
                        .filter(Objects::nonNull)
                        .forEach(stat -> {
                            resStat.scanned += stat.scanned;
                            resStat.types.putAll(stat.types);
                        });

                    log.info(indexStatStr(resStat));
                }
                catch (Exception e) {
                    log.error("Error when trying to print index build/rebuild statistics [cacheName=" +
                        cctx.cache().name() + ", grpName=" + cctx.group().name() + "]", e);
                }
            }

            buildIdxFut.onDone(err);
        });

        buildIdxCompoundFut.markInitialized();
    }

    /**
     * Prints index cache stats to log.
     *
     * @param stat Index cache stats.
     * @throws IgniteCheckedException if failed to get index size.
     */
    private String indexStatStr(SchemaIndexCacheStat stat) throws IgniteCheckedException {
        SB res = new SB();

        res.a("Details for cache rebuilding [name=" + cctx.cache().name() + ", grpName=" + cctx.group().name() + ']');
        res.a(U.nl());
        res.a("   Scanned rows " + stat.scanned + ", visited types " + stat.types.keySet());
        res.a(U.nl());

        final GridQueryIndexing idx = cctx.kernalContext().query().getIndexing();

        for (QueryTypeDescriptorImpl type : stat.types.values()) {
            res.a("        Type name=" + type.name());
            res.a(U.nl());

            final String pk = "_key_PK";

            res.a("            Index: name=" + pk + ", size=" + idx.indexSize(type.schemaName(), pk));
            res.a(U.nl());

            final Map<String, GridQueryIndexDescriptor> indexes = type.indexes();

            for (GridQueryIndexDescriptor descriptor : indexes.values()) {
                final long size = idx.indexSize(type.schemaName(), descriptor.name());

                res.a("            Index: name=" + descriptor.name() + ", size=" + size);
                res.a(U.nl());
            }
        }

        return res.toString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }
}
