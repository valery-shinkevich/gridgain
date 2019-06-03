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

#include <ignite/cluster/cluster_group.h>
#include <ignite/cluster/cluster_node.h>

#include <ignite/impl/cluster/cluster_node_impl.h>
#include "ignite/impl/cluster/cluster_group_impl.h"

using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::jni::java;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {

            /** Attribute: platform. */
            const std::string attrPlatform = "org.apache.ignite.platform";

            /** Platform. */
            const std::string platform = "cpp";

            struct Command
            {
                enum Type
                {
                    FOR_ATTRIBUTE = 2,

                    FOR_DATA = 5,

                    NODES = 12,

                    FOR_SERVERS = 23,

                    SET_ACTIVE = 28,

                    IS_ACTIVE = 29
                };
            };

            ClusterGroupImpl::ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef) :
                InteropTarget(env, javaRef), nodes(new std::vector<ClusterNode>()), topVer(0)
            {
                computeImpl = InternalGetCompute();
            }

            ClusterGroupImpl::~ClusterGroupImpl()
            {
                // No-op.
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForAttribute(std::string name, std::string val)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteString(name);
                writer.WriteString(val);

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(Command::FOR_ATTRIBUTE, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForDataNodes(std::string cacheName)
            {
                return ForCacheNodes(cacheName, Command::FOR_DATA);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForServers()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_SERVERS, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForCpp()
            {
                return ForAttribute(attrPlatform, platform);
            }

            ClusterGroupImpl::SP_ComputeImpl ClusterGroupImpl::GetCompute()
            {
                return computeImpl;
            }

            ClusterGroupImpl::SP_ComputeImpl ClusterGroupImpl::GetCompute(ClusterGroup grp)
            {
                return grp.GetImpl().Get()->GetCompute();
            }

            std::vector<ClusterNode> ClusterGroupImpl::GetNodes()
            {
                return RefreshNodes();
            }

            bool ClusterGroupImpl::IsActive()
            {
                IgniteError err;

                int64_t res = OutInOpLong(Command::IS_ACTIVE, 0, err);

                IgniteError::ThrowIfNeeded(err);

                return res == 1;
            }

            void ClusterGroupImpl::SetActive(bool active)
            {
                IgniteError err;

                OutInOpLong(Command::SET_ACTIVE, active ? 1 : 0, err);

                IgniteError::ThrowIfNeeded(err);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForCacheNodes(std::string name, int32_t op)
            {
                SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(mem.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteString(name);

                out.Synchronize();

                IgniteError err;
                jobject target = InStreamOutObject(op, *mem.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), target));
            }

            SP_ClusterGroupImpl ClusterGroupImpl::FromTarget(jobject javaRef)
            {
                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), javaRef));
            }

            ClusterGroupImpl::SP_ComputeImpl ClusterGroupImpl::InternalGetCompute()
            {
                jobject computeProc = GetEnvironment().GetProcessorCompute(GetTarget());

                return SP_ComputeImpl(new compute::ComputeImpl(GetEnvironmentPointer(), computeProc));
            }

            std::vector<ClusterNode> ClusterGroupImpl::RefreshNodes()
            {
                CsLockGuard mtx(nodesLock);

                SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();
                interop::InteropOutputStream out(memIn.Get());
                binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                writer.WriteInt64(topVer);

                out.Synchronize();

                IgniteError err;
                InStreamOutStream(Command::NODES, *memIn.Get(), *memOut.Get(), err);
                IgniteError::ThrowIfNeeded(err);

                interop::InteropInputStream inStream(memOut.Get());
                binary::BinaryReaderImpl reader(&inStream);

                bool wasUpdated = reader.ReadBool();
                if (wasUpdated)
                {
                    topVer = reader.ReadInt64();
                    int cnt = reader.ReadInt32();

                    SP_ClusterNodes newNodes(new std::vector<ClusterNode>());
                    for (int i = 0; i < cnt; i++)
                    {
                        SP_ClusterNodeImpl impl = GetEnvironment().GetNode(reader.ReadGuid());
                        if (impl.IsValid())
                            newNodes.Get()->push_back(ClusterNode(impl));
                    }

                    nodes = newNodes;

                    return *nodes.Get();
                }

                return *nodes.Get();
            }
        }
    }
}

