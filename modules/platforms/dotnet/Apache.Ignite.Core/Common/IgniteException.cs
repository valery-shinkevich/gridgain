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

namespace Apache.Ignite.Core.Common
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// General Ignite exception. Indicates any error condition within Ignite.
    /// </summary>
    [Serializable]
    public class IgniteException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        public IgniteException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public IgniteException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public IgniteException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected IgniteException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
            // No-op.
        }
    }
}
