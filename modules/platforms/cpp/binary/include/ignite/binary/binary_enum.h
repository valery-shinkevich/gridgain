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

/**
 * @file
 * Declares ignite::binary::BinaryEnum class template and helping macros
 * to declare enum type specialization for user types.
 */

#ifndef _IGNITE_BINARY_BINARY_ENUM
#define _IGNITE_BINARY_BINARY_ENUM

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/binary/binary_type.h>

namespace ignite
{
    namespace binary
    {
        /**
         * Binary enum structure. Defines a set of functions required for enum type to be serialized and deserialized.
         *
         * Methods that should be defined:
         *   static void GetTypeName(std::string& dst) - should place type name in @c dst parameter. This method should
         *     always return the same value.
         *   static int32_t GetTypeId() - should return Type ID.
         *   static int32_t GetOrdinal(T) - should return ordinal value for enum value.
         *   static T FromOrdinal(int32_t) - should return enum value for a given ordinal value.
         *   static bool IsNull(T) - check whether passed enum object should be interpreted as NULL.
         *   static T GetNull() - get NULL value for the given enum type.
         *
         * It is recommended to use BinaryEnumDefault as a base class for default implementation of GetTypeId(),
         * GetOrdinal() and FromOrdinal() methods for plain enum types. In this case, only GetTypeName() should be
         * implemented by a user directly.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryEnum { };

        /**
         * Default implementations of BinaryEnum.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryEnumDefault
        {
            /**
             * Get type ID for the enum type.
             *
             * @return Type ID.
             */
            static int32_t GetTypeId()
            {
                std::string typeName;
                BinaryEnum<T>::GetTypeName(typeName);

                return GetBinaryStringHashCode(typeName.c_str());
            }

            /**
             * Get enum type ordinal.
             *
             * @return Ordinal of the enum type.
             */
            static int32_t GetOrdinal(T value)
            {
                return static_cast<int32_t>(value);
            }

            /**
             * Get enum value for the given ordinal value.
             *
             * @param ordinal Ordinal value of the enum.
             */
            static T FromOrdinal(int32_t ordinal)
            {
                return static_cast<T>(ordinal);
            }
        };

        /**
         * Default implementations of BinaryType hashing functions and non-null type behaviour.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryEnumDefaultAll :
            BinaryEnumDefault<T>,
            BinaryTypeNonNullableType<T> { };

        /**
         * BinaryEnum template specialization for pointers.
         */
        template <typename T>
        struct BinaryEnum<T*>
        {
            /** Actual type. */
            typedef BinaryEnum<T> BinaryEnumDereferenced;

            /**
             * Get binary object type ID.
             *
             * @return Type ID.
             */
            static int32_t GetTypeId()
            {
                return BinaryEnumDereferenced::GetTypeId();
            }

            /**
             * Get binary object type name.
             *
             * @param dst Output type name.
             */
            static void GetTypeName(std::string& dst)
            {
                BinaryEnumDereferenced::GetTypeName(dst);
            }

            /**
             * Get enum type ordinal.
             *
             * @return Ordinal of the enum type.
             */
            static int32_t GetOrdinal(T* value)
            {
                return BinaryEnumDereferenced::GetOrdinal(*value);
            }

            /**
             * Get enum value for the given ordinal value.
             *
             * @param ordinal Ordinal value of the enum.
             */
            static T* FromOrdinal(int32_t ordinal)
            {
                return new T(BinaryEnumDereferenced::FromOrdinal(ordinal));
            }

            /**
             * Check whether passed enum should be interpreted as NULL.
             *
             * @param obj Enum value to test.
             * @return True if enum value should be interpreted as NULL.
             */
            static bool IsNull(T* const& obj)
            {
                return !obj || BinaryEnumDereferenced::IsNull(*obj);
            }

            /**
             * Get NULL value for the enum type.
             *
             * @param dst NULL value for the enum.
             */
            static void GetNull(T*& dst)
            {
                dst = 0;
            }
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_ENUM
