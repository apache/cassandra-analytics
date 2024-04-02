/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.cql3.functions.types;

import java.util.Collection;

import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jetbrains.annotations.NotNull;

/**
 * Helper methods to access package-private UDT methods
 */
public final class UserTypeHelper
{
    private UserTypeHelper()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @NotNull
    public static UDTValue newUDTValue(UserType userType)
    {
        return new UDTValue(userType);
    }

    @NotNull
    public static UserType newUserType(String keyspace,
                                       String typeName,
                                       boolean frozen,
                                       Collection<UserType.Field> fields,
                                       ProtocolVersion protocolVersion)
    {
        return new UserType(keyspace, typeName, frozen, fields, protocolVersion, CqlType.CODEC_REGISTRY);
    }

    @NotNull
    public static UserType.Field newField(String name, DataType type)
    {
        return new UserType.Field(name, type);
    }
}
