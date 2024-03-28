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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * The BridgeUdtValue class exists because the Cassandra values produced (UDTValue) are not serializable
 * because they come from the classloader inside the bridge, and therefore can't be passed around
 * from one Spark phase to another. Therefore, we build a map of these instances (potentially nested)
 * and return them from the conversion stage for later use when the writer actually writes them.
 */
public class BridgeUdtValue implements Serializable
{
    public final String name;
    public final Map<String, Object> udtMap;

    public BridgeUdtValue(String name, Map<String, Object> valueMap)
    {
        this.name = name;
        this.udtMap = valueMap;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        BridgeUdtValue udtValue = (BridgeUdtValue) o;
        return Objects.equals(name, udtValue.name) && Objects.equals(udtMap, udtValue.udtMap);
    }

    public int hashCode()
    {
        return Objects.hash(name, udtMap);
    }

    public String toString()
    {
        return "BridgeUdtValue{" +
               "name='" + name + '\'' +
               ", udtMap=" + udtMap +
               '}';
    }
}
