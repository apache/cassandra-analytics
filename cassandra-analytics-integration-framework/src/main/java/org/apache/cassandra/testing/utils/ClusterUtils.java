/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.testing.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

/**
 * Utilities for working with jvm-dtest clusters.
 *
 * <p>This class should never be called from within the cluster, always in the App ClassLoader.
 *
 * <p>Parts of this class are copied from Cassandra's {@code org.apache.cassandra.distributed.shared.ClusterUtils}
 * class
 */
public final class ClusterUtils
{
    private ClusterUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Start the instance with the given System Properties, after the instance has started,
     * the properties will be cleared.
     *
     * @param instance the instance
     * @param fn       the consumer function
     * @param <I>      the concrete instance type
     * @return the started instance
     */
    public static <I extends IInstance> I start(I instance, Consumer<WithProperties> fn)
    {
        return start(instance, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Start the instance with the given System Properties, after the instance has started,
     * the properties will be cleared.
     *
     * @param instance the instance
     * @param fn       the bi-consumer function
     * @param <I>      the concrete instance type
     * @return the started instance
     */
    public static <I extends IInstance> I start(I instance, BiConsumer<I, WithProperties> fn)
    {
        try (WithProperties properties = new WithProperties())
        {
            fn.accept(instance, properties);
            instance.startup();
            return instance;
        }
    }

    /**
     * Get the ring from the perspective of the instance.
     *
     * @param instance the instance
     * @return a list with the parsed ring results
     */
    public static List<RingInstanceDetails> ring(IInstance instance)
    {
        NodeToolResult results = instance.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }


    private static List<RingInstanceDetails> parseRing(String str)
    {
        // 127.0.0.3  rack0       Up     Normal  46.21 KB        100.00%             -1
        // /127.0.0.1:7012  Unknown     ?      Normal  ?               100.00%             -3074457345618258603
        Pattern pattern = Pattern.compile("^(/?[0-9.:]+)\\s+(\\w+|\\?)\\s+(\\w+|\\?)\\s+(\\w+|\\?).*?(-?\\d+)\\s*$");
        List<RingInstanceDetails> details = new ArrayList<>();
        String[] lines = str.split("\n");
        for (String line : lines)
        {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find())
            {
                continue;
            }
            details.add(new RingInstanceDetails(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5)));
        }

        return details;
    }


    public static final class RingInstanceDetails
    {
        private final String address;
        private final String rack;
        private final String status;
        private final String state;
        private final String token;

        private RingInstanceDetails(String address, String rack, String status, String state, String token)
        {
            this.address = address;
            this.rack = rack;
            this.status = status;
            this.state = state;
            this.token = token;
        }

        public String getAddress()
        {
            return address;
        }

        public String getStatus()
        {
            return status;
        }

        public String getState()
        {
            return state;
        }

        public String getToken()
        {
            return token;
        }

        @Override
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
            RingInstanceDetails that = (RingInstanceDetails) o;
            return Objects.equals(address, that.address) &&
                   Objects.equals(rack, that.rack) &&
                   Objects.equals(status, that.status) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(token, that.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, rack, status, state, token);
        }

        public String toString()
        {
            return Arrays.asList(address, rack, status, state, token).toString();
        }
    }
}
