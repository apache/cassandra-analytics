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

package org.apache.cassandra.cdc.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cdc.msg.CdcEvent;

import static org.apache.cassandra.spark.utils.ArrayUtils.listOf;

public interface TopicSupplier
{
    String topic(CdcEvent event);

    /**
     * @param topic static string for an unchanging topic name
     * @return a static topic provider that always returns the same topic name
     */
    static StaticTopicSupplier staticTopicSupplier(String topic)
    {
        return new StaticTopicSupplier(topic);
    }

    /**
     * @param format topic string format like "org.apple.ase.%s"
     * @return a topic provider that formats a string template to include the keyspace name
     */
    static PerKeyspace keyspaceSupplier(String format)
    {
        return new PerKeyspace(format);
    }

    /**
     * @param format topic string format like "org.apple.ase.%s.%s"
     * @return a topic provider that formats a string template to include the keyspace and table name
     */
    static PerKeyspaceTable keyspaceTableSupplier(String format)
    {
        return new PerKeyspaceTable(format);
    }

    /**
     * @param format topic string format like "org.apple.ase.%s"
     * @return a topic provider that formats a string template to include the table name
     */
    static PerTable tableSupplier(String format)
    {
        return new PerTable(format);
    }

    static MapTopicSupplier mapSupplier(String format)
    {
        return new MapTopicSupplier(format);
    }

    abstract class StringFormatTopicSupplier implements TopicSupplier
    {
        protected final String format;
        protected ThreadLocal<Map<List<String>, String>> cache = ThreadLocal.withInitial(HashMap::new);

        protected StringFormatTopicSupplier(String format)
        {
            this.format = format;
        }

        protected abstract List<String> args(CdcEvent event);

        public String topic(CdcEvent event)
        {
            return cache.get().computeIfAbsent(args(event), args -> String.format(format, args.toArray()));
        }
    }

    class PerKeyspace extends StringFormatTopicSupplier
    {
        protected PerKeyspace(String format)
        {
            super(format);
        }

        protected List<String> args(CdcEvent event)
        {
            return listOf(event.keyspace);
        }
    }

    class PerKeyspaceTable extends StringFormatTopicSupplier
    {
        protected PerKeyspaceTable(String format)
        {
            super(format);
        }

        protected List<String> args(CdcEvent event)
        {
            return listOf(event.keyspace, event.table);
        }
    }

    class PerTable extends StringFormatTopicSupplier
    {
        protected PerTable(String format)
        {
            super(format);
        }

        protected List<String> args(CdcEvent event)
        {
            return listOf(event.table);
        }
    }

    class StaticTopicSupplier implements TopicSupplier
    {
        private final String topic;

        StaticTopicSupplier(String topic)
        {
            this.topic = topic;
        }

        public String topic(CdcEvent event)
        {
            return topic;
        }
    }

    class MapTopicSupplier implements TopicSupplier
    {
        private final Map<String, String> topicsMap;

        MapTopicSupplier(String topicsMapString)
        {
            ObjectMapper mapper = new ObjectMapper();

            try
            {
                topicsMap = mapper.readValue(topicsMapString, new TypeReference<HashMap<String, String>>()
                {
                });
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public String topic(CdcEvent event)
        {
            return topicsMap.get(event.keyspace + '.' + event.table);
        }
    }
}
