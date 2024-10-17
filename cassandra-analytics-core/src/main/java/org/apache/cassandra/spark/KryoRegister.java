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

package org.apache.cassandra.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import org.apache.cassandra.bridge.BigNumberConfigImpl;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.jetbrains.annotations.NotNull;

/**
 * Helper class to register classes for Kryo serialization
 */
public class KryoRegister implements KryoRegistrator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KryoRegister.class);
    private static final String SPARK_SERIALIZER = "spark.serializer";
    private static final String SPARK_REGISTRATORS = "spark.kryo.registrator";
    private static final Map<Class<?>, Serializer<?>> KRYO_SERIALIZERS = Collections.synchronizedMap(new LinkedHashMap<>());

    static
    {
        // Cassandra-version-agnostic Kryo serializers
        KRYO_SERIALIZERS.put(LocalDataLayer.class, new LocalDataLayer.Serializer());
        KRYO_SERIALIZERS.put(CassandraInstance.class, CassandraInstance.SERIALIZER);
        KRYO_SERIALIZERS.put(ReplicationFactor.class, new ReplicationFactor.Serializer());
        KRYO_SERIALIZERS.put(CassandraRing.class, new CassandraRing.Serializer());
        KRYO_SERIALIZERS.put(TokenPartitioner.class, new TokenPartitioner.Serializer());
        KRYO_SERIALIZERS.put(CassandraDataLayer.class, new CassandraDataLayer.Serializer());
        KRYO_SERIALIZERS.put(BigNumberConfigImpl.class, new BigNumberConfigImpl.Serializer());
        KRYO_SERIALIZERS.put(SslConfig.class, new SslConfig.Serializer());
    }

    public static <T> void addSerializer(@NotNull Class<T> type, @NotNull Serializer<T> serializer)
    {
        LOGGER.info("Registering custom Kryo serializer type={}", type.getName());
        KRYO_SERIALIZERS.put(type, serializer);
    }

    @Override
    public void registerClasses(@NotNull Kryo kryo)
    {
        LOGGER.info("Initializing KryoRegister");

        // TODO: Implicitly defaulting to Cassandra version 4.0 is a part of a previously published API.
        //       We might want to persist the version of Cassandra into the Spark configuration instead.
        CassandraBridgeFactory.get(CassandraVersion.FOURZERO).kryoRegister(kryo);

        KRYO_SERIALIZERS.forEach(kryo::register);
    }

    public static void setup(@NotNull SparkConf configuration)
    {
        // Use KryoSerializer
        LOGGER.info("Setting up Kryo");
        configuration.set(SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");

        // Add KryoRegister to SparkConf serialization if not already there
        Set<String> registratorsSet = Arrays.stream(configuration.get(SPARK_REGISTRATORS, "").split(","))
                                            .filter(string -> string != null && !string.isEmpty())
                                            .collect(Collectors.toSet());
        registratorsSet.add(KryoRegister.class.getName());
        String registratorsString = String.join(",", registratorsSet);
        LOGGER.info("Setting kryo registrators: " + registratorsString);
        configuration.set(SPARK_REGISTRATORS, registratorsString);

        configuration.registerKryoClasses(new Class<?>[]{KryoRegister.class});
    }
}
