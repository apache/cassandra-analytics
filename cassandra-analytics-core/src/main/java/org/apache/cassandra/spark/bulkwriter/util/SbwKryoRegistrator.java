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

package org.apache.cassandra.spark.bulkwriter.util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.spark.bulkwriter.CassandraBulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.TokenPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.jetbrains.annotations.NotNull;

public class SbwKryoRegistrator implements KryoRegistrator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SbwKryoRegistrator.class);
    protected static final String KRYO_KEY = "spark.kryo.registrator";

    // CHECKSTYLE IGNORE: Despite being static and final, this is a mutable field not to be confused with a constant
    private static final Set<Class<? extends Serializable>> javaSerializableClasses =
    Sets.newHashSet(CassandraBulkWriterContext.class,
                    TokenPartitioner.class,
                    RingInstance.class);

    @Override
    public void registerClasses(@NotNull Kryo kryo)
    {
        LOGGER.debug("Registering Spark Bulk Writer classes with Kryo which require use of Java Serializer");
        // NOTE: The order of calls to `register` matters, so we sort by class name just to make sure we always
        //       register classes in the same order - HashSet doesn't guarantee its iteration order
        javaSerializableClasses.stream()
                               .sorted(Comparator.comparing(Class::getCanonicalName))
                               .forEach(javaSerializableClass -> kryo.register(javaSerializableClass, new SbwJavaSerializer()));
    }

    public static void addJavaSerializableClass(@NotNull Class<? extends Serializable> javaSerializableClass)
    {
        javaSerializableClasses.add(javaSerializableClass);
    }

    public static void setupKryoRegistrator(@NotNull SparkConf configuration)
    {
        String registrators = configuration.get(KRYO_KEY, "");
        String registrator = SbwKryoRegistrator.class.getName();
        if (!registrators.contains(registrator))
        {
            configuration.set(KRYO_KEY, registrators + "," + registrator);
        }
    }
}
