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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import static org.assertj.core.api.Assertions.assertThat;

class SbwKryoRegistratorTest
{
    /**
     * Rows are shuffled between keying and writing them, and the writer normalizes inet values to
     * {@link InetAddress}. Kryo's default serializer skips the transient field that holds the address and
     * rebuilds the object with its no-argument constructor, which produces 0.0.0.0 and ::.
     */
    @Test
    void inetAddressesSurviveTheShuffle() throws UnknownHostException
    {
        SparkConf configuration = new SparkConf();
        KryoRegister.setup(configuration);
        BulkSparkConf.setupSparkConf(configuration, true);
        SerializerInstance serializer = new KryoSerializer(configuration).newInstance();
        Object[] row = {InetAddress.getByName("10.0.0.1"), InetAddress.getByName("2001:db8::ff00:42:8329"), 1};

        ClassTag<Object[]> classTag = ClassTag$.MODULE$.apply(Object[].class);
        Object[] shuffledRow = serializer.deserialize(serializer.serialize(row, classTag), classTag);

        assertThat(shuffledRow).containsExactly(row);
    }
}
