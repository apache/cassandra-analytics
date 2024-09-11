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

package org.apache.cassandra.spark.utils;

import java.util.List;

import scala.collection.mutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Compatibility layer for scala conversions
 */
public final class ScalaConversionUtils
{
    private ScalaConversionUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static <A> java.lang.Iterable<A> asJavaIterable(scala.collection.Iterable<A> iterable)
    {
        return CollectionConverters.asJava(iterable);
    }

    public static <A> scala.collection.Iterator<A> asScalaIterator(java.util.Iterator<A> iterator)
    {
        return CollectionConverters.asScala(iterator);
    }

    public static <K, V> java.util.Map<K, V> mapAsJavaMap(scala.collection.Map<K, V> map)
    {
        return CollectionConverters.asJava(map);
    }

    public static <A> List<A> mutableSeqAsJavaList(Seq<A> seq)
    {
        return CollectionConverters.asJava(seq);
    }
}
