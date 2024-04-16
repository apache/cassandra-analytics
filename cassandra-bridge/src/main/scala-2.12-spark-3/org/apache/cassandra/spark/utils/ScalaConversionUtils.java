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

import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

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
        return JavaConverters.<A>asJavaIterable(iterable);
    }

    public static <A> scala.collection.Iterator<A> asScalaIterator(java.util.Iterator<A> iterator)
    {
        return JavaConverters.<A>asScalaIterator(iterator);
    }

    public static <A, B> java.util.Map<A, B> mapAsJavaMap(scala.collection.Map<A, B> map)
    {
        return JavaConverters.<A, B>mapAsJavaMap(map);
    }

    public static <A> List<A> mutableSeqAsJavaList(Seq<A> seq)
    {
        return JavaConverters.<A>mutableSeqAsJavaList(seq);
    }
}
