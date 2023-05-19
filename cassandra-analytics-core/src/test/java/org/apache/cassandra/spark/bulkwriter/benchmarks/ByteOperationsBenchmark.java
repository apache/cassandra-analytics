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

package org.apache.cassandra.spark.bulkwriter.benchmarks;

import org.apache.cassandra.spark.bulkwriter.util.FastByteOperations;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@SuppressWarnings("unused")
public class ByteOperationsBenchmark
{
    @Param({"1", "10", "100", "1000"})
    int numBytes;
    ByteBuffer bytes1;
    ByteBuffer bytes2;
    FastByteOperations.PureJavaOperations pureJavaOperations = new FastByteOperations.PureJavaOperations();
    FastByteOperations.UnsafeOperations unsafeOperations = new FastByteOperations.UnsafeOperations();

    @Setup(Level.Trial)
    public void setup()
    {
        byte[] bytes = RandomUtils.randomBytes(numBytes);
        bytes1 = ByteBuffer.wrap(bytes.clone());
        bytes2 = ByteBuffer.wrap(bytes.clone());
    }

    @Benchmark
    public int unsafeImplementation()
    {
        bytes1.position(0);
        bytes2.position(0);
        int result = unsafeOperations.compare(bytes1, bytes2);
        assert 0 == result : "Failed to compare";
        return result;
    }

    @Benchmark
    public int javaImplementation()
    {
        bytes1.position(0);
        bytes2.position(0);
        int result = pureJavaOperations.compare(bytes1, bytes2);
        assert result == 0 : "Failed to compare";
        return result;
    }
}
