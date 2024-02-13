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

package org.apache.cassandra.sidecar.testing;

import java.util.function.Predicate;

import org.apache.cassandra.distributed.impl.AbstractCluster;

/**
 * Predicate to instruct the JVM DTest framework on the classes should be loaded by the shared classloader.
 */
public class JvmDTestSharedClassesPredicate implements Predicate<String>
{
    public static final JvmDTestSharedClassesPredicate INSTANCE = new JvmDTestSharedClassesPredicate();

    private static final Predicate<String> EXTRA = className -> {
        // Those classes can be reached by Spark SizeEstimator, when it estimates the broadcast variable.
        // In the test scenario containing cassandra instance shutdown, there is a chance that it pick the class
        // that is loaded by the closed instance classloader, causing the following exception.
        // java.lang.IllegalStateException: Can't load <CLASS>. Instance class loader is already closed.
        return className.equals("org.apache.cassandra.utils.concurrent.Ref$OnLeak")
               || className.startsWith("org.apache.cassandra.metrics.RestorableMeter");
    };
    private static final Predicate<String> DELEGATE = AbstractCluster.SHARED_PREDICATE.or(EXTRA);

    @Override
    public boolean test(String s)
    {
        return DELEGATE.test(s);
    }
}
