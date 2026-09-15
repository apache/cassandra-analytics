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

package org.apache.cassandra.analytics;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.pool.TypePool;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Installs topology-change hooks in the Cassandra instance classloader, before or after Transactional Cluster Metadata (TCM).
 */
public final class TopologyChangeBBUtils
{
    private TopologyChangeBBUtils()
    {
    }

    public static void installBootstrap(ClassLoader loader, Class<?> interceptor)
    {
        install(loader, interceptor, "org.apache.cassandra.tcm.sequences.BootstrapAndJoin", named("bootstrap"),
                "org.apache.cassandra.service.StorageService", named("bootstrap").and(takesArguments(2)));
    }

    public static void installLeaving(ClassLoader loader, Class<?> interceptor)
    {
        install(loader, interceptor, "org.apache.cassandra.tcm.sequences.UnbootstrapStreams", named("execute"),
                "org.apache.cassandra.service.StorageService", named("unbootstrap"));
    }

    public static void installMoving(ClassLoader loader, Class<?> interceptor)
    {
        install(loader, interceptor, "org.apache.cassandra.tcm.sequences.Move", named("executeNext"),
                "org.apache.cassandra.service.RangeRelocator", named("stream"));
    }

    private static void install(ClassLoader loader, Class<?> interceptor,
                                String tcmClass, ElementMatcher.Junction<MethodDescription> tcmMethod,
                                String legacyClass, ElementMatcher.Junction<MethodDescription> legacyMethod)
    {
        TypePool pool = TypePool.Default.of(loader);
        TypePool.Resolution tcm = pool.describe(tcmClass);
        TypeDescription type = tcm.isResolved() ? tcm.resolve() : pool.describe(legacyClass).resolve();
        ElementMatcher.Junction<MethodDescription> method = tcm.isResolved() ? tcmMethod : legacyMethod;
        if (type.getDeclaredMethods().filter(method).isEmpty())
        {
            throw new IllegalStateException("Missing topology-change interception point in " + type.getName());
        }
        new ByteBuddy().rebase(type, ClassFileLocator.ForClassLoader.of(loader))
                       .method(method)
                       .intercept(MethodDelegation.to(interceptor))
                       .make(TypeResolutionStrategy.Lazy.INSTANCE, pool)
                       .load(loader, ClassLoadingStrategy.Default.INJECTION);
    }
}
