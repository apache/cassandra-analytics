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

import java.net.URLClassLoader;
import java.util.concurrent.Callable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.vdurmont.semver4j.Semver;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.testing.TestVersion;
import org.apache.cassandra.testing.TestVersionSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks hook binding against the actual dtest jar without starting a cluster.
 */
class TopologyChangeBBUtilsTest
{
    @ParameterizedTest
    @ValueSource(strings = {"bootstrap", "leaving", "moving"})
    void installsHookInDtestJar(String operation) throws Exception
    {
        TestVersion version = TestVersionSupplier.testVersions().findFirst().orElseThrow();
        Versions.Version dtest = Versions.find().getLatest(new Semver(version.version(), Semver.SemverType.LOOSE));
        try (URLClassLoader loader = new URLClassLoader(dtest.classpath, getClass().getClassLoader()))
        {
            String target;
            boolean tcm = loader.getResource("org/apache/cassandra/tcm/sequences/BootstrapAndJoin.class") != null;
            switch (operation)
            {
                case "bootstrap":
                    TopologyChangeBBUtils.installBootstrap(loader, BootstrapInterceptor.class);
                    target = tcm ? "org.apache.cassandra.tcm.sequences.BootstrapAndJoin"
                                 : "org.apache.cassandra.service.StorageService";
                    break;
                case "leaving":
                    TopologyChangeBBUtils.installLeaving(loader, LeavingInterceptor.class);
                    target = tcm ? "org.apache.cassandra.tcm.sequences.UnbootstrapStreams"
                                 : "org.apache.cassandra.service.StorageService";
                    break;
                case "moving":
                    TopologyChangeBBUtils.installMoving(loader, MovingInterceptor.class);
                    target = tcm ? "org.apache.cassandra.tcm.sequences.Move"
                                 : "org.apache.cassandra.service.RangeRelocator";
                    break;
                default:
                    throw new IllegalArgumentException(operation);
            }
            Class<?> intercepted = Class.forName(target, false, loader);
            assertThat(intercepted.getClassLoader()).isSameAs(loader);
            assertThat(intercepted.getDeclaredMethods()).anyMatch(method -> method.getName().contains("$original$"));
        }
    }

    public static class BootstrapInterceptor
    {
        public static boolean bootstrap(@SuperCall Callable<Boolean> original) throws Exception
        {
            return original.call();
        }
    }

    public static class LeavingInterceptor
    {
        public static void unbootstrap(@SuperCall Callable<?> original) throws Exception
        {
            original.call();
        }
    }

    public static class MovingInterceptor
    {
        @RuntimeType
        public static Object stream(@SuperCall Callable<?> original) throws Exception
        {
            return original.call();
        }
    }
}
