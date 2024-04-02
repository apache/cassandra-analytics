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

package org.apache.cassandra.bridge;

import java.net.URL;
import java.net.URLClassLoader;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This custom implementation of a {@link ClassLoader} enables deferred execution-time loading of a particular version
 * of class hierarchy from one of many embedded the {@code cassandra-all} library JARs. It first attempts to load any
 * requested class from the extracted JAR, and resorts to using the parent class loader when the the class is not there.
 * This behavior is opposite to the one of standard {@link URLClassLoader}, which invokes its parent class loader first.
 */
public class PostDelegationClassLoader extends URLClassLoader
{
    public PostDelegationClassLoader(@NotNull URL[] urls, @Nullable ClassLoader parent)
    {
        super(urls, parent);
    }

    @Override
    @Nullable
    protected synchronized Class<?> loadClass(@Nullable String name, boolean resolve) throws ClassNotFoundException
    {
        // First, check if the class has already been loaded
        Class<?> type = findLoadedClass(name);
        if (type == null)
        {
            try
            {
                type = findClass(name);
            }
            catch (ClassNotFoundException | SecurityException | LinkageError exception)
            {
                // ClassNotFoundException thrown if class not found
            }
            if (type == null)
            {
                // If not found, then invoke findClass in order to find the class
                type = super.loadClass(name, false);
            }
        }
        if (resolve)
        {
            resolveClass(type);
        }
        return type;
    }
}
