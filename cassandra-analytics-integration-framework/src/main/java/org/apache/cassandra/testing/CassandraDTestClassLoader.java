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

package org.apache.cassandra.testing;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A dedicated classloader for the in-jvm dtest jar. Manages classes needed for distributed dtests separately
 * from application code.
 */
public class CassandraDTestClassLoader extends ClassLoader
{
    public static final String CLASS_EXT = ".class";
    public static final int CLASS_EXT_LEN = CLASS_EXT.length();
    private final Set<String> locallyManagedClasses;
    private final URLClassLoader urlClassLoader;

    public CassandraDTestClassLoader(ClassLoader parent, URL[] dTestJarClasspath) throws IOException
    {
        super(Objects.requireNonNull(parent, "parent class loader cannot be null"));
        this.locallyManagedClasses = buildLocallyManagedClasses(dTestJarClasspath);
        this.urlClassLoader = new URLClassLoader(dTestJarClasspath);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        if (!locallyManagedClasses.contains(name))
        {
            // Not managed by this classloader, delegate to parent
            return super.loadClass(name);
        }
        // Load classes for dtest in its own classloader
        return urlClassLoader.loadClass(name);
    }

    protected Set<String> buildLocallyManagedClasses(URL[] classpath) throws IOException
    {
        Set<String> classesInJar = new HashSet<>();
        for (URL c : classpath)
        {
            try (InputStream inputStream = c.openStream();
                 ZipInputStream zipInputStream = new ZipInputStream(inputStream))
            {
                ZipEntry entry;
                while ((entry = zipInputStream.getNextEntry()) != null)
                {
                    String name = entry.getName();
                    if (!entry.isDirectory() && name.endsWith(CLASS_EXT))
                    {
                        String className = name.substring(0, name.length() - CLASS_EXT_LEN).replace("/", ".");
                        classesInJar.add(className);
                    }
                }
            }
        }
        return classesInJar;
    }
}
