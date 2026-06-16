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

package org.apache.cassandra.spark.common;

import java.nio.file.Path;

import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.spark.data.FileType;

public final class SSTables
{
    /**
     * Suffix identifying the primary SSTable data component, e.g. "-Data.db".
     * The leading '-' is significant: it excludes SAI per-index components such as
     * "...+TermsData.db" that also end with "Data.db".
     */
    private static final String DATA_COMPONENT_SUFFIX = "-" + FileType.DATA.getFileSuffix();

    /**
     * Glob matching primary SSTable data components, e.g. "*-Data.db".
     * Suitable for {@link java.nio.file.Files#newDirectoryStream(Path, String)}.
     */
    public static final String DATA_COMPONENT_GLOB = "*" + DATA_COMPONENT_SUFFIX;

    private SSTables()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Determine whether the given file name is a primary SSTable data component ("&lt;descriptor&gt;-Data.db").
     * The leading '-' check excludes SAI per-index components such as "...+TermsData.db" which also end with "Data.db".
     *
     * @param fileName file name (not a full path)
     * @return true if the name is a primary data component
     */
    public static boolean isDataComponent(String fileName)
    {
        return fileName.endsWith(DATA_COMPONENT_SUFFIX);
    }

    /**
     * Determine whether the given path is a primary SSTable data component ("&lt;descriptor&gt;-Data.db").
     *
     * @param path file path
     * @return true if the path's file name is a primary data component
     * @see #isDataComponent(String)
     */
    public static boolean isDataComponent(Path path)
    {
        return isDataComponent(path.getFileName().toString());
    }

    /**
     * Get the sstable base name from data file path.
     * For example, the base name of data file '/path/to/table/nb-1-big-Data.db' is 'nb-1-big'
     *
     * @deprecated use {@code #getSSTableDescriptor(Path).baseFilename} instead
     *
     * @param dataFile data file path
     * @return sstable base name
     */
    @Deprecated
    public static String getSSTableBaseName(Path dataFile)
    {
        String fileName = dataFile.getFileName().toString();
        return fileName.substring(0, fileName.lastIndexOf("-"));
    }

    /**
     * Get the {@link SSTableDescriptor} from the data file path.
     * @param dataFile data file path
     * @return sstable descriptor
     */
    public static SSTableDescriptor getSSTableDescriptor(Path dataFile)
    {
        String baseFilename = getSSTableBaseName(dataFile);
        return new SSTableDescriptor(baseFilename);
    }
}
