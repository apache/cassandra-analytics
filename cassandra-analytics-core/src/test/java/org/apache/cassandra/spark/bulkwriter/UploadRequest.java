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

package org.apache.cassandra.spark.bulkwriter;

import java.nio.file.Path;

import org.apache.cassandra.spark.common.MD5Hash;
import org.apache.cassandra.spark.common.model.CassandraInstance;

public class UploadRequest
{
    public final Path path;
    public final int ssTableIdx;
    public final CassandraInstance instance;
    public final String sesssionId;
    public final MD5Hash fileHash;
    public final boolean uploadSucceeded;

    public UploadRequest(Path path,
                         int ssTableIdx,
                         CassandraInstance instance,
                         String sesssionId,
                         MD5Hash fileHash,
                         boolean uploadSucceeded)
    {
        this.path = path;
        this.ssTableIdx = ssTableIdx;
        this.instance = instance;
        this.sesssionId = sesssionId;
        this.fileHash = fileHash;
        this.uploadSucceeded = uploadSucceeded;
    }
}
