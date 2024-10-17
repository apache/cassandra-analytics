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

package org.apache.cassandra.cdc;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

public class CdcKryoRegister
{
    private static final ThreadLocal<Kryo> KRYO = ThreadLocal.withInitial(() -> {
        final Kryo kryo = new Kryo();
        new CdcKryoRegister().registerClasses(kryo);
        return kryo;
    });

    public static Kryo kryo()
    {
        return KRYO.get();
    }

    public void registerClasses(Kryo kryo)
    {
        kryo.register(CassandraInstance.class, CassandraInstance.SERIALIZER);
        kryo.register(ReplicationFactor.class, ReplicationFactor.SERIALIZER);
        kryo.register(Marker.class, Marker.SERIALIZER);
        kryo.register(CdcState.class, CdcState.SERIALIZER);
        kryo.register(PartitionUpdateWrapper.Digest.class, PartitionUpdateWrapper.DIGEST_SERIALIZER);
    }
}
