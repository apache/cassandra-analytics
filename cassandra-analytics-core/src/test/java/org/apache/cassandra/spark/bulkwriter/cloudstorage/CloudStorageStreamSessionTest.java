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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.DataTransport;
import org.apache.cassandra.spark.bulkwriter.DataTransportInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.MockBulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.MockTableWriter;
import org.apache.cassandra.spark.bulkwriter.NonValidatingTestSortedSSTableWriter;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.SortedSSTableWriter;
import org.apache.cassandra.spark.bulkwriter.TokenRangeMappingUtils;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.cassandra.spark.utils.TemporaryDirectory;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class CloudStorageStreamSessionTest
{
    @TempDir
    private Path folder;

    @Test
    void testSendBundles() throws IOException, URISyntaxException
    {
        // setup
        UUID jobId = UUID.randomUUID();
        String sessionId = "1-" + UUID.randomUUID();
        BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId.toString(), sessionId);
        TransportContext.CloudStorageTransportContext transportContext = mock(TransportContext.CloudStorageTransportContext.class);
        TokenRangeMapping<RingInstance> topology = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        MockBulkWriterContext bulkWriterContext = new MockBulkWriterContext(topology);
        BulkWriterContext spiedWriterContext = spy(bulkWriterContext);
        ReplicaAwareFailureHandler<RingInstance> replicaAwareFailureHandler =
        new MultiClusterReplicaAwareFailureHandler<>(bulkWriterContext.cluster().getPartitioner());
        Range<BigInteger> range = Range.range(BigInteger.valueOf(100L), BoundType.OPEN, BigInteger.valueOf(199L), BoundType.CLOSED);
        JobInfo job = mock(JobInfo.class);
        when(job.getRestoreJobId()).thenReturn(jobId);
        when(job.qualifiedTableName()).thenReturn(new QualifiedTableName("ks", "table1"));
        MockTableWriter tableWriter = new MockTableWriter(folder);
        SortedSSTableWriter sstableWriter = new NonValidatingTestSortedSSTableWriter(tableWriter, folder, new XXHash32DigestAlgorithm(), 1);

        DataTransportInfo transportInfo = mock(DataTransportInfo.class);
        when(transportInfo.getTransport()).thenReturn(DataTransport.S3_COMPAT);
        when(transportInfo.getMaxSizePerBundleInBytes()).thenReturn(5 * 1024L);
        when(job.transportInfo()).thenReturn(transportInfo);
        when(spiedWriterContext.job()).thenReturn(job);
        when(job.effectiveSidecarPort()).thenReturn(65055);

        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.getTokenRangeMapping(anyBoolean())).thenReturn(topology);
        when(spiedWriterContext.cluster()).thenReturn(clusterInfo);
        StorageTransportConfiguration storageTransportConfiguration = mock(StorageTransportConfiguration.class, RETURNS_DEEP_STUBS);
        when(transportContext.transportConfiguration()).thenReturn(storageTransportConfiguration);

        StorageTransportExtension transportExtension = mock(StorageTransportExtension.class);
        when(transportContext.transportExtensionImplementation()).thenReturn(transportExtension);

        try (TemporaryDirectory tempDir = new TemporaryDirectory())
        {
            // setup continued
            Path sourceDir = Paths.get(getClass().getResource("/data/ks/table1-ea3b3e6b-0d78-4913-89f2-15fcf98711d0").toURI());
            Path outputDir = tempDir.path();
            FileUtils.copyDirectory(sourceDir.toFile(), outputDir.toFile());

            CassandraBridge bridge = generateBridge(outputDir);
            SSTableLister ssTableLister = new SSTableLister(new QualifiedTableName("ks", "table1"), bridge);
            SSTablesBundler ssTablesBundler = new SSTablesBundler(outputDir, ssTableLister, nameGenerator, 5 * 1024);
            ssTablesBundler.includeDirectory(outputDir);
            ssTablesBundler.finish();
            List<Bundle> bundles = ImmutableList.copyOf(ssTablesBundler);

            SidecarClient sidecarClient = mock(SidecarClient.class);
            StorageClient storageClient = mock(StorageClient.class);
            MockBlobTransferApi blobDataTransferApi = new MockBlobTransferApi(spiedWriterContext.job(), sidecarClient, storageClient);
            when(transportContext.dataTransferApi()).thenReturn(blobDataTransferApi);
            when(transportContext.transportConfiguration().readAccessConfiguration(null).bucket()).thenReturn("readBucket");

            CloudStorageStreamSession ss = new CloudStorageStreamSession(spiedWriterContext, sstableWriter,
                                                                         transportContext, sessionId,
                                                                         range, bridge, replicaAwareFailureHandler,
                                                                         Executors.newSingleThreadExecutor());

            // test begins
            for (Bundle bundle : bundles)
            {
                ss.sendBundle(bundle, true);
            }

            assertEquals(bundles.size(), ss.createdRestoreSlices().size(),
                         "It should create 1 slice per bundle");
            Bundle actualBundle1 = blobDataTransferApi.uploadedBundleManifest.get(BigInteger.valueOf(1L));
            BundleManifest.Entry actualBundle1Entry = actualBundle1.manifestEntry("na-1-big-");
            assertEquals(BigInteger.valueOf(1L), actualBundle1Entry.startToken());
            assertEquals(BigInteger.valueOf(3L), actualBundle1Entry.endToken());
            Map<String, String> bundle1ComponentsChecksum = actualBundle1Entry.componentsChecksum();
            assertEquals("f48b39a3", bundle1ComponentsChecksum.get("na-1-big-Data.db"));
            assertEquals("ee128018", bundle1ComponentsChecksum.get("na-1-big-Index.db"));
            assertEquals("e2c32c23", bundle1ComponentsChecksum.get("na-1-big-Summary.db"));
            assertEquals("f773fcc6", bundle1ComponentsChecksum.get("na-1-big-Statistics.db"));
            assertEquals("7c8ef1f5", bundle1ComponentsChecksum.get("na-1-big-TOC.txt"));
            assertEquals("72fc4f9c", bundle1ComponentsChecksum.get("na-1-big-Filter.db"));

            Bundle actualBundle2 = blobDataTransferApi.uploadedBundleManifest.get(BigInteger.valueOf(3L));
            BundleManifest.Entry actualBundle2Entry = actualBundle2.manifestEntry("na-2-big-");
            assertEquals(BigInteger.valueOf(3L), actualBundle2Entry.startToken());
            assertEquals(BigInteger.valueOf(6L), actualBundle2Entry.endToken());
            Map<String, String> bundle2ComponentsChecksum = actualBundle2Entry.componentsChecksum();
            assertEquals("f48b39a3", bundle2ComponentsChecksum.get("na-2-big-Data.db"));
            assertEquals("ee128018", bundle2ComponentsChecksum.get("na-2-big-Index.db"));
            assertEquals("e2c32c23", bundle2ComponentsChecksum.get("na-2-big-Summary.db"));
            assertEquals("f773fcc6", bundle2ComponentsChecksum.get("na-2-big-Statistics.db"));
            assertEquals("7c8ef1f5", bundle2ComponentsChecksum.get("na-2-big-TOC.txt"));
            assertEquals("72fc4f9c", bundle2ComponentsChecksum.get("na-2-big-Filter.db"));
        }
    }

    private CassandraBridge generateBridge(Path outputDir)
    {
        CassandraBridge bridge = mock(CassandraBridge.class);

        SSTableSummary summary1 = new SSTableSummary(BigInteger.valueOf(1L), BigInteger.valueOf(3L), "na-1-big-");
        SSTableSummary summary2 = new SSTableSummary(BigInteger.valueOf(3L), BigInteger.valueOf(6L), "na-2-big-");

        FileSystemSSTable ssTable1 = new FileSystemSSTable(outputDir.resolve("na-1-big-Data.db"), false, null);
        FileSystemSSTable ssTable2 = new FileSystemSSTable(outputDir.resolve("na-2-big-Data.db"), false, null);
        when(bridge.getSSTableSummary("ks", "table1", ssTable1)).thenReturn(summary1);
        when(bridge.getSSTableSummary("ks", "table1", ssTable2)).thenReturn(summary2);
        return bridge;
    }

    public static class MockBlobTransferApi extends CloudStorageDataTransferApiImpl
    {
        Map<BigInteger, Bundle> uploadedBundleManifest = new HashMap<>();

        MockBlobTransferApi(JobInfo jobInfo, SidecarClient sidecarClient, StorageClient storageClient)
        {
            super(jobInfo, sidecarClient, storageClient);
        }

        @Override
        public RestoreJobSummaryResponsePayload restoreJobSummary()
        {
            RestoreJobSummaryResponsePayload payload = mock(RestoreJobSummaryResponsePayload.class);
            o.a.c.sidecar.client.shaded.common.data.StorageCredentials credentials = mock(o.a.c.sidecar.client.shaded.common.data.StorageCredentials.class);
            when(credentials.accessKeyId()).thenReturn("id");
            when(credentials.secretAccessKey()).thenReturn("key");
            when(credentials.sessionToken()).thenReturn("token");
            when(payload.secrets()).thenReturn(new RestoreJobSecrets(credentials, credentials));
            return payload;
        }

        @Override
        public BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle)
        {
            uploadedBundleManifest.put(bundle.startToken, bundle);
            return BundleStorageObject.builder()
                                      .bundle(bundle)
                                      .storageObjectChecksum("dummy")
                                      .storageObjectKey("some_prefix-" + bundle.bundleFile.getFileName().toString())
                                      .build();
        }

        @Override
        public void createRestoreSliceFromExecutor(SidecarInstance sidecarInstance,
                                                   CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException
        {
            // the request is always successful
        }
    }
}
