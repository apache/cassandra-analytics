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
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.transports.storage.StorageAccessConfiguration;
import org.apache.cassandra.spark.transports.storage.StorageAuth;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StorageClientS3MockTest
{
    private static final int CHUNK_SIZE = 5 * 1024 * 1024;
    private static final String SIZE_MISMATCH_BUCKET = "size-mismatch-bucket";
    private static final String PART_FAILURE_BUCKET = "part-failure-bucket";
    private static final StorageAuth AUTH = new StorageCredentials("accessKey", "secretKey", "sessionToken");
    private static S3MockContainer s3Mock;

    @TempDir
    private Path tempFolder;

    @BeforeAll
    static void startS3Mock()
    {
        s3Mock = new S3MockContainer("2.17.0").withInitialBuckets(SIZE_MISMATCH_BUCKET + "," + PART_FAILURE_BUCKET);
        s3Mock.start();
    }

    @AfterAll
    static void stopS3Mock()
    {
        if (s3Mock != null)
        {
            s3Mock.stop();
        }
    }

    @Test
    void testCleanupOnBundleSizeMismatch() throws IOException
    {
        Bundle bundle = createBundle();
        long actualSize = bundle.bundleCompressedSize - 1;
        try (FileChannel channel = FileChannel.open(bundle.bundleFile, StandardOpenOption.WRITE))
        {
            channel.truncate(actualSize);
        }

        try (S3AsyncClient realClient = createS3Client();
             S3AsyncClient verifier = createS3Client())
        {
            S3AsyncClient mockClient = delegatingClient(realClient);
            AtomicReference<ListPartsRequest> upload = new AtomicReference<>();
            AtomicReference<ListPartsResponse> beforeAbort = capturePartsBeforeAbort(mockClient, realClient, verifier, upload);
            try (StorageClient storageClient = createStorageClient(SIZE_MISMATCH_BUCKET, mockClient))
            {
                Throwable failure = catchThrowable(() -> storageClient.multiPartUpload(AUTH, bundle));
                assertThat(failure).isInstanceOf(IOException.class)
                                   .hasMessage("Bundle %s size mismatch: expected %d bytes, uploaded %d bytes",
                                               bundle.bundleFile, bundle.bundleCompressedSize, actualSize);
                assertThat(failure.getSuppressed()).isEmpty();
                assertThat(beforeAbort.get()).isNotNull();
                assertThat(beforeAbort.get().parts()).extracting(Part::partNumber).containsExactlyInAnyOrder(1, 2);
                assertThat(beforeAbort.get().parts().stream().mapToLong(Part::size).sum()).isEqualTo(actualSize);
                assertCleanup(mockClient, verifier, upload.get());
            }
        }
    }

    @Test
    void testCleanupOnPartialUploadFailure() throws IOException
    {
        Bundle bundle = createBundle();
        IllegalStateException partFailure = new IllegalStateException("Simulated part upload failure");
        try (S3AsyncClient realClient = createS3Client();
             S3AsyncClient verifier = createS3Client())
        {
            S3AsyncClient mockClient = delegatingClient(realClient);
            doAnswer(invocation -> {
                UploadPartRequest request = invocation.getArgument(0);
                AsyncRequestBody body = invocation.getArgument(1);
                if (request.partNumber() == 2)
                {
                    return CompletableFuture.failedFuture(partFailure);
                }
                return realClient.uploadPart(request, body);
            }).when(mockClient).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));

            AtomicReference<ListPartsRequest> upload = new AtomicReference<>();
            AtomicReference<ListPartsResponse> beforeAbort = capturePartsBeforeAbort(mockClient, realClient, verifier, upload);
            try (StorageClient storageClient = createStorageClient(PART_FAILURE_BUCKET, mockClient))
            {
                Throwable failure = catchThrowable(() -> storageClient.multiPartUpload(AUTH, bundle));
                assertThat(failure).isInstanceOf(ExecutionException.class).hasCause(partFailure);
                assertThat(failure.getSuppressed()).isEmpty();
                assertThat(beforeAbort.get()).isNotNull();
                assertThat(beforeAbort.get().parts()).extracting(Part::partNumber).containsExactly(1);
                assertThat(beforeAbort.get().parts().get(0).size()).isEqualTo(CHUNK_SIZE);
                assertCleanup(mockClient, verifier, upload.get());
            }
        }
    }

    private static S3AsyncClient delegatingClient(S3AsyncClient realClient)
    {
        S3AsyncClient client = mock(S3AsyncClient.class, delegatesTo(realClient));
        doNothing().when(client).close();
        return client;
    }

    private static AtomicReference<ListPartsResponse> capturePartsBeforeAbort(S3AsyncClient client,
                                                                              S3AsyncClient realClient,
                                                                              S3AsyncClient verifier,
                                                                              AtomicReference<ListPartsRequest> upload)
    {
        AtomicReference<ListPartsResponse> beforeAbort = new AtomicReference<>();
        doAnswer(invocation -> {
            AbortMultipartUploadRequest request = invocation.getArgument(0);
            ListPartsRequest list = ListPartsRequest.builder()
                                                    .bucket(request.bucket())
                                                    .key(request.key())
                                                    .uploadId(request.uploadId())
                                                    .build();
            if (upload.compareAndSet(null, list))
            {
                // inspect the S3 server before the first abort
                beforeAbort.set(verifier.listParts(list).join());
            }
            return realClient.abortMultipartUpload(request);
        }).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        return beforeAbort;
    }

    private static void assertCleanup(S3AsyncClient client, S3AsyncClient verifier, ListPartsRequest upload)
    {
        verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(client, atLeastOnce()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        verify(client, atLeastOnce()).listParts(any(ListPartsRequest.class));
        assertThat(upload).isNotNull();
        try
        {
            ListPartsResponse response = verifier.listParts(upload).join();
            assertThat(response.parts()).isEmpty();
            assertThat(response.isTruncated()).isNotEqualTo(true);
        }
        catch (CompletionException e)
        {
            assertThat(e).hasCauseInstanceOf(NoSuchUploadException.class);
        }
        assertThat(verifier.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(upload.bucket()).build())
                           .join().uploads()).isEmpty();
        assertThat(verifier.listObjectsV2(ListObjectsV2Request.builder().bucket(upload.bucket()).build())
                           .join().contents()).isEmpty();
    }

    private static S3AsyncClient createS3Client()
    {
        return S3AsyncClient.builder()
                            .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                            .region(Region.US_EAST_1)
                            .credentialsProvider(AUTH.toAwsCredentialsProvider())
                            .forcePathStyle(true)
                            .overrideConfiguration(b -> b.apiCallTimeout(Duration.ofSeconds(30)))
                            .build();
    }

    private static StorageClient createStorageClient(String bucket, S3AsyncClient client)
    {
        StorageAccessConfiguration access = new StorageAccessConfiguration("us-east-1", bucket, AUTH);
        StorageTransportConfiguration config = new StorageTransportConfiguration("prefix", Collections.emptyMap(),
                                                                                 access, Collections.emptyMap());
        return new StorageClient(config, new DataChunker(CHUNK_SIZE), client);
    }

    private Bundle createBundle() throws IOException
    {
        byte[] data = new byte[CHUNK_SIZE + 1024];
        new Random(31).nextBytes(data);
        Path component = Files.write(tempFolder.resolve("na-1-big-Data.db"), data);
        SSTableSummary summary = new SSTableSummary(BigInteger.ONE, BigInteger.TEN, "na-1-big");
        SSTableFilesAndRange sstable = new SSTableFilesAndRange(summary, Collections.singletonList(component), data.length);
        Digest digest = mock(Digest.class);
        when(digest.value()).thenReturn("checksum");
        Bundle bundle = Bundle.builder()
                              .bundleSequence(0)
                              .sourceSSTables(Collections.singletonList(sstable))
                              .bundleNameGenerator(new BundleNameGenerator("jobId", "sessionId"))
                              .bundleStagingDirectory(Files.createDirectories(tempFolder.resolve("staging")))
                              .fileDigests(Collections.singletonMap(component, digest))
                              .build();
        assertThat(bundle.bundleCompressedSize).isGreaterThan(CHUNK_SIZE + 1L).isLessThanOrEqualTo(2L * CHUNK_SIZE);
        return bundle;
    }
}
