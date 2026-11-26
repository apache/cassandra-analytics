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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.apache.cassandra.bridge.SSTableSummary;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.SSTableCollector.SSTableFilesAndRange;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.transports.storage.StorageAccessConfiguration;
import org.apache.cassandra.spark.transports.storage.StorageAuth;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StorageClientTest
{
    @TempDir
    private Path tempFolder;

    private S3AsyncClient client;
    private final StorageAuth auth = new StorageCredentials("accessKey", "secretKey", "sessionToken");

    @BeforeEach
    void setUp()
    {
        client = mock(S3AsyncClient.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder().uploadId("uploadId").build()));
        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(AbortMultipartUploadResponse.builder().build()));
        when(client.listParts(any(ListPartsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(ListPartsResponse.builder().isTruncated(false).build()));
        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.completedFuture(UploadPartResponse.builder().eTag("part-etag").build()));
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 1 })
    void testMultipartUploadFailsOnBundleSizeMismatch(int sizeDelta) throws Exception
    {
        Bundle bundle = createBundle();
        long expectedSize = bundle.bundleCompressedSize;
        byte[] original = Files.readAllBytes(bundle.bundleFile);
        byte[] modified = Arrays.copyOf(original, original.length + sizeDelta);
        Files.write(bundle.bundleFile, modified);

        try (StorageClient storageClient = createStorageClient())
        {
            assertThatThrownBy(() -> storageClient.multiPartUpload(auth, bundle))
            .isInstanceOf(IOException.class)
            .hasMessage("Bundle %s size mismatch: expected %d bytes, uploaded %d bytes",
                        bundle.bundleFile, expectedSize, (long) modified.length);

            verify(client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(client, atLeastOnce()).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verifyCleanup(1, 1);
        }
    }

    @Test
    void testMultipartUploadCompletesWhenBundleSizeMatches() throws Exception
    {
        Bundle bundle = createBundle();
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(CompleteMultipartUploadResponse.builder().eTag("bundle-etag").build()));

        try (StorageClient storageClient = createStorageClient())
        {
            storageClient.multiPartUpload(auth, bundle);
            verify(client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(client, atLeastOnce()).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
            verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            verify(client, never()).listParts(any(ListPartsRequest.class));
        }
    }

    @Test
    void testAbortOnPartialUploadFailure() throws Exception
    {
        Bundle bundle = createBundle();
        IllegalStateException failure = new IllegalStateException("Part failed");
        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.completedFuture(UploadPartResponse.builder().eTag("part-etag").build()))
        .thenReturn(CompletableFuture.failedFuture(failure));

        try (StorageClient storageClient = createStorageClient())
        {
            assertThatThrownBy(() -> storageClient.multiPartUpload(auth, bundle))
            .isInstanceOf(ExecutionException.class).hasCause(failure);
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verifyAbort();
        }
    }

    @Test
    void testAbortWaitsForOutstandingParts() throws Exception
    {
        Bundle bundle = createBundle();
        CompletableFuture<UploadPartResponse> pending = new CompletableFuture<>();
        CountDownLatch secondPart = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("Part submission failed");
        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(pending)
        .thenAnswer(invocation -> {
            secondPart.countDown();
            throw failure;
        });
        try (StorageClient storageClient = createStorageClient())
        {
            CompletableFuture<Void> upload = CompletableFuture.runAsync(() -> {
                assertThatThrownBy(() -> storageClient.multiPartUpload(auth, bundle)).isSameAs(failure);
            });
            try
            {
                assertThat(secondPart.await(5, TimeUnit.SECONDS)).isTrue();
                // assert that the upload future has not completed
                assertThatThrownBy(() -> upload.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                verify(client, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            }
            finally
            {
                pending.complete(UploadPartResponse.builder().eTag("part-etag").build());
                upload.get(5, TimeUnit.SECONDS);
            }
            verifyAbort();
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        }
    }

    @Test
    void testAbortOnCompletionFailure() throws Exception
    {
        Bundle bundle = createBundle();
        IllegalStateException failure = new IllegalStateException("Completion failed");
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(failure));

        try (StorageClient storageClient = createStorageClient())
        {
            assertThatThrownBy(() -> storageClient.multiPartUpload(auth, bundle))
            .isInstanceOf(ExecutionException.class).hasCause(failure);
            verifyAbort();
        }
    }

    @Test
    void testAbortFailurePropagatesOriginalFailure() throws Exception
    {
        Bundle bundle = createBundle();
        IllegalStateException failure = new IllegalStateException("Part failed");
        IllegalStateException abortFailure = new IllegalStateException("Abort failed");
        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.failedFuture(failure));
        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(abortFailure));

        try (StorageClient storageClient = createStorageClient())
        {
            assertThatThrownBy(() -> storageClient.multiPartUpload(auth, bundle))
            .isInstanceOf(ExecutionException.class).hasCause(failure)
            .satisfies(thrown -> {
                assertThat(thrown.getSuppressed()).hasSize(1);
                assertThat(thrown.getSuppressed()[0]).isInstanceOf(CompletionException.class).hasCause(abortFailure);
            });
            verifyAbort();
            verify(client, never()).listParts(any(ListPartsRequest.class));
        }
    }

    @Test
    void testAbortRemovesAllParts() throws Exception
    {
        Throwable maybeError = failUpload();

        assertThat(maybeError.getSuppressed()).isEmpty();
        verifyCleanup(1, 1);
    }

    @Test
    void testAbortRetriesWhenPartsRemain() throws Exception
    {
        when(client.listParts(any(ListPartsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(remainingParts()))
        .thenReturn(CompletableFuture.completedFuture(ListPartsResponse.builder().isTruncated(false).build()));

        Throwable maybeError = failUpload();

        assertThat(maybeError.getSuppressed()).isEmpty();
        verifyCleanup(2, 2);
    }

    @Test
    void testAbortStopsWhenUploadDoesNotExist() throws Exception
    {
        when(client.listParts(any(ListPartsRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(NoSuchUploadException.builder().statusCode(404).build()));

        Throwable maybeError = failUpload();

        assertThat(maybeError.getSuppressed()).isEmpty();
        verifyCleanup(1, 1);
    }

    @Test
    void testAbortWhenUploadAlreadyDoesNotExist() throws Exception
    {
        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(NoSuchUploadException.builder().statusCode(404).build()));

        Throwable maybeError = failUpload();

        assertThat(maybeError.getSuppressed()).isEmpty();
        verifyCleanup(1, 0);
    }

    @Test
    void testAbortExhaustsRetries() throws Exception
    {
        when(client.listParts(any(ListPartsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(remainingParts()));

        Throwable failure = failUpload();

        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0])
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Failed to remove all parts of multipart upload uploadId after 3 attempts");
        verifyCleanup(3, 3);
    }

    private static ListPartsResponse remainingParts()
    {
        return ListPartsResponse.builder()
                                .parts(Part.builder().partNumber(1).build())
                                .isTruncated(false)
                                .build();
    }

    private Throwable failUpload() throws Exception
    {
        Bundle bundle = createBundle();
        IllegalStateException partFailure = new IllegalStateException("Part failed");

        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.failedFuture(partFailure));

        try (StorageClient storageClient = createStorageClient())
        {
            Throwable failure = catchThrowable(() -> storageClient.multiPartUpload(auth, bundle));

            assertThat(failure).isInstanceOf(ExecutionException.class).hasCause(partFailure);
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

            return failure;
        }
    }

    private void verifyCleanup(int abortAttempts, int listAttempts)
    {
        ArgumentCaptor<CreateMultipartUploadRequest> create = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        ArgumentCaptor<AbortMultipartUploadRequest> abort = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        ArgumentCaptor<ListPartsRequest> list = ArgumentCaptor.forClass(ListPartsRequest.class);
        verify(client).createMultipartUpload(create.capture());
        verify(client, times(abortAttempts)).abortMultipartUpload(abort.capture());
        verify(client, times(listAttempts)).listParts(any(ListPartsRequest.class));

        InOrder order = inOrder(client);
        for (int attempt = 0; attempt < abortAttempts; attempt++)
        {
            order.verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            if (attempt < listAttempts)
            {
                order.verify(client).listParts(list.capture());
            }
        }
        for (AbortMultipartUploadRequest request : abort.getAllValues())
        {
            assertThat(request.bucket()).isEqualTo(create.getValue().bucket());
            assertThat(request.key()).isEqualTo(create.getValue().key());
            assertThat(request.uploadId()).isEqualTo("uploadId");
        }
        for (ListPartsRequest request : list.getAllValues())
        {
            assertThat(request.bucket()).isEqualTo(create.getValue().bucket());
            assertThat(request.key()).isEqualTo(create.getValue().key());
            assertThat(request.uploadId()).isEqualTo("uploadId");
            assertThat(request.maxParts()).isEqualTo(1);
        }
    }

    private void verifyAbort()
    {
        ArgumentCaptor<CreateMultipartUploadRequest> create = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        ArgumentCaptor<AbortMultipartUploadRequest> abort = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(client).createMultipartUpload(create.capture());
        verify(client).abortMultipartUpload(abort.capture());
        assertThat(abort.getValue().bucket()).isEqualTo(create.getValue().bucket());
        assertThat(abort.getValue().key()).isEqualTo(create.getValue().key());
        assertThat(abort.getValue().uploadId()).isEqualTo("uploadId");
    }

    private StorageClient createStorageClient()
    {
        StorageAccessConfiguration access = new StorageAccessConfiguration("us-east-1", "bucket", auth);
        StorageTransportConfiguration config = new StorageTransportConfiguration("prefix", Collections.emptyMap(),
                                                                                 access, Collections.emptyMap());
        // use chunks size less than complete bundle size (100)
        return new StorageClient(config, new DataChunker(64, false), client);
    }

    private Bundle createBundle() throws IOException
    {
        Path component = Files.write(tempFolder.resolve("na-1-big-Data.db"), new byte[100]);
        SSTableSummary summary = new SSTableSummary(BigInteger.ONE, BigInteger.TEN, "na-1-big");
        SSTableFilesAndRange sstable = new SSTableFilesAndRange(summary, Collections.singletonList(component), 100);
        Digest digest = mock(Digest.class);
        when(digest.value()).thenReturn("checksum");
        return Bundle.builder()
                     .bundleSequence(0)
                     .sourceSSTables(Collections.singletonList(sstable))
                     .bundleNameGenerator(new BundleNameGenerator("jobId", "sessionId"))
                     .bundleStagingDirectory(Files.createDirectories(tempFolder.resolve("staging")))
                     .fileDigests(Collections.singletonMap(component, digest))
                     .build();
    }
}
