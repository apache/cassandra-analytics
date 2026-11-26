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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.spark.transports.storage.StorageAuth;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

/**
 * Client used for upload SSTable bundle to S3 bucket
 */
public class StorageClient implements AutoCloseable
{
    public static final char SEPARATOR = '/';
    private static final int ABORT_MAX_ATTEMPTS = 3;
    private static final int ABORT_RETRY_DELAY_MS = 1000;

    private final StorageTransportConfiguration storageTransportConfiguration;
    private final DataChunker dataChunker;
    private final Tagging tagging;
    private final S3AsyncClient client;
    private final Map<StorageAuth, AwsCredentialsProvider> credentialsCache;

    public StorageClient(StorageTransportConfiguration storageTransportConfiguration,
                         StorageClientConfig storageClientConfig)
    {
        this(storageTransportConfiguration, new DataChunker(storageClientConfig.maxChunkSizeInBytes),
             createClient(storageTransportConfiguration, storageClientConfig));
    }

    @VisibleForTesting
    StorageClient(StorageTransportConfiguration storageTransportConfiguration,
                  DataChunker dataChunker,
                  S3AsyncClient client)
    {
        this.storageTransportConfiguration = storageTransportConfiguration;
        this.client = client;
        this.dataChunker = dataChunker;
        List<Tag> tags = this.storageTransportConfiguration.getObjectTags()
                                                           .entrySet()
                                                           .stream()
                                                           .map(entry -> Tag.builder()
                                                                            .key(entry.getKey())
                                                                            .value(entry.getValue())
                                                                            .build())
                                                           .collect(Collectors.toList());
        this.tagging = Tagging.builder().tagSet(tags).build();
        this.credentialsCache = new ConcurrentHashMap<>();
    }

    private static S3AsyncClient createClient(StorageTransportConfiguration storageTransportConfiguration,
                                              StorageClientConfig storageClientConfig)
    {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(storageClientConfig.concurrency, // core
                                                             storageClientConfig.concurrency, // max
                                                             // keep alive
                                                             storageClientConfig.threadKeepAliveSeconds, TimeUnit.SECONDS,
                                                             new LinkedBlockingQueue<>(), // unbounded work queue
                                                             new ThreadFactoryBuilder().threadNamePrefix(storageClientConfig.threadNamePrefix)
                                                                                       .daemonThreads(true)
                                                                                       .build());
        // Must set it to allow threads to time out, so that it can release resources when idle.
        executor.allowCoreThreadTimeOut(true);
        Map<SdkAdvancedAsyncClientOption<?>, ?> advancedOptions = Collections.singletonMap(
        SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor
        );


        String region = storageTransportConfiguration.writeAccessConfiguration().region();
        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                                                          .region(Region.of(region))
                                                          .asyncConfiguration(b -> b.advancedOptions(advancedOptions));
        if (storageClientConfig.endpointOverride != null)
        {
            clientBuilder.endpointOverride(storageClientConfig.endpointOverride)
                         .forcePathStyle(true);
        }
        if (storageClientConfig.httpsProxy != null)
        {
            ProxyConfiguration proxyConfig = ProxyConfiguration.builder()
                                                               .host(storageClientConfig.httpsProxy.getHost())
                                                               .port(storageClientConfig.httpsProxy.getPort())
                                                               .scheme(storageClientConfig.httpsProxy.getScheme())
                                                               .build();
            Duration connectionAcquisitionTimeout = Duration.ofSeconds(storageClientConfig.nioHttpClientConnectionAcquisitionTimeoutSeconds);
            clientBuilder.httpClient(NettyNioAsyncHttpClient.builder()
                                                            .proxyConfiguration(proxyConfig)
                                                            .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                                                            .maxConcurrency(storageClientConfig.nioHttpClientMaxConcurrency)
                                                            .build());
        }
        return clientBuilder.build();
    }

    /**
     * We use {@link CreateMultipartUploadRequest} to break down each SSTable bundle into chunks, according to
     * chunk size set, and upload to S3.
     *
     * @param auth   the auth mechanism for this upload (e.g. {@link org.apache.cassandra.spark.transports.storage.StorageCredentials}
     *               for static STS keys, {@link org.apache.cassandra.spark.transports.storage.IamStorageAuth} for instance profile)
     * @param bundle bundle of sstables
     * @return BundleStorageObject representing the uploaded bundle
     * @throws IOException          when an IO exception occurs during the multipart upload
     * @throws ExecutionException   when it fails to retrieve the state of a task
     * @throws InterruptedException when the thread is interrupted
     */
    public BundleStorageObject multiPartUpload(StorageAuth auth,
                                               Bundle bundle)
    throws IOException, ExecutionException, InterruptedException
    {

        String key = calculateStorageKeyForBundle(storageTransportConfiguration.getPrefix(),
                                                  bundle.bundleFile);
        String bucket = storageTransportConfiguration.writeAccessConfiguration().bucket();
        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder()
                                                                                          .overrideConfiguration(credentialsOverride(auth))
                                                                                          .bucket(bucket)
                                                                                          .key(key)
                                                                                          .tagging(tagging)
                                                                                          .build();

        CreateMultipartUploadResponse multipartUploadResponse = client.createMultipartUpload(multipartUploadRequest).get();
        String uploadId = multipartUploadResponse.uploadId();

        CompleteMultipartUploadResponse completeMultipartUploadResponse;
        try
        {
            List<CompletedPart> completedParts = uploadPartsOfBundle(key, uploadId, auth, bundle);
            // tell s3 to merge all completed parts by making the CompleteMultipartUploadRequest
            CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
                                                                               .parts(completedParts)
                                                                               .build();
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                                                                                                          .overrideConfiguration(credentialsOverride(auth))
                                                                                                          .bucket(bucket)
                                                                                                          .key(key)
                                                                                                          .uploadId(uploadId)
                                                                                                          .multipartUpload(completedUpload)
                                                                                                          .build();
            completeMultipartUploadResponse = client.completeMultipartUpload(completeMultipartUploadRequest).get();
        }
        catch (IOException | ExecutionException | InterruptedException | RuntimeException failure)
        {
            try
            {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                                                                                      .overrideConfiguration(credentialsOverride(auth))
                                                                                      .bucket(bucket)
                                                                                      .key(key)
                                                                                      .uploadId(uploadId)
                                                                                      .build();
                abortMultipartUpload(abortRequest);
            }
            catch (RuntimeException abortFailure)
            {
                if (failure != abortFailure)
                {
                    failure.addSuppressed(abortFailure);
                }
            }
            finally
            {
                if (failure instanceof InterruptedException)
                {
                    Thread.currentThread().interrupt();
                }
            }
            throw failure;
        }

        return BundleStorageObject.builder()
                                  .bundle(bundle)
                                  .storageObjectKey(key)
                                  .storageObjectChecksum(completeMultipartUploadResponse.eTag())
                                  .build();
    }

    private void abortMultipartUpload(AbortMultipartUploadRequest abortRequest)
    {
        ListPartsRequest listRequest = ListPartsRequest.builder()
                                                       .bucket(abortRequest.bucket())
                                                       .key(abortRequest.key())
                                                       .uploadId(abortRequest.uploadId())
                                                       .overrideConfiguration(abortRequest.overrideConfiguration().orElse(null))
                                                       .maxParts(1)
                                                       .build();

        for (int attempt = 0; attempt < ABORT_MAX_ATTEMPTS; attempt++)
        {
            try
            {
                client.abortMultipartUpload(abortRequest).join();
                // Quote from AWS S3 AbortMultipartUpload documentation:
                // To verify that all parts have been removed and prevent getting charged for the part storage,
                // you should call the ListParts API operation and ensure that the parts list is empty.
                ListPartsResponse response = client.listParts(listRequest).join();
                if (response.parts().isEmpty() && !Boolean.TRUE.equals(response.isTruncated()))
                {
                    return;
                }
                if (attempt + 1 < ABORT_MAX_ATTEMPTS)
                {
                    Uninterruptibles.sleepUninterruptibly(ABORT_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                }
            }
            catch (CompletionException e)
            {
                if (e.getCause() instanceof NoSuchUploadException)
                {
                    return;
                }
                throw e;
            }
        }

        throw new IllegalStateException(String.format("Failed to remove all parts of multipart upload %s after %d attempts",
                                                      abortRequest.uploadId(), ABORT_MAX_ATTEMPTS));
    }

    @Override
    public void close()
    {
        if (client != null)
        {
            client.close();
        }
    }

    private List<CompletedPart> uploadPartsOfBundle(String key, String uploadId,
                                                    StorageAuth auth,
                                                    Bundle bundle)
    throws IOException, ExecutionException, InterruptedException
    {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
        try
        {
            long uploadedBytes = 0;
            String bucket = storageTransportConfiguration.writeAccessConfiguration().bucket();
            // upload the zip file using multipart upload
            // todo: use the simple upload when the zip file is small
            try (ReadableByteChannel channel = Channels.newChannel(Files.newInputStream(bundle.bundleFile)))
            {
                Iterator<ByteBuffer> chunks = dataChunker.chunks(channel);
                // part number must start from 1 and not exceed 10,000.
                // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpuchecksums
                int partNumber = 1;
                while (chunks.hasNext())
                {
                    int loopPartNumber = partNumber;
                    ByteBuffer buffer = chunks.next();
                    uploadedBytes += buffer.remaining();
                    // get a copy of the remaining ByteBuffer; for the last chunk, the cloned ByteBuffer is resized accordingly.
                    AsyncRequestBody body = AsyncRequestBody.fromRemainingByteBuffer(buffer);
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                                                                           .overrideConfiguration(credentialsOverride(auth))
                                                                           .bucket(bucket)
                                                                           .key(key)
                                                                           .uploadId(uploadId)
                                                                           .partNumber(loopPartNumber)
                                                                           .build();
                    Function<UploadPartResponse, CompletedPart> buildPart = r -> CompletedPart.builder()
                                                                                              .partNumber(loopPartNumber)
                                                                                              .eTag(r.eTag())
                                                                                              .build();
                    CompletableFuture<CompletedPart> completedPart = client.uploadPart(uploadPartRequest, body)
                                                                           .thenApply(buildPart);
                    futures.add(completedPart);
                    partNumber++;
                }
            }
            catch (UncheckedIOException e)
            {
                throw new IOException("Failed to read bundle: " + bundle.bundleFile, e);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(); // exit/throw early from here
            if (uploadedBytes != bundle.bundleCompressedSize)
            {
                throw new IOException(String.format("Bundle %s size mismatch: expected %d bytes, uploaded %d bytes",
                                                    bundle.bundleFile, bundle.bundleCompressedSize, uploadedBytes));
            }
            return futures.stream().map(CompletableFuture::join).collect(Collectors.toList()); // collect in the correct sequence
        }
        finally
        {
            // if an error was thrown in try-catch block, still wait for all submitted
            // requests to complete before eventual aborting the whole upload
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                             .handle((ignored, failure) -> null)
                             .join();
        }
    }

    private String calculateStorageKeyForBundle(String prefix, Path bundleLocation)
    {
        return prefix + SEPARATOR + bundleLocation.getFileName();
    }

    private Consumer<AwsRequestOverrideConfiguration.Builder> credentialsOverride(StorageAuth auth)
    {
        return b -> b.credentialsProvider(credentialsCache.computeIfAbsent(auth, StorageAuth::toAwsCredentialsProvider));
    }
}
