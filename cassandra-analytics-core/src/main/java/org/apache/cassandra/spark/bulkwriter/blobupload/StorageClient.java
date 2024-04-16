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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.io.IOException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
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
    private final StorageTransportConfiguration storageTransportConfiguration;
    private final DataChunker dataChunker;
    private final Tagging tagging;
    private final S3AsyncClient client;
    private final Map<StorageCredentials, AwsCredentialsProvider> credentialsCache;

    public StorageClient(StorageTransportConfiguration storageTransportConfiguration,
                         StorageClientConfig storageClientConfig)
    {
        this.storageTransportConfiguration = storageTransportConfiguration;
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


        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                                                          .region(Region.of(this.storageTransportConfiguration.getWriteRegion()))
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
        this.client = clientBuilder.build();
        this.dataChunker = new DataChunker(storageClientConfig.maxChunkSizeInBytes);
        List<Tag> tags = this.storageTransportConfiguration.getTags()
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

    /**
     * We use {@link CreateMultipartUploadRequest} to break down each SSTable bundle into chunks, according to
     * chunk size set, and upload to S3.
     *
     * @param credentials credentials used for uploading to S3
     * @param bundle      bundle of sstables
     * @return BundleStorageObject representing the uploaded bundle
     * @throws IOException          when an IO exception occurs during the multipart upload
     * @throws ExecutionException   when it fails to retrieve the state of a task
     * @throws InterruptedException when the thread is interrupted
     */
    public BundleStorageObject multiPartUpload(StorageCredentials credentials,
                                               Bundle bundle)
    throws IOException, ExecutionException, InterruptedException
    {
        if (credentials == null)
        {
            throw new IllegalArgumentException("No credentials provided for uploading bundles");
        }

        String key = calculateStorageKeyForBundle(storageTransportConfiguration.getPrefix(),
                                                  bundle.bundleFile);
        // todo: add retry policy to multi part requests
        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder()
                                                                                          .overrideConfiguration(credentialsOverride(credentials))
                                                                                          .bucket(storageTransportConfiguration.getWriteBucket())
                                                                                          .key(key)
                                                                                          .tagging(tagging)
                                                                                          .build();

        CreateMultipartUploadResponse multipartUploadResponse = client.createMultipartUpload(multipartUploadRequest).get();
        String uploadId = multipartUploadResponse.uploadId();

        List<CompletedPart> completedParts = uploadPartsOfBundle(key, uploadId, credentials, bundle);

        // tell s3 to merge all completed parts by making the CompleteMultipartUploadRequest
        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
                                                                           .parts(completedParts)
                                                                           .build();

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                                                                                                      .overrideConfiguration(credentialsOverride(credentials))
                                                                                                      .bucket(storageTransportConfiguration.getWriteBucket())
                                                                                                      .key(key)
                                                                                                      .uploadId(uploadId)
                                                                                                      .multipartUpload(completedUpload)
                                                                                                      .build();
        CompleteMultipartUploadResponse completeMultipartUploadResponse = client.completeMultipartUpload(completeMultipartUploadRequest).get();
        return BundleStorageObject.builder()
                                  .bundle(bundle)
                                  .storageObjectKey(key)
                                  .storageObjectChecksum(completeMultipartUploadResponse.eTag())
                                  .build();
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
                                                    StorageCredentials credentials,
                                                    Bundle bundle)
    throws IOException, ExecutionException, InterruptedException
    {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
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
                AsyncRequestBody body = AsyncRequestBody.fromBytes(ByteBufferUtils.getArray(buffer));
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                                                                       .overrideConfiguration(credentialsOverride(credentials))
                                                                       .bucket(storageTransportConfiguration.getWriteBucket())
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

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();          // exit/throw early from here
        return futures.stream().map(CompletableFuture::join).collect(Collectors.toList()); // or collect in the correct sequence
    }

    private String calculateStorageKeyForBundle(String prefix, Path bundleLocation)
    {
        return prefix + SEPARATOR + bundleLocation.getFileName();
    }

    private AwsCredentialsProvider toCredentialsProvider(StorageCredentials storageCredentials)
    {
        AwsCredentials credentials = AwsSessionCredentials.create(storageCredentials.getAccessKeyId(),
                                                                  storageCredentials.getSecretKey(),
                                                                  storageCredentials.getSessionToken());
        return StaticCredentialsProvider.create(credentials);
    }

    private Consumer<AwsRequestOverrideConfiguration.Builder> credentialsOverride(StorageCredentials credentials)
    {
        return b -> b.credentialsProvider(credentialsCache.computeIfAbsent(credentials, this::toCredentialsProvider));
    }
}
