package com.flipkart.avroserialization.uniontest;

/**
 * Created by dhritiman.das on 6/1/16.
 */

import com.flipkart.d42Tests.*;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Longs;
import org.apache.avro.Schema;
import org.apache.avro.file.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;



/**
 * @author tushar.naik
 * @version 1.0
 * @date 19/02/16 - 1:48 AM
 */
public class D42Client implements CentralClient {

    private static final Logger LOG = LoggerFactory.getLogger(D42Client.class.getSimpleName());

    /* amazon s3 client connection */
    protected final AmazonS3Client conn;

    public D42Client(String endpoint, String accessKey, String secretKey) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        conn = new AmazonS3Client(credentials, clientConfig);
        conn.setEndpoint(endpoint);
        conn.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    }

    /**
     * get all buckets
     *
     * @return list of buckets
     */
    @Override
    public List<Bucket> getBuckets() {
        return conn.listBuckets();
    }

    /**
     * create a new bucket with name
     *
     * @param bucketName name of bucket
     */
    @Override
    public void createBucket(String bucketName) {
        LOG.info("creating bucket: {}", bucketName);
        conn.createBucket(bucketName);
    }

    /**
     * upload file under a bucket and a key
     *
     * @param bucketName bucket
     * @param key        key
     * @param filePath   file to be uploaded
     */
    @Override
    public void put(String bucketName, String key, String filePath) {
        LOG.info("uploading file: {},  key:{}, bucket:{}", filePath, key, bucketName);
        // Create a list of UploadPartResponse objects. You get one of these
        // for each part upload.
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Step 1: Initialize.
        InitiateMultipartUploadRequest initRequest = new
                InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResponse =
                conn.initiateMultipartUpload(initRequest);

        File file = new File(filePath);
        long contentLength = file.length();
        long partSize = 128 * 1024 * 1024; // Set part size to 128 MB.

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 128 MB. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName).withKey(key)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(
                        conn.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(
                    bucketName,
                    key,
                    initResponse.getUploadId(),
                    partETags);

            conn.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            conn.abortMultipartUpload(new AbortMultipartUploadRequest(
                    bucketName, key, initResponse.getUploadId()));
            LOG.error("Error while uploading file: {},  key:{}, bucket:{}", filePath, key, bucketName, e);
            throw new RuntimeException(e);
        }
    }


    /**
     * download a file from bucket, key with given filepath
     *
     * @param bucketName bucket
     * @param key        key
     * @param filePath   filepath to be downloaded
     */
    @Override
    public void get(String bucketName, String key, String filePath) {
        LOG.info("downloading file: {},  key:{}, bucket:{}", filePath, key, bucketName);
        conn.getObject(
                new GetObjectRequest(bucketName, key),
                new File(filePath)
        );
    }

    /**
     * download a file from bucket, key with given filepath
     *
     * @param bucketName  bucket
     * @param key         key
     * @param bufferSize  size of buffer for bufferedInputStream
     *
     * @return A bufferedInputStream wrapping around the S3ObjectStream
     */
    @Override
    public BufferedInputStream get(String bucketName, String key, int bufferSize) {
        LOG.info("downloading  key:{}, bucket:{}", key, bucketName);
        S3Object s3Object = conn.getObject(
                new GetObjectRequest(bucketName, key)
        );
        return new BufferedInputStream(s3Object.getObjectContent(), bufferSize);
    }

    /**
     * return list of keys in the bucket
     *
     * @param bucketName bucket
     * @return list of keys as strings
     */
    @Override
    public List<String> getKeys(String bucketName) {
        LOG.info("fetching keys for bucket : {}", bucketName);
        List<String> keys = new ArrayList<String>();
        ObjectListing objects = conn.listObjects(bucketName);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                keys.add(objectSummary.getKey());
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        LOG.info(".. fetched keys for bucket: {}, keys: {}", bucketName, keys);
        return keys;
    }

    /**
     * retrieve the latest keys for a given bucket
     *
     * @param bucketName bucket
     * @return s3 object summary of the latest object
     */
    @Override
    public S3ObjectSummary getLatestKey(String bucketName) {
        LOG.info("fetching latest keys for bucket : {}", bucketName);
        S3ObjectSummary latestKey = null;
        Date latestDate = new Date(1);  // oldest time --> the milliseconds since January 1, 1970, 00:00:00 GMT.
        ObjectListing objects = conn.listObjects(bucketName);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                final Date lastModifiedDate = objectSummary.getLastModified();
                if (lastModifiedDate.compareTo(latestDate) > 0) {
                    latestKey = objectSummary;
                    latestDate = lastModifiedDate;
                }
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
        LOG.info("fetched latest key for bucket: {}, key: {}, s3Obj: {}", bucketName, latestKey != null ? latestKey.getKey() : null, latestKey);
        return latestKey;
    }

    @Override
    public void lsBucket(String bucketName) {
        ObjectListing objects = conn.listObjects(bucketName);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + "\t" +
                        objectSummary.getSize() + "\t" +
                        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    /**
     * delete key of a bucket
     *
     * @param bucketName bucket
     * @param key        key
     */
    @Override
    public void deleteKey(String bucketName, String key) {
        LOG.info("deleting key:{}, bucket:{}", key, bucketName);
        conn.deleteObject(bucketName, key);
    }

    /**
     * delete bucket and all its keys entirely
     *
     * @param bucketName bucket
     */
    @Override
    public void deleteBucket(String bucketName) {
        LOG.info("deleting bucket:{}", bucketName);
        conn.deleteBucket(bucketName);
    }

    /**
     * check if key is present in the bucket
     *
     * @param bucketName bucket
     * @param key        key
     * @return true if key exists, false if it doesn't
     */
    @Override
    public boolean checkKey(String bucketName, String key) {
        final List<String> keys = getKeys(bucketName);
        return keys.contains(key);
    }

}
