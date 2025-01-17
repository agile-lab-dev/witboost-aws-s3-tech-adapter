package com.witboost.provisioning.s3.client;

import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import io.vavr.control.Either;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

/**
 * BucketManager provides utility methods for managing Amazon S3 buckets and their contents.
 */
@NoArgsConstructor
@Service
public class BucketManager {

    private final Logger logger = LoggerFactory.getLogger(BucketManager.class);

    @Value("${s3.bucket.wait-timeout-seconds}")
    private int bucketWaitTimeoutSeconds;

    @Value("${s3.object.wait-timeout-seconds}")
    private int objectWaitTimeoutSeconds;

    /**
     * Creates an S3 bucket if it does not already exist.
     * If the bucket already exists in a different region, an error is returned.
     * This operation waits until the bucket is confirmed to exist.
     *
     * @param s3Client   the {@link S3Client} used to interact with Amazon S3.
     * @param bucketName the name of the bucket to create.
     * @param region     the desired region for the bucket.
     * @return an {@link Either} containing {@link FailedOperation} in case of error or {@code null} on success.
     */
    public Either<FailedOperation, Void> createBucket(
            @NotNull S3Client s3Client, @NotNull String bucketName, @NotNull String region) {
        try {
            logger.info("Starting creation of bucket '{}' in region '{}'.", bucketName, region);

            Either<FailedOperation, Boolean> bucketExists = doesBucketExist(s3Client, bucketName);
            if (bucketExists.isLeft()) return Either.left(bucketExists.getLeft());

            // Bucket already exists
            if (bucketExists.get()) {
                Either<FailedOperation, String> existingRegion = getBucketRegion(s3Client, bucketName);
                if (existingRegion.isLeft()) return Either.left(existingRegion.getLeft());

                if (region.equals(existingRegion.get())) {
                    logger.info("Bucket '{}' already exists in region '{}'.", bucketName, region);
                    return Either.right(null);
                }

                String error = String.format(
                        "[Bucket: %s] Error: The bucket already exists in a different region (%s). Cannot create a new one in %s",
                        bucketName, existingRegion.get(), region);
                logger.error(error);
                return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error))));
            }

            // Create the bucket
            CreateBucketRequest createRequest =
                    CreateBucketRequest.builder().bucket(bucketName).build();
            s3Client.createBucket(createRequest);

            // Wait until the bucket exists
            Either<FailedOperation, Void> waitForBucketExistence = waitForBucketExistence(s3Client, bucketName);
            if (waitForBucketExistence.isLeft()) return Either.left(waitForBucketExistence.getLeft());

            logger.info("Bucket '{}' is successfully created in region '{}'.", bucketName, region);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while creating the bucket. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Retrieves the AWS region where the specified bucket is located.
     * This operation returns the region or an error if the region cannot be retrieved.
     *
     * @param s3Client   the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     * @return an {@link Either} containing the region name or a {@link FailedOperation} in case of error.
     */
    protected Either<FailedOperation, String> getBucketRegion(@NotNull S3Client s3Client, @NotBlank String bucketName) {
        try {
            logger.debug("Retrieving region for bucket '{}'.", bucketName);

            GetBucketLocationResponse locationResponse = s3Client.getBucketLocation(
                    GetBucketLocationRequest.builder().bucket(bucketName).build());

            BucketLocationConstraint locationConstraint = locationResponse.locationConstraint();
            String region = (locationConstraint == null) ? "us-east-1" : locationConstraint.toString();

            logger.debug("Bucket '{}' is located in region '{}'.", bucketName, region);
            return Either.right(region);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while getting the region of the bucket. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Waits until the specified bucket exists.
     * The operation will block until the bucket is confirmed to exist or a timeout occurs.
     *
     * @param s3         the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket to wait for.
     * @return an {@link Either} containing {@link FailedOperation} in case of error or {@code null} on success.
     */
    protected Either<FailedOperation, Void> waitForBucketExistence(S3Client s3, String bucketName) {
        logger.debug("Waiting for bucket '{}' to exist.", bucketName);
        try {
            S3Waiter waiter = s3.waiter();
            HeadBucketRequest request =
                    HeadBucketRequest.builder().bucket(bucketName).build();

            WaiterOverrideConfiguration overrideConfig = WaiterOverrideConfiguration.builder()
                    .waitTimeout(java.time.Duration.ofSeconds(bucketWaitTimeoutSeconds))
                    .build();

            AtomicBoolean bucketExists = new AtomicBoolean(false);

            waiter.waitUntilBucketExists(request, overrideConfig)
                    .matched()
                    .response()
                    .ifPresentOrElse(
                            response -> {
                                logger.info("Bucket '{}' is confirmed to exist.", bucketName);
                                bucketExists.set(true);
                            },
                            () -> {
                                String error = String.format(
                                        "[Bucket '%s'] The bucket does not exist or an unexpected condition occurred.",
                                        bucketName);
                                logger.error(error);
                            });

            if (bucketExists.get()) {
                return Either.right(null);
            } else {
                String error = String.format(
                        "[Bucket '%s'] The bucket does not exist or an unexpected condition occurred.", bucketName);
                return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error))));
            }
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket '%s'] An error occurred while waiting for bucket to exist: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Checks whether a bucket exists in Amazon S3.
     * If the bucket does not exist, a {@code false} value is returned. If an error occurs,
     * a {@link FailedOperation} is returned with the error details.
     *
     * @param s3Client   the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket to check.
     * @return an {@link Either} containing {@code true} if the bucket exists, {@code false} if not,
     *         or a {@link FailedOperation} in case of an error.
     */
    public Either<FailedOperation, Boolean> doesBucketExist(S3Client s3Client, @NotBlank String bucketName) {
        try {
            logger.info("Checking if bucket '{}' exists.", bucketName);
            s3Client.getBucketAcl(r -> r.bucket(bucketName));
            logger.info("Bucket '{}' exists.", bucketName);

            return Either.right(true);
        } catch (AwsServiceException awsEx) {
            if (awsEx.statusCode() == HttpStatusCode.NOT_FOUND) {
                logger.info("Bucket '{}' does not exist.", bucketName);
                return Either.right(false);
            }
            String error = String.format(
                    "[Bucket %s] Error: An AWS service error occurred while checking the bucket existence. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, awsEx.getMessage());
            logger.error(error, awsEx);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, awsEx))));
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while checking the bucket existence. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Creates a folder-like structure in the specified S3 bucket.
     *
     * @param s3         the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     * @param folderPath the desired folder path.
     * @return an {@link Either} containing {@link FailedOperation} in case of error or {@code null} on success.
     */
    public Either<FailedOperation, Void> createFolder(
            @NotNull S3Client s3, @NotNull String bucketName, @NotNull String folderPath) {
        try {

            logger.info("Starting creation of the folder '{}' in bucket '{}'.", folderPath, bucketName);

            String formattedFolderPath = folderPath.endsWith("/") ? folderPath : folderPath + "/";

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(formattedFolderPath)
                    .build();

            s3.putObject(request, RequestBody.empty());

            // Wait until the folder is confirmed to exist
            Either<FailedOperation, Void> waitForObjectExistence =
                    waitForObjectExistence(s3, bucketName, formattedFolderPath);
            if (waitForObjectExistence.isLeft()) return Either.left(waitForObjectExistence.getLeft());

            logger.info("Folder '{}' in bucket '{}' is successfully created.", formattedFolderPath, bucketName);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s, Folder: %s] Error: An unexpected error occurred while creating the folder. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, folderPath, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Waits until the specified object exists in the given bucket.
     * The operation will block until the object is confirmed to exist or a timeout occurs.
     *
     * @param s3         the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     * @param objectKey  the key of the object to wait for.
     * @return an {@link Either} containing {@link FailedOperation} in case of error or {@code null} on success.
     */
    protected Either<FailedOperation, Void> waitForObjectExistence(S3Client s3, String bucketName, String objectKey) {
        logger.debug("Waiting for object '{}' in bucket '{}' to exist.", objectKey, bucketName);

        try {
            S3Waiter waiter = s3.waiter();
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            WaiterOverrideConfiguration overrideConfig = WaiterOverrideConfiguration.builder()
                    .waitTimeout(java.time.Duration.ofSeconds(objectWaitTimeoutSeconds))
                    .build();

            AtomicBoolean objectExists = new AtomicBoolean(false);
            waiter.waitUntilObjectExists(request, overrideConfig)
                    .matched()
                    .response()
                    .ifPresentOrElse(
                            response -> {
                                logger.info("Object '{}' is confirmed to exist in bucket '{}'.", objectKey, bucketName);
                                objectExists.set(true);
                            },
                            () -> {
                                String error = String.format(
                                        "[Bucket '%s', Object '%s'] The object does not exist in the bucket or an unexpected condition occurred.",
                                        bucketName, objectKey);
                                logger.error(error);
                            });

            if (objectExists.get()) {
                return Either.right(null);
            } else {
                String error = String.format(
                        "[Bucket '%s', Object '%s'] The object does not exist in the bucket or an unexpected condition occurred.",
                        bucketName, objectKey);
                return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error))));
            }
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket '%s', Object '%s'] An error occurred while waiting for object to exist in bucket. Details %s",
                    bucketName, objectKey, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Deletes all objects in the specified S3 bucket that match the given prefix.
     * If no objects are found with the specified prefix, no operation is performed.
     *
     * @param s3         the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     * @param prefix     the prefix to match for object deletion.
     * @return an {@link Either} containing {@link FailedOperation} in case of error or {@code null} on success.
     */
    public Either<FailedOperation, Void> deleteObjectsWithPrefix(S3Client s3, String bucketName, String prefix) {
        logger.info("Starting deletion of objects with prefix '{}' in bucket '{}'.", prefix, bucketName);

        try {
            // Ensure the prefix has a trailing slash
            String formattedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";

            // Retrieve all objects matching the specified prefix
            List<ObjectIdentifier> objectIdentifiers = fetchObjectsWithPrefix(s3, bucketName, formattedPrefix);

            // Add the folder itself to the list, even if it has no objects
            objectIdentifiers.add(
                    ObjectIdentifier.builder().key(formattedPrefix).build());

            if (objectIdentifiers.isEmpty()) {
                logger.info("No objects found with prefix '{}' in bucket '{}'.", prefix, bucketName);
                return Either.right(null);
            }

            // Delete all objects in batches to respect S3 API limits
            Either<FailedOperation, Void> deletedObjects = deleteObjectsInBatches(s3, bucketName, objectIdentifiers);
            if (deletedObjects.isLeft()) return Either.left(deletedObjects.getLeft());

            logger.info("Successfully deleted all objects with prefix '{}' in bucket '{}'.", prefix, bucketName);
            return Either.right(null);

        } catch (AwsServiceException awsEx) {
            String error = String.format(
                    "[Bucket: '%s', Prefix: '%s'] An AWS service error occurred during object deletion. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, prefix, awsEx.awsErrorDetails().errorMessage());
            logger.error(error, awsEx);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, awsEx))));
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: '%s', Prefix: '%s'] An unexpected error occurred during object deletion. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    bucketName, prefix, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Retrieves all objects in the specified bucket that match the given prefix.
     *
     * @param s3         The S3 client instance.
     * @param bucketName The name of the bucket.
     * @param prefix     The prefix to match.
     * @return A list of {@link ObjectIdentifier} for each matching object.
     */
    private List<ObjectIdentifier> fetchObjectsWithPrefix(S3Client s3, String bucketName, String prefix) {
        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        ListObjectsV2Request listRequest =
                ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();

        ListObjectsV2Response listResponse;
        do {
            // List objects matching the prefix
            listResponse = s3.listObjectsV2(listRequest);

            // Convert each object into an ObjectIdentifier and add it to the list
            if (listResponse.contents() != null) {
                listResponse
                        .contents()
                        .forEach(object -> objectIdentifiers.add(
                                ObjectIdentifier.builder().key(object.key()).build()));
            }
            // Update the request with the continuation token if the result is truncated
            listRequest = listRequest.toBuilder()
                    .continuationToken(listResponse.nextContinuationToken())
                    .build();
        } while (Boolean.TRUE.equals(listResponse.isTruncated())); // Continue if more objects are available

        return objectIdentifiers;
    }

    /**
     * Deletes objects in batches to comply with the S3 API limit of 1000 objects per request.
     *
     * @param s3                the {@link S3Client} used to perform the operation.
     * @param bucketName        the name of the bucket.
     * @param objectIdentifiers the list of objects to delete.
     * @return an {@link Either} containing {@link FailedOperation} in case of errors or {@code null} on success.
     */
    protected Either<FailedOperation, Void> deleteObjectsInBatches(
            S3Client s3, String bucketName, List<ObjectIdentifier> objectIdentifiers) {
        int batchSize = 1000; // S3 API limit for batch deletion
        ArrayList<Problem> problems = new ArrayList<>();

        for (int i = 0; i < objectIdentifiers.size(); i += batchSize) {
            // Create a batch of up to 1000 objects
            List<ObjectIdentifier> batch =
                    objectIdentifiers.subList(i, Math.min(i + batchSize, objectIdentifiers.size()));

            // Build the delete request for the current batch
            DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(batch).build())
                    .build();

            // Execute the delete request
            DeleteObjectsResponse response = s3.deleteObjects(deleteRequest);

            response.errors().forEach(s3Error -> {
                String error =
                        String.format("Error deleting object with key '%s': %s", s3Error.key(), s3Error.message());
                logger.error(error);
                problems.add(new Problem(error));
            });
        }

        if (problems.isEmpty()) return Either.right(null);
        return Either.left(new FailedOperation(
                String.format("[Bucket '%s'] Error(s) during object deletion.", bucketName), problems));
    }
}
