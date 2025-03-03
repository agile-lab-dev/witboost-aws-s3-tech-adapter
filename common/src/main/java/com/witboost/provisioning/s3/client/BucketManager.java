package com.witboost.provisioning.s3.client;

import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.s3.model.BucketTag;
import com.witboost.provisioning.s3.model.LifeCycleConfiguration;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import jakarta.validation.constraints.NotBlank;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
import software.amazon.awssdk.services.kms.KmsClient;
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
     * Creates an S3 bucket if it does not already exist. If the bucket exists in a different region, an error is returned.
     * Additionally, this method can update the bucket's configuration by applying tags, encryption settings, and versioning.
     *
     * @param s3Client       the {@link S3Client} used to interact with Amazon S3.
     * @param kmsClient the {@link KmsClient} used for AWS KMS encryption.
     * @param bucketName     the name of the bucket to create.
     * @param s3Specific     S3-specific configurations.
     * @param accountId      the AWS account ID. This is used when configuring encryption or permissions for the bucket.
     *
     * @return an {@link Either} containing:
     *         - {@link FailedOperation} in case of error, detailing the failure reason.
     *         - {@code null} on success, indicating that the bucket was successfully created or updated.
     *
     */
    public Either<FailedOperation, Void> createOrUpdateBucket(
            @NotNull S3Client s3Client,
            @NotNull KmsClient kmsClient,
            @NotNull String bucketName,
            S3Specific s3Specific,
            String accountId) {
        try {
            String region = s3Specific.getRegion();

            Either<FailedOperation, Boolean> bucketExists = doesBucketExist(s3Client, bucketName);
            if (bucketExists.isLeft()) return Either.left(bucketExists.getLeft());

            if (!bucketExists.get()) {
                logger.info("Starting creation of bucket '{}' in region '{}'.", bucketName, region);

                CreateBucketRequest createRequest =
                        CreateBucketRequest.builder().bucket(bucketName).build();

                s3Client.createBucket(createRequest);
                Either<FailedOperation, Void> waitForBucketExistence = waitForBucketExistence(s3Client, bucketName);
                if (waitForBucketExistence.isLeft()) return Either.left(waitForBucketExistence.getLeft());

                logger.info("Bucket '{}' created in region '{}'.", bucketName, region);

            } else {
                Either<FailedOperation, String> existingRegion = getBucketRegion(s3Client, bucketName);
                if (existingRegion.isLeft()) return Either.left(existingRegion.getLeft());

                if (!region.equals(existingRegion.get())) {
                    String error = String.format(
                            "[Bucket: %s] Error: The bucket already exists in region %s and cannot be created or updated in region %s.",
                            bucketName, existingRegion.get(), region);
                    logger.error(error);
                    return Either.left(new FailedOperation(error, List.of(new Problem(error))));
                }
            }

            logger.info("Starting the update of the bucket configurations of '{}'.", bucketName);

            List<BucketTag> tags = s3Specific.getBucketTags();
            var bucketTagging = applyBucketTags(s3Client, bucketName, tags);
            if (bucketTagging.isLeft()) return Either.left(bucketTagging.getLeft());

            var bucketPolicySecureTransport = applyBucketPolicyForSecureTransport(s3Client, bucketName);
            if (bucketPolicySecureTransport.isLeft()) return Either.left(bucketPolicySecureTransport.getLeft());

            if (s3Specific.getServerSideEncryption().equals(ServerSideEncryption.AWS_KMS)) {
                var enableKMS = enableKMS(s3Client, kmsClient, bucketName, s3Specific, accountId);
                if (enableKMS.isLeft()) return Either.left(enableKMS.getLeft());
            } else { // default encryption
                var enableAES256 = enableAES256(s3Client, bucketName);
                if (enableAES256.isLeft()) return Either.left(enableAES256.getLeft());
            }

            if (s3Specific.getMultipleVersion()) {
                var multipleVersioning =
                        enableBucketVersioning(s3Client, bucketName, s3Specific.getLifeCycleConfiguration());
                if (multipleVersioning.isLeft()) return Either.left(multipleVersioning.getLeft());
            }

            logger.info("Bucket '{}' is successfully created or updated in region '{}'.", bucketName, region);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while creating the bucket. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }

    /**
     * Enables AES256 encryption for the specified S3 bucket.
     *
     * @param s3Client   the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     *
     * @return an {@link Either} containing:
     *         - {@code null} if AES256 encryption is successfully applied.
     *         - {@link FailedOperation} if an error occurs while applying encryption settings.
     */
    protected Either<FailedOperation, Void> enableAES256(@NotNull S3Client s3Client, @NotNull String bucketName) {
        try {
            logger.info("Enabling AES256 encryption for bucket: '{}'.", bucketName);

            s3Client.putBucketEncryption(PutBucketEncryptionRequest.builder()
                    .bucket(bucketName)
                    .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                            .rules(ServerSideEncryptionRule.builder()
                                    .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                            .sseAlgorithm(ServerSideEncryption.AES256)
                                            .build())
                                    .build())
                            .build())
                    .build());
            logger.info("AES256 encryption enabled for bucket: '{}'.", bucketName);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while enabling AES256 encryption. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }

    /**
     * Enables AWS KMS encryption for the specified S3 bucket.
     * If KMS encryption is already enabled, no changes are applied.
     *
     * @param s3Client       the {@link S3Client} used to perform the operation.
     * @param kmsClient the {@link KmsClient} used for key management.
     * @param bucketName     the name of the bucket.
     * @param s3Specific     S3-specific configurations.
     * @param accountId      the AWS account ID, required for KMS key creation.
     *
     * @return an {@link Either} containing:
     *         - {@code null} if KMS encryption is successfully enabled or already active.
     *         - {@link FailedOperation} if an error occurs while retrieving encryption settings or applying KMS.
     */
    protected Either<FailedOperation, Void> enableKMS(
            @NotNull S3Client s3Client,
            @NotNull KmsClient kmsClient,
            @NotNull String bucketName,
            @NotNull S3Specific s3Specific,
            String accountId) {

        try {
            logger.info(
                    "Request to enable KMS encryption. Checking current encryption settings for bucket: '{}'.",
                    bucketName);

            GetBucketEncryptionResponse currentEncryption = s3Client.getBucketEncryption(
                    GetBucketEncryptionRequest.builder().bucket(bucketName).build());

            if (isKmsEnabled(currentEncryption)) {
                logger.info("KMS encryption is already enabled for bucket: '{}'.", bucketName);
                return Either.right(null);
            }

            logger.info("KMS encryption not enabled. Enabling KMS encryption for bucket: '{}'.", bucketName);

            Either<FailedOperation, String> createKey = new KmsManager()
                    .createKey(
                            kmsClient,
                            accountId,
                            String.format("Witboost-generated KMS key for bucket '%s'", bucketName),
                            s3Specific.getBucketTags());
            if (createKey.isLeft()) return Either.left(createKey.getLeft());

            String keyId = createKey.get();

            s3Client.putBucketEncryption(PutBucketEncryptionRequest.builder()
                    .bucket(bucketName)
                    .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                            .rules(ServerSideEncryptionRule.builder()
                                    .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                            .sseAlgorithm(ServerSideEncryption.AWS_KMS)
                                            .kmsMasterKeyID(keyId)
                                            .build())
                                    .build())
                            .build())
                    .build());
            logger.info("KMS encryption enabled with key ID '{}' for bucket: '{}'.", keyId, bucketName);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while enabling KMS encryption. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }

    protected boolean isKmsEnabled(GetBucketEncryptionResponse response) {
        if (response == null || response.serverSideEncryptionConfiguration() == null) {
            return false;
        }

        ServerSideEncryptionConfiguration config = response.serverSideEncryptionConfiguration();

        for (ServerSideEncryptionRule rule : config.rules()) {
            ServerSideEncryptionByDefault encryptionByDefault = rule.applyServerSideEncryptionByDefault();
            if (encryptionByDefault != null && "aws:kms".equals(encryptionByDefault.sseAlgorithmAsString())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Enables versioning for the specified bucket and applies lifecycle configuration.
     *
     * @param s3Client             the {@link S3Client} used to perform the operation.
     * @param bucketName           the name of the bucket.
     * @param lifeCycleConfiguration the lifecycle configuration to apply.
     *
     * @return an {@link Either} containing:
     *         - {@code null} if versioning is successfully enabled.
     *         - {@link FailedOperation} if an error occurs while enabling versioning.
     */
    protected Either<FailedOperation, Void> enableBucketVersioning(
            @NotNull S3Client s3Client, @NotNull String bucketName, LifeCycleConfiguration lifeCycleConfiguration) {

        try {
            logger.info("Enabling versioning for bucket: '{}'.", bucketName);

            VersioningConfiguration versioningConfiguration = VersioningConfiguration.builder()
                    .status(BucketVersioningStatus.ENABLED)
                    .build();
            PutBucketVersioningRequest putBucketVersioningRequest = PutBucketVersioningRequest.builder()
                    .bucket(bucketName)
                    .versioningConfiguration(versioningConfiguration)
                    .build();
            s3Client.putBucketVersioning(putBucketVersioningRequest);

            logger.info("Versioning enabled for bucket: '{}'.", bucketName);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while enabling versioning. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }

        if (lifeCycleConfiguration.getPermanentlyDelete() != null)
            return applyLifeCycleConfiguration(s3Client, bucketName, lifeCycleConfiguration);
        return Either.right(null);
    }

    protected Either<FailedOperation, Void> applyLifeCycleConfiguration(
            @NotNull S3Client s3Client, @NotNull String bucketName, LifeCycleConfiguration lifeCycleConfiguration) {
        try {
            logger.info("Applying lifecycle configuration for bucket: '{}'.", bucketName);

            BucketLifecycleConfiguration bucketLifecycleConfiguration = BucketLifecycleConfiguration.builder()
                    .rules(LifecycleRule.builder()
                            .id("witboostLifeCycleConfiguration")
                            .status(ExpirationStatus.ENABLED)
                            .filter(LifecycleRuleFilter.builder().build())
                            .noncurrentVersionExpiration(NoncurrentVersionExpiration.builder()
                                    .noncurrentDays(lifeCycleConfiguration
                                            .getPermanentlyDelete()
                                            .getDaysAfterBecomeNonCurrent())
                                    .newerNoncurrentVersions(lifeCycleConfiguration
                                            .getPermanentlyDelete()
                                            .getNumberOfVersionsToRetain())
                                    .build())
                            .build())
                    .build();

            s3Client.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                    .bucket(bucketName)
                    .transitionDefaultMinimumObjectSize(TransitionDefaultMinimumObjectSize.ALL_STORAGE_CLASSES_128_K)
                    .lifecycleConfiguration(bucketLifecycleConfiguration)
                    .build());

            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while applying lifecycle configuration. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }

    /**
     * Applies a bucket policy enforcing secure transport (HTTPS-only access).
     * The policy should be contained in a json file.
     *
     * @param s3Client   the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     *
     * @return an {@link Either} containing:
     *         - {@code null} if the policy is successfully applied.
     *         - {@link FailedOperation} if an error occurs while reading the policy file or applying the policy.
     */
    protected Either<FailedOperation, Void> applyBucketPolicyForSecureTransport(
            @NotNull S3Client s3Client, @NotNull String bucketName) {

        try {
            logger.info("Applying secure transport policy for bucket: '{}'.", bucketName);

            String bucketPolicy;

            if (System.getProperty("bucket.policy.path") != null) {
                String policyPath = System.getProperty("bucket.policy.path");
                logger.info("Using custom bucket policy path: {}", policyPath);
                bucketPolicy = new String(Files.readAllBytes(Paths.get(policyPath)));

            } else {
                InputStream inputStream = getClass().getClassLoader().getResourceAsStream("bucket-policy.json");
                logger.info("Using default bucket policy path");
                bucketPolicy = new String(Objects.requireNonNull(inputStream).readAllBytes(), StandardCharsets.UTF_8);
            }

            logger.debug("Original bucket policy: {}", bucketPolicy);
            String updatedPolicy = bucketPolicy.replace("{bucketName}", bucketName);
            logger.debug("Updated bucket policy: {}", updatedPolicy);

            PutBucketPolicyRequest putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                    .bucket(bucketName)
                    .policy(updatedPolicy)
                    .build();

            s3Client.putBucketPolicy(putBucketPolicyRequest);

            logger.info("Secure transport policy applied for bucket: '{}'.", bucketName);
            return Either.right(null);

        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while applying secure transport policy. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }

    /**
     * Applies tags to the specified S3 bucket.
     * If no tags are provided, the method returns successfully without applying any changes.
     *
     * @param s3Client   the {@link S3Client} used to perform the operation.
     * @param bucketName the name of the bucket.
     * @param tags       the list of tags to apply.
     *
     * @return an {@link Either} containing:
     *         - {@code null} if tags are successfully applied or no tags were provided.
     *         - {@link FailedOperation} if an error occurs while applying the tags.
     */
    protected Either<FailedOperation, Void> applyBucketTags(
            @NotNull S3Client s3Client, @NotNull String bucketName, List<BucketTag> tags) {
        List<software.amazon.awssdk.services.s3.model.Tag> awsTags = new ArrayList<>();
        try {
            if (tags == null || tags.isEmpty()) {
                logger.info("No tags provided for bucket: '{}'. Skipping tag application.", bucketName);
                return Either.right(null);
            }
            logger.info("Applying tags to bucket: '{}'.", bucketName);

            tags.forEach(tag -> awsTags.add(software.amazon.awssdk.services.s3.model.Tag.builder()
                    .key(tag.getKey())
                    .value(tag.getValue())
                    .build()));

            s3Client.putBucketTagging(PutBucketTaggingRequest.builder()
                    .bucket(bucketName)
                    .tagging(Tagging.builder().tagSet(awsTags).build())
                    .build());

            logger.info("Tags successfully applied to bucket: '{}'.", bucketName);
            return Either.right(null);
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while applying tags to the bucket. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
                return Either.left(new FailedOperation(error, List.of(new Problem(error))));
            }
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket '%s'] An error occurred while waiting for bucket to exist: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
            boolean bucketExists = s3Client.listBuckets().buckets().stream()
                    .anyMatch(bucket -> bucket.name().equalsIgnoreCase(bucketName));

            logger.info("Does bucket '{}' exist? {}", bucketName, bucketExists);

            return Either.right(bucketExists);
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: %s] Error: An unexpected error occurred while checking the bucket existence. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
    public Either<FailedOperation, String> getBucketRegion(@NotNull S3Client s3Client, @NotBlank String bucketName) {
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
                    "[Bucket: %s] Error: An unexpected error occurred while getting the region of the bucket. Details: %s",
                    bucketName, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
                    "[Bucket: %s, Folder: %s] Error: An unexpected error occurred while creating the folder. Details: %s",
                    bucketName, folderPath, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
                return Either.left(new FailedOperation(error, List.of(new Problem(error))));
            }
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket '%s', Object '%s'] An error occurred while waiting for object to exist in bucket. Details %s",
                    bucketName, objectKey, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
                    "[Bucket: '%s', Prefix: '%s'] An AWS service error occurred during object deletion. Details: %s",
                    bucketName, prefix, awsEx.awsErrorDetails().errorMessage());
            logger.error(error, awsEx);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, awsEx))));
        } catch (Exception e) {
            String error = String.format(
                    "[Bucket: '%s', Prefix: '%s'] An unexpected error occurred during object deletion. Details: %s",
                    bucketName, prefix, e.getMessage());
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
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
