package com.witboost.provisioning.s3.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.s3.model.BucketTag;
import com.witboost.provisioning.s3.model.IntelligentTieringConfiguration;
import com.witboost.provisioning.s3.model.LifeCycleConfiguration;
import com.witboost.provisioning.s3.model.LifeCycleConfigurationPermanentlyDelete;
import com.witboost.provisioning.s3.model.S3Specific;
import io.vavr.control.Either;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sts.StsClient;

@SpringBootTest
public class BucketManagerTest {

    @Mock
    private KmsClient kmsClient;

    @Mock
    private S3Client s3Client;

    @MockitoBean
    private StsClient stsClient;

    @Autowired
    private BucketManager bucketManager;

    private S3Specific s3Specific;

    private MockedStatic<Files> mockedFiles;

    private String bucketName = "my-bucket";

    @BeforeEach
    public void setUp() {

        MockitoAnnotations.openMocks(this);

        mockedFiles = mockStatic(Files.class);
        mockedFiles
                .when(() -> Files.readAllBytes(any(Path.class)))
                .thenReturn("{\"Statement\": [{\"Effect\": \"Allow\"}]}".getBytes());
        String region = "us-east-1";

        BucketTag tag = new BucketTag();
        tag.setKey("tagKey");
        tag.setValue("tagValue");

        s3Specific = new S3Specific();
        s3Specific.setRegion(region);
        s3Specific.setMultipleVersion(true);
        s3Specific.setBucketTags(List.of(tag));
        s3Specific.setServerSideEncryption(ServerSideEncryption.AES256);
        LifeCycleConfiguration lifeCycleConfiguration = new LifeCycleConfiguration();
        LifeCycleConfigurationPermanentlyDelete lifeCycleConfigurationPermanentlyDelete =
                new LifeCycleConfigurationPermanentlyDelete();
        lifeCycleConfigurationPermanentlyDelete.setDaysAfterBecomeNonCurrent(15);
        lifeCycleConfigurationPermanentlyDelete.setNumberOfVersionsToRetain(8);
        lifeCycleConfiguration.setPermanentlyDelete(lifeCycleConfigurationPermanentlyDelete);
        IntelligentTieringConfiguration intelligentTieringConfiguration = new IntelligentTieringConfiguration();
        intelligentTieringConfiguration.setArchiveAccessTierEnabled(false);
        intelligentTieringConfiguration.setDeepArchiveAccessTierEnabled(false);
        s3Specific.setLifeCycleConfiguration(lifeCycleConfiguration);
        s3Specific.setIntelligentTieringConfiguration(intelligentTieringConfiguration);
    }

    @AfterEach
    void tearDown() {
        mockedFiles.close();
    }

    @Test
    public void testCreateFolder_success() {
        String folderPath = "my-folder";

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(mock(PutObjectResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);

        WaiterResponse waiterResponse = mock(WaiterResponse.class);
        ResponseOrException<HeadObjectResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadObjectResponse> response = Optional.of(mock(HeadObjectResponse.class));
        when(responseOrException.response()).thenReturn(response);

        when(s3Waiter.waitUntilObjectExists(any(HeadObjectRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        Either<FailedOperation, Void> result = bucketManager.createFolder(s3Client, bucketName, folderPath);

        assertTrue(result.isRight());
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    public void testCreateFolder_failure_objectNotExists() {
        String folderPath = "my-folder/";

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(mock(PutObjectResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);

        WaiterResponse<HeadObjectResponse> waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilObjectExists(any(HeadObjectRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadObjectResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);
        when(responseOrException.response()).thenReturn(Optional.empty());

        Either<FailedOperation, Void> result = bucketManager.createFolder(s3Client, bucketName, folderPath);

        assertTrue(result.isLeft());
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket 'my-bucket', Object 'my-folder/'] The object does not exist in the bucket or an unexpected condition occurred.");
    }

    @Test
    public void testCreateFolder_failure() {
        String folderPath = "my-folder";

        doThrow(new RuntimeException("S3 service unavailable"))
                .when(s3Client)
                .putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Either<FailedOperation, Void> result = bucketManager.createFolder(s3Client, bucketName, folderPath);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    public void testCreateOrUpdateBucket_success() {
        s3Specific.setBucketTags(null);
        s3Specific.setServerSideEncryption(ServerSideEncryption.AWS_KMS);
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                .build();

        when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isRight());
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testCreateOrUpdateBucketIntelligentTearing_success() {
        s3Specific.setBucketTags(null);
        IntelligentTieringConfiguration intelligentTieringConfiguration = new IntelligentTieringConfiguration();
        intelligentTieringConfiguration.setArchiveAccessTierEnabled(true);
        intelligentTieringConfiguration.setArchiveAccessTierDays(90);
        intelligentTieringConfiguration.setDeepArchiveAccessTierEnabled(true);
        intelligentTieringConfiguration.setDeepArchiveAccessTierDays(180);
        s3Specific.setIntelligentTieringConfiguration(intelligentTieringConfiguration);
        s3Specific.setServerSideEncryption(ServerSideEncryption.AWS_KMS);
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                .build();

        when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isRight());
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testCreateOrUpdateBucketNoMultipleVersioning_errorPuttingBucketTags() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        when(s3Client.putBucketTagging(any(PutBucketTaggingRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(BucketLocationConstraint.EU_CENTRAL_1);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        s3Specific.setMultipleVersion(false);
        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());

        System.out.println(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket: my-bucket] Error: An unexpected error occurred while applying tags to the bucket. Details: runtime exception");
    }

    @Test
    public void testCreateOrUpdateBucketNoMultipleVersioning_success() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(BucketLocationConstraint.EU_CENTRAL_1);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        s3Specific.setMultipleVersion(false);
        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isRight());
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testCreateBucket_failure_errorWaitingForOrUpdateBucket() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));
        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket 'my-bucket'] An error occurred while waiting for bucket to exist: runtime exception");
    }

    @Test
    void testCreateBucket_success_existingOrUpdateBucketInRegion() {
        s3Specific.setRegion("eu-central-1");

        when(s3Client.listBuckets())
                .thenReturn(ListBucketsResponse.builder()
                        .buckets(Bucket.builder().name(bucketName).build())
                        .build());
        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(BucketLocationConstraint.EU_CENTRAL_1);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isRight());
        verify(s3Client, times(1)).putBucketPolicy(any(PutBucketPolicyRequest.class));
    }

    @Test
    public void testCreateBucket_failure_existingOrUpdateBucketInAnotherRegion() {
        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        List<Bucket> bucketList = List.of(Bucket.builder().name(bucketName).build());
        when(listBucketsResponse.buckets()).thenReturn(bucketList);
        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(BucketLocationConstraint.EU_CENTRAL_1);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "The bucket already exists in region eu-central-1 and cannot be created or updated in region us-east-1.");
    }

    @Test
    public void testCreateBucket_failure_errorInOrUpdateBucketExists() {
        when(s3Client.listBuckets()).thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket: my-bucket] Error: An unexpected error occurred while checking the bucket existence.");
        assert result.getLeft().message().contains("Details: runtime exception");
    }

    @Test
    public void testCreateOrUpdateBucket_failure_errorGettingRegion() {
        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        List<Bucket> bucketList = List.of(Bucket.builder().name(bucketName).build());
        when(listBucketsResponse.buckets()).thenReturn(bucketList);

        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        s3Specific.setRegion("us-west-2");
        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket: my-bucket] Error: An unexpected error occurred while getting the region of the bucket.");
        assert result.getLeft().message().contains("Details: runtime exception");
    }

    @Test
    public void testCreateOrUpdateBucket_exception() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));
        when(s3Client.createBucket(any(CreateBucketRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class))).thenReturn(waiterResponse);
        var responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft().message().contains("Details: runtime exception");
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testDeleteObjectsWithPrefix_success() {
        String prefix = "my-prefix/";

        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(prefix + "file1.txt").build())
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResponse);
        DeleteObjectsResponse deleteObjectsResponse = mock(DeleteObjectsResponse.class);
        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteObjectsResponse);

        Either<FailedOperation, Void> result = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, prefix);

        assertTrue(result.isRight());
        verify(s3Client, times(1)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    public void testDeleteObjectsWithPrefix_success_emptyList() {

        String prefix = "my-prefix/";

        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(prefix + "file1.txt").build())
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mock(ListObjectsV2Response.class));
        DeleteObjectsResponse deleteObjectsResponse = mock(DeleteObjectsResponse.class);
        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteObjectsResponse);

        Either<FailedOperation, Void> result = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, prefix);

        assertTrue(result.isRight());
        verify(s3Client, times(1)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    public void testDeleteObjectsInBatches_withErrors() {

        List<ObjectIdentifier> objectIdentifiers = List.of(
                ObjectIdentifier.builder().key("file1.txt").build(),
                ObjectIdentifier.builder().key("file2.txt").build());

        S3Error error1 =
                S3Error.builder().key("file1.txt").message("Access Denied").build();

        S3Error error2 =
                S3Error.builder().key("file2.txt").message("No such key").build();

        DeleteObjectsResponse deleteObjectsResponse =
                DeleteObjectsResponse.builder().errors(error1, error2).build();

        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteObjectsResponse);

        Either<FailedOperation, Void> result =
                bucketManager.deleteObjectsInBatches(s3Client, bucketName, objectIdentifiers);

        assertTrue(result.isLeft());
        FailedOperation failedOperation = result.getLeft();
        assertNotNull(failedOperation);
        assertEquals(2, failedOperation.problems().size());
        assertTrue(failedOperation.problems().stream()
                .anyMatch(problem -> problem.getMessage().contains("Error deleting object with key 'file1.txt'")));
        assertTrue(failedOperation.problems().stream()
                .anyMatch(problem -> problem.getMessage().contains("Error deleting object with key 'file2.txt'")));

        verify(s3Client, times(1)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    public void testDeleteObjectsWithPrefix_failure_emptyObjectIdentifiers() {
        String prefix = "my-prefix";

        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(Collections.emptyList())
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResponse);

        DeleteObjectsResponse deleteObjectsResponse = mock(DeleteObjectsResponse.class);
        when(s3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteObjectsResponse);

        Either<FailedOperation, Void> result = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, prefix);

        assertTrue(result.isRight());
    }

    @Test
    public void testDeleteObjectsWithPrefix_failure_AWSException() {
        String prefix = "my-prefix/";

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorMessage("AWS error")
                                .build())
                        .build());

        Either<FailedOperation, Void> result = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, prefix);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        assert result.getLeft().message().contains("Details: AWS error");
    }

    @Test
    public void testDeleteObjectsWithPrefix_failure_Exception() {
        String prefix = "my-prefix/";

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, prefix);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        assert result.getLeft().message().contains("Details: runtime exception");
    }

    @Test
    void testWaitForBucketExistence_failure_EmptyBucket() {
        S3Client mockS3Client = mock(S3Client.class);
        S3Waiter mockWaiter = mock(S3Waiter.class);
        HeadBucketRequest mockRequest = mock(HeadBucketRequest.class);

        WaiterResponse<HeadBucketResponse> mockWaiterResponse = mock(WaiterResponse.class);
        ResponseOrException<HeadBucketResponse> mockResponseOrException = mock(ResponseOrException.class);

        when(mockWaiterResponse.matched()).thenReturn(mockResponseOrException);
        when(mockResponseOrException.response()).thenReturn(Optional.empty());

        when(mockS3Client.waiter()).thenReturn(mockWaiter);
        when(mockWaiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(mockWaiterResponse);

        Either<FailedOperation, Void> result = bucketManager.waitForBucketExistence(mockS3Client, "test-bucket");

        assertTrue(result.isLeft(), "Expected operation to fail due to empty bucket.");
        verify(mockS3Client, times(1)).waiter();
        verify(mockWaiter, times(1))
                .waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class));

        String expectedError = "[Bucket 'test-bucket'] The bucket does not exist or an unexpected condition occurred.";
        assertEquals(expectedError, result.getLeft().problems().get(0).getMessage());
    }

    @Test
    void testWaitForBucketExistence_failure_exception() {
        S3Client mockS3Client = mock(S3Client.class);
        S3Waiter mockWaiter = mock(S3Waiter.class);
        HeadBucketRequest mockRequest = mock(HeadBucketRequest.class);

        WaiterResponse<HeadBucketResponse> mockWaiterResponse = mock(WaiterResponse.class);
        ResponseOrException<HeadBucketResponse> mockResponseOrException = mock(ResponseOrException.class);

        when(mockWaiterResponse.matched()).thenThrow(new RuntimeException("runtime exception"));
        when(mockResponseOrException.response()).thenReturn(Optional.empty());

        when(mockS3Client.waiter()).thenReturn(mockWaiter);
        when(mockWaiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(mockWaiterResponse);

        Either<FailedOperation, Void> result = bucketManager.waitForBucketExistence(mockS3Client, "test-bucket");

        assertTrue(result.isLeft(), "Expected operation to fail due to exception.");
        verify(mockS3Client, times(1)).waiter();
        verify(mockWaiter, times(1))
                .waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class));
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket 'test-bucket'] An error occurred while waiting for bucket to exist: runtime exception");
    }

    @Test
    void testWaitForObjectExistence_failure_exception() {
        S3Client mockS3Client = mock(S3Client.class);
        S3Waiter mockWaiter = mock(S3Waiter.class);
        HeadObjectRequest mockRequest = mock(HeadObjectRequest.class);

        WaiterResponse<HeadObjectResponse> mockWaiterResponse = mock(WaiterResponse.class);
        ResponseOrException<HeadObjectResponse> mockResponseOrException = mock(ResponseOrException.class);

        when(mockWaiterResponse.matched()).thenThrow(new RuntimeException("runtime exception"));
        when(mockResponseOrException.response()).thenReturn(Optional.empty());

        when(mockS3Client.waiter()).thenReturn(mockWaiter);
        when(mockWaiter.waitUntilObjectExists(any(HeadObjectRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(mockWaiterResponse);

        Either<FailedOperation, Void> result =
                bucketManager.waitForObjectExistence(mockS3Client, "test-bucket", "test-object");

        assertTrue(result.isLeft(), "Expected operation to fail due to exception.");
        verify(mockS3Client, times(1)).waiter();
        verify(mockWaiter, times(1))
                .waitUntilObjectExists(any(HeadObjectRequest.class), any(WaiterOverrideConfiguration.class));
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket 'test-bucket', Object 'test-object'] An error occurred while waiting for object to exist");
    }

    @Test
    void testIsKmsEnabled_true() {
        GetBucketEncryptionResponse encryptionResponse = GetBucketEncryptionResponse.builder()
                .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                        .rules(ServerSideEncryptionRule.builder()
                                .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                        .sseAlgorithm(ServerSideEncryption.AWS_KMS)
                                        .build())
                                .build())
                        .build())
                .build();

        boolean result = bucketManager.isKmsEnabled(encryptionResponse);
        assertTrue(result, "Expected KMS to be enabled");
    }

    @Test
    void testIsKmsEnabled_false() {
        GetBucketEncryptionResponse encryptionResponse = GetBucketEncryptionResponse.builder()
                .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                        .rules(ServerSideEncryptionRule.builder()
                                .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                        .sseAlgorithm(ServerSideEncryption.AES256)
                                        .build())
                                .build())
                        .build())
                .build();

        boolean result = bucketManager.isKmsEnabled(encryptionResponse);
        assertFalse(result, "Expected KMS to be disabled");
    }

    @Test
    void testIsKmsEnabled_nullResponse() {
        boolean result = bucketManager.isKmsEnabled(null);
        assertFalse(result, "Expected KMS to be disabled with null response");
    }

    @Test
    void testEnableBucketVersioning_exception() {
        when(s3Client.putBucketVersioning(any(PutBucketVersioningRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));
        Either<FailedOperation, Void> result =
                bucketManager.enableBucketVersioning(s3Client, "bucket", new LifeCycleConfiguration());

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("An unexpected error occurred while enabling versioning"));
    }

    @Test
    void testEnableBucketVersioningNullPermanentDelete_exception() {
        when(s3Client.putBucketVersioning(any(PutBucketVersioningRequest.class)))
                .thenReturn(any(PutBucketVersioningResponse.class));
        Either<FailedOperation, Void> result =
                bucketManager.enableBucketVersioning(s3Client, "bucket", new LifeCycleConfiguration());

        assertTrue(result.isRight());
    }

    @Test
    void testApplyLifeCycleConfiguration_exception() {
        when(s3Client.putBucketLifecycleConfiguration(any(PutBucketLifecycleConfigurationRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));
        Either<FailedOperation, Void> result =
                bucketManager.applyLifeCycleConfiguration(s3Client, "bucket", new LifeCycleConfiguration());

        assertTrue(result.isLeft());
        assertTrue(result.getLeft()
                .message()
                .contains("An unexpected error occurred while applying lifecycle configuration."));
    }

    @Test
    void testApplyBucketTags_success() {
        String bucketName = "test-bucket";
        BucketTag bucketTag1 = new BucketTag();
        bucketTag1.setKey("key1");
        bucketTag1.setValue("value1");

        BucketTag bucketTag2 = new BucketTag();
        bucketTag1.setKey("key2");
        bucketTag1.setValue("value2");

        List<BucketTag> tags = List.of(bucketTag1, bucketTag2);

        bucketManager.applyBucketTags(s3Client, bucketName, tags);

        verify(s3Client, times(1)).putBucketTagging(any(PutBucketTaggingRequest.class));
    }

    @Test
    void testApplyBucketTags_emptyTags() {
        String bucketName = "test-bucket";
        List<BucketTag> tags = List.of();

        bucketManager.applyBucketTags(s3Client, bucketName, tags);

        verify(s3Client, never()).putBucketTagging(any(PutBucketTaggingRequest.class));
    }

    @Test
    void testApplyBucketPolicyForSecureTransport_success() {
        String bucketName = "test-bucket";

        bucketManager.applyBucketPolicyForSecureTransport(s3Client, bucketName);

        verify(s3Client, times(1)).putBucketPolicy(any(PutBucketPolicyRequest.class));
    }

    @Test
    void testApplyBucketPolicyForSecureTransport_success_UsingCustomPolicyPath() {
        String customPolicyPath = "/custom/path/bucket-policy.json";

        System.setProperty("bucket.policy.path", customPolicyPath);
        Either<FailedOperation, Void> result = bucketManager.applyBucketPolicyForSecureTransport(s3Client, bucketName);
        assertTrue(result.isRight());
        assertEquals(customPolicyPath, System.getProperty("bucket.policy.path"));
        System.clearProperty("bucket.policy.path");
    }

    @Test
    void testApplyBucketPolicyForSecureTransport_IOException() {
        String bucketName = "test-bucket";
        String customPolicyPath = "/custom/path/kms-policy.json";

        System.setProperty("bucket.policy.path", customPolicyPath);

        mockedFiles
                .when(() -> Files.readAllBytes(Paths.get(customPolicyPath)))
                .thenThrow(new IOException("IO Exception"));
        Either<FailedOperation, Void> result = bucketManager.applyBucketPolicyForSecureTransport(s3Client, bucketName);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft()
                .message()
                .contains(
                        "An unexpected error occurred while applying secure transport policy. Details: IO Exception"));

        System.clearProperty("kms.policy.path");
    }

    @Test
    void testEnableAES256() {
        String bucketName = "test-bucket";
        bucketManager.enableAES256(s3Client, bucketName);

        verify(s3Client, times(1)).putBucketEncryption(any(PutBucketEncryptionRequest.class));
    }

    @Test
    void testEnableAES256_exception() {
        String bucketName = "test-bucket";
        when(s3Client.putBucketEncryption(any(PutBucketEncryptionRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));
        Either<FailedOperation, Void> result = bucketManager.enableAES256(s3Client, bucketName);

        assertTrue(result.isLeft());
        assertTrue(
                result.getLeft().message().contains("An unexpected error occurred while enabling AES256 encryption."));
    }

    @Test
    void testEnableKMS_KMSCurrentEncryption() {
        String bucketName = "test-bucket";
        s3Specific.setServerSideEncryption(ServerSideEncryption.AWS_KMS);

        GetBucketEncryptionResponse encryptionResponse = GetBucketEncryptionResponse.builder()
                .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                        .rules(ServerSideEncryptionRule.builder()
                                .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                        .sseAlgorithm(ServerSideEncryption.AWS_KMS)
                                        .build())
                                .build())
                        .build())
                .build();

        when(s3Client.getBucketEncryption(any(GetBucketEncryptionRequest.class)))
                .thenReturn(encryptionResponse);
        Either<FailedOperation, Void> result =
                bucketManager.enableKMS(s3Client, kmsClient, bucketName, s3Specific, "accountID");

        assertTrue(result.isRight());
        verify(s3Client, times(0)).putBucketEncryption(any(PutBucketEncryptionRequest.class));
    }

    @Test
    void testEnableKMS_Exception() {
        String bucketName = "test-bucket";
        s3Specific.setServerSideEncryption(ServerSideEncryption.AWS_KMS);

        when(s3Client.getBucketEncryption(any(GetBucketEncryptionRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));
        Either<FailedOperation, Void> result =
                bucketManager.enableKMS(s3Client, kmsClient, bucketName, s3Specific, "accountID");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("An unexpected error occurred while enabling KMS encryption"));
    }

    @Test
    void testDoesBucketExist_true() {
        String bucketName = "test-bucket";
        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        when(listBucketsResponse.buckets())
                .thenReturn(List.of(Bucket.builder().name(bucketName).build()));

        Either<FailedOperation, Boolean> result = bucketManager.doesBucketExist(s3Client, bucketName);

        assertTrue(result.isRight());
        assertTrue(result.get(), "Expected bucket to exist");
    }

    @Test
    void testDoesBucketExist_false() {
        String bucketName = "test-bucket";
        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        when(listBucketsResponse.buckets()).thenReturn(List.of());

        Either<FailedOperation, Boolean> result = bucketManager.doesBucketExist(s3Client, bucketName);

        assertTrue(result.isRight());
        assertFalse(result.get(), "Expected bucket not to exist");
    }

    @Test
    void testDoesBucketExist_exception() {
        String bucketName = "test-bucket";
        when(s3Client.listBuckets()).thenThrow(new RuntimeException("S3 error"));

        Either<FailedOperation, Boolean> result = bucketManager.doesBucketExist(s3Client, bucketName);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft().message(), "Expected an error message");
    }

    @Test
    public void testCreateOrUpdateBucket_failure_bucketPolicySecureTransport() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        doThrow(new RuntimeException("Bucket policy error"))
                .when(s3Client)
                .putBucketPolicy(any(PutBucketPolicyRequest.class));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("Bucket policy error"));
    }

    @Test
    public void testCreateOrUpdateBucket_failure_enableKMS() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                .build();

        when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        doThrow(new RuntimeException("KMS encryption error"))
                .when(s3Client)
                .putBucketEncryption(any(PutBucketEncryptionRequest.class));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("KMS encryption error"));
    }

    @Test
    public void testCreateOrUpdateBucket_failure_enableAES256() {
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        doThrow(new RuntimeException("AES256 encryption error"))
                .when(s3Client)
                .putBucketEncryption(any(PutBucketEncryptionRequest.class));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("AES256 encryption error"));
    }

    @Test
    public void testCreateOrUpdateBucket_failure_multipleVersioning() {

        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        doThrow(new RuntimeException("Versioning error"))
                .when(s3Client)
                .putBucketVersioning(any(PutBucketVersioningRequest.class));

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("Versioning error"));
    }

    @Test
    public void testCreateOrUpdateBucket_failure_errorGeneratingKey() {
        s3Specific.setBucketTags(null);
        s3Specific.setServerSideEncryption(ServerSideEncryption.AWS_KMS);
        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);

        CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                .build();

        when(kmsClient.createKey(any(CreateKeyRequest.class)))
                .thenThrow(new RuntimeException("runtime exception creating KMS key"));

        ResponseOrException<HeadBucketResponse> responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Optional<HeadBucketResponse> response = Optional.of(mock(HeadBucketResponse.class));
        when(responseOrException.response()).thenReturn(response);

        Either<FailedOperation, Void> result =
                bucketManager.createOrUpdateBucket(s3Client, kmsClient, bucketName, s3Specific, "accountId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("runtime exception creating KMS key"));
    }
}
