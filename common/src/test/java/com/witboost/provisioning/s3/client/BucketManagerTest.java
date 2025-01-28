package com.witboost.provisioning.s3.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.witboost.provisioning.model.common.FailedOperation;
import io.vavr.control.Either;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

@SpringBootTest
public class BucketManagerTest {

    @Mock
    private S3Client s3Client;

    @Autowired
    private BucketManager bucketManager;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateFolder_success() {
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
    public void testCreateBucket_success() {
        String bucketName = "my-bucket";
        String region = "us-east-1";

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

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, true);

        assertTrue(result.isRight());
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testCreateBucket_failure_errorWaitingForBucket() {
        String bucketName = "my-bucket";
        String region = "us-east-1";

        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));
        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket 'my-bucket'] An error occurred while waiting for bucket to exist: runtime exception");
    }

    @Test
    public void testCreateBucket_success_existingBucketInRegion() {
        String bucketName = "my-bucket";
        String region = "us-east-1";

        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        List<Bucket> bucketList = List.of(Bucket.builder().name(bucketName).build());
        when(listBucketsResponse.buckets()).thenReturn(bucketList);

        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(null);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        when(s3Client.createBucket(any(CreateBucketRequest.class))).thenReturn(mock(CreateBucketResponse.class));
        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class), any(WaiterOverrideConfiguration.class)))
                .thenReturn(waiterResponse);
        var responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isRight());
    }

    @Test
    public void testCreateBucket_failure_existingBucketInAnotherRegion() {
        String bucketName = "my-bucket";
        String region = "us-west-2";

        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        List<Bucket> bucketList = List.of(Bucket.builder().name(bucketName).build());
        when(listBucketsResponse.buckets()).thenReturn(bucketList);
        GetBucketLocationResponse getBucketLocationResponse = mock(GetBucketLocationResponse.class);
        when(getBucketLocationResponse.locationConstraint()).thenReturn(BucketLocationConstraint.EU_CENTRAL_1);
        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class))).thenReturn(getBucketLocationResponse);

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft().message().contains("The bucket already exists in a different region (eu-central-1)");
    }

    @Test
    public void testCreateBucket_failure_errorInBucketExists() {
        String bucketName = "my-bucket";
        String region = "us-west-2";

        when(s3Client.listBuckets()).thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket: my-bucket] Error: An unexpected error occurred while checking the bucket existence.");
        assert result.getLeft().message().contains("Details: runtime exception");
    }

    @Test
    public void testCreateBucket_failure_errorGettingRegion() {
        String bucketName = "my-bucket";
        String region = "us-west-2";

        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);
        when(s3Client.listBuckets()).thenReturn(listBucketsResponse);
        List<Bucket> bucketList = List.of(Bucket.builder().name(bucketName).build());
        when(listBucketsResponse.buckets()).thenReturn(bucketList);

        when(s3Client.getBucketLocation(any(GetBucketLocationRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft()
                .message()
                .contains(
                        "[Bucket: my-bucket] Error: An unexpected error occurred while getting the region of the bucket.");
        assert result.getLeft().message().contains("Details: runtime exception");
    }

    @Test
    public void testCreateBucket_exception() {
        String bucketName = "my-bucket";
        String region = "us-east-1";

        when(s3Client.listBuckets()).thenReturn(mock(ListBucketsResponse.class));

        when(s3Client.createBucket(any(CreateBucketRequest.class)))
                .thenThrow(new RuntimeException("runtime exception"));

        S3Waiter s3Waiter = mock(S3Waiter.class);
        when(s3Client.waiter()).thenReturn(s3Waiter);
        var waiterResponse = mock(WaiterResponse.class);
        when(s3Waiter.waitUntilBucketExists(any(HeadBucketRequest.class))).thenReturn(waiterResponse);
        var responseOrException = mock(ResponseOrException.class);
        when(waiterResponse.matched()).thenReturn(responseOrException);

        Either<FailedOperation, Void> result = bucketManager.createBucket(s3Client, bucketName, region, false);

        assertTrue(result.isLeft());
        assertNotNull(result.getLeft());
        assert result.getLeft().message().contains("Details: runtime exception");
        verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    public void testDeleteObjectsWithPrefix_success() {
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
        String bucketName = "my-bucket";
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
}
