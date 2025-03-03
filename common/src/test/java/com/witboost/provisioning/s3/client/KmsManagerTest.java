package com.witboost.provisioning.s3.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.s3.model.BucketTag;
import io.vavr.control.Either;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

class KmsManagerTest {

    @Mock
    private KmsClient kmsClient;

    @InjectMocks
    private KmsManager kmsManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateKey_Success() {
        String keyDesc = "Test Key Description";
        BucketTag tag = new BucketTag();
        tag.setKey("tagKey");
        tag.setValue("tagValue");
        List<BucketTag> tags = List.of(tag);

        CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                .build();

        when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

        Either<FailedOperation, String> result = kmsManager.createKey(kmsClient, "accountId", keyDesc, tags);

        assertTrue(result.isRight());
        assertEquals("testKeyId", result.get());
        verify(kmsClient).createKey(any(CreateKeyRequest.class));
    }

    @Test
    void testCreateKey_Failure() {
        String keyDesc = "Test Key Description";
        BucketTag tag = new BucketTag();
        tag.setKey("tagKey");
        tag.setValue("tagValue");
        List<BucketTag> tags = List.of(tag);

        when(kmsClient.createKey(any(CreateKeyRequest.class))).thenThrow(new RuntimeException("KMS error"));

        Either<FailedOperation, String> result = kmsManager.createKey(kmsClient, "accountId", keyDesc, tags);
        assertTrue(result.isLeft());
        assertTrue(result.getLeft().message().contains("KMS error"));
        verify(kmsClient).createKey(any(CreateKeyRequest.class));
    }

    @Test
    void testCreateKey_UsingCustomPolicyPath() {
        String keyDesc = "Test Key Description";
        String customPolicyPath = "/custom/path/kms-policy.json";

        System.setProperty("kms.policy.path", customPolicyPath);

        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles
                    .when(() -> Files.readAllBytes(Paths.get(customPolicyPath)))
                    .thenReturn("{accountID: testAccount}".getBytes());

            CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                    .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                    .build();

            when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

            Either<FailedOperation, String> result = kmsManager.createKey(kmsClient, "accountId", keyDesc, List.of());
            assertTrue(result.isRight());
            assertEquals("testKeyId", result.get());
            assertEquals(customPolicyPath, System.getProperty("kms.policy.path"));
        }

        System.clearProperty("kms.policy.path");
    }

    @Test
    void testCreateKey_UsingCustomPolicyPath_IOException() {
        String keyDesc = "Test Key Description";
        String customPolicyPath = "/custom/path/kms-policy.json";

        System.setProperty("kms.policy.path", customPolicyPath);

        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles
                    .when(() -> Files.readAllBytes(Paths.get(customPolicyPath)))
                    .thenThrow(new IOException("IO Exception"));

            CreateKeyResponse createKeyResponse = CreateKeyResponse.builder()
                    .keyMetadata(KeyMetadata.builder().keyId("testKeyId").build())
                    .build();

            when(kmsClient.createKey(any(CreateKeyRequest.class))).thenReturn(createKeyResponse);

            Either<FailedOperation, String> result = kmsManager.createKey(kmsClient, "accountId", keyDesc, List.of());
            assertTrue(result.isLeft());
            assertTrue(result.getLeft().message().contains("IO Exception"));
        }

        System.clearProperty("kms.policy.path");
    }
}
