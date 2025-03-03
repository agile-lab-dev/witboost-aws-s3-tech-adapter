package com.witboost.provisioning.s3.client;

import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.s3.model.BucketTag;
import io.vavr.control.Either;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

@Service
public class KmsManager {

    private static final Logger logger = LoggerFactory.getLogger(KmsManager.class);

    /**
     * Creates a new symmetric encryption key in AWS KMS.
     *
     * <p>This method generates a new symmetric key for encryption and decryption purposes.
     * The key policy is loaded from a predefined JSON file or a custom path if specified.
     * If tags are provided, they are attached to the key.</p>
     *
     * @param kmsClient  the AWS KMS client used for key creation
     * @param accountId  the AWS account ID where the key will be created
     * @param keyDesc    a description of the key to be created
     * @param tags       a list of {@link BucketTag} to associate with the key (optional)
     * @return an {@link Either} containing the key ID if successful, or a {@link FailedOperation} in case of failure
     */
    public Either<FailedOperation, String> createKey(
            KmsClient kmsClient, String accountId, String keyDesc, List<BucketTag> tags) {

        try {
            logger.info("Starting creation of a new KMS key");

            String kmsPolicy;
            if (System.getProperty("kms.policy.path") != null) {
                String policyPath = System.getProperty("kms.policy.path");
                logger.info("Using custom KMS policy path: {}", policyPath);
                kmsPolicy = new String(Files.readAllBytes(Paths.get(policyPath)));

            } else {
                InputStream inputStream = getClass().getClassLoader().getResourceAsStream("kms-policy.json");
                logger.info("Using default KMS policy path");
                kmsPolicy = new String(Objects.requireNonNull(inputStream).readAllBytes(), StandardCharsets.UTF_8);
            }

            logger.debug("Original KMS policy: {}", kmsPolicy);
            String updatedPolicy = kmsPolicy.replace("{accountID}", accountId);
            logger.debug("Updated KMS policy: {}", updatedPolicy);

            CreateKeyRequest.Builder keyRequestBuilder = CreateKeyRequest.builder()
                    .description(keyDesc)
                    .keySpec(KeySpec.SYMMETRIC_DEFAULT)
                    .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
                    .policy(updatedPolicy);

            if (!(tags == null || tags.isEmpty())) {
                List<Tag> awsTags = new ArrayList<>();
                tags.forEach(t -> awsTags.add(
                        Tag.builder().tagKey(t.getKey()).tagValue(t.getValue()).build()));

                keyRequestBuilder.tags(awsTags);

                logger.debug("Tags added to the KMS key: {}", tags);
            }

            CreateKeyRequest keyRequest = keyRequestBuilder.build();
            logger.debug("Creating KMS key with description: {}", keyDesc);

            CreateKeyResponse response = kmsClient.createKey(keyRequest);
            String keyId = response.keyMetadata().keyId();
            logger.info("Successfully created KMS key with ID: {}", keyId);
            return Either.right(keyId);

        } catch (IOException e) {
            String error = "Error reading KMS policy file: " + e.getMessage();
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        } catch (Exception e) {
            String error = "Unexpected error during KMS key creation: " + e.getMessage();
            logger.error(error, e);
            return Either.left(new FailedOperation(error, List.of(new Problem(error, e))));
        }
    }
}
