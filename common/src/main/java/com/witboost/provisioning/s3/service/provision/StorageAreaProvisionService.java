package com.witboost.provisioning.s3.service.provision;

import com.witboost.provisioning.framework.service.ProvisionService;
import com.witboost.provisioning.model.Specific;
import com.witboost.provisioning.model.StorageArea;
import com.witboost.provisioning.model.common.FailedOperation;
import com.witboost.provisioning.model.common.Problem;
import com.witboost.provisioning.model.request.ProvisionOperationRequest;
import com.witboost.provisioning.model.status.ProvisionInfo;
import com.witboost.provisioning.s3.client.BucketManager;
import com.witboost.provisioning.s3.model.S3Specific;
import com.witboost.provisioning.s3.utils.S3Utils;
import io.vavr.control.Either;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

@Service
public class StorageAreaProvisionService implements ProvisionService {

    private final Logger logger = LoggerFactory.getLogger(StorageAreaProvisionService.class);

    private final Function<Region, S3Client> s3ClientProvider;
    private final Function<Region, KmsClient> kmsClientProvider;

    private final BucketManager bucketManager;
    private final StsClient stsClient;

    public StorageAreaProvisionService(
            Function<Region, S3Client> s3ClientProvider,
            Function<Region, KmsClient> kmsClientProvider,
            StsClient stsClient,
            BucketManager bucketManager) {
        this.s3ClientProvider = s3ClientProvider;
        this.kmsClientProvider = kmsClientProvider;
        this.bucketManager = bucketManager;
        this.stsClient = stsClient;
    }

    @Override
    public Either<FailedOperation, ProvisionInfo> provision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {

        var component = getComponent(operationRequest);
        if (component.isLeft()) return Either.left(component.getLeft());

        if (!(component.get() instanceof StorageArea<?> storageArea)) {
            String error = String.format(
                    "Invalid component type. %s is not a valid Storage Area",
                    component.get().getName());
            logger.error(error);
            return Either.left(new FailedOperation(error, List.of(new Problem(error))));
        } else {

            Either<FailedOperation, S3Specific> s3SpecificEither = getS3Specific(component.get());
            if (s3SpecificEither.isLeft()) return Either.left(s3SpecificEither.getLeft());

            S3Specific s3Specific = s3SpecificEither.get();
            Region region = Region.of(s3Specific.getRegion());

            // Get the S3Client from the provider (cached or new)
            S3Client s3Client = s3ClientProvider.apply(region);
            KmsClient kmsClient = kmsClientProvider.apply(region);

            String bucketName = S3Utils.computeBucketName(operationRequest.getDataProduct());

            Either<FailedOperation, Void> bucketCreationResult = bucketManager.createOrUpdateBucket(
                    s3Client,
                    kmsClient,
                    bucketName,
                    s3Specific,
                    stsClient.getCallerIdentity().account());

            if (bucketCreationResult.isLeft()) return Either.left(bucketCreationResult.getLeft());

            String[] componentIdParts = component.get().getId().split(":");
            String dpVersion = componentIdParts[componentIdParts.length - 2];
            String folderPath = "v" + dpVersion;
            String location = String.format("s3://%s/%s", bucketName, folderPath);

            Either<FailedOperation, Void> folderCreationResult =
                    bucketManager.createFolder(s3Client, bucketName, folderPath);

            if (folderCreationResult.isLeft()) return Either.left(folderCreationResult.getLeft());

            var info = Map.of(
                    "bucket",
                    Map.of(
                            "type", "string",
                            "label", "Bucket name",
                            "value", bucketName),
                    "folder",
                    Map.of(
                            "type", "string",
                            "label", "Folder name",
                            "value", folderPath),
                    "location",
                    Map.of(
                            "type", "string",
                            "label", "Location",
                            "value", location));

            ProvisionInfo provisionInfo = ProvisionInfo.builder()
                    .privateInfo(Optional.of(info))
                    .publicInfo(Optional.of(info))
                    .build();

            logger.info(String.format(
                    "Provisioning of %s completed successfully", component.get().getName()));
            return Either.right(provisionInfo);
        }
    }

    @Override
    public Either<FailedOperation, ProvisionInfo> unprovision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {

        var component = getComponent(operationRequest);
        if (component.isLeft()) return Either.left(component.getLeft());

        var info = Map.of(
                "result",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Operation result",
                        "value",
                        String.format(
                                "Unprovisioning of %s completed successfully",
                                component.get().getName())));

        ProvisionInfo provisionInfo = ProvisionInfo.builder()
                .privateInfo(Optional.of(info))
                .publicInfo(Optional.of(info))
                .build();

        logger.info(String.format(
                "Unprovisioning of %s completed successfully", component.get().getName()));

        return Either.right(provisionInfo);
    }

    private Either<FailedOperation, S3Specific> getS3Specific(
            com.witboost.provisioning.model.Component<? extends Specific> component) {

        var componentSpecific = component.getSpecific();

        if (componentSpecific instanceof @Valid S3Specific) return Either.right((S3Specific) componentSpecific);

        String error = String.format("Invalid Specific type of %s. Expected S3Specific.", component.getName());
        logger.error(error);
        return Either.left(new FailedOperation(error, List.of(new Problem(error))));
    }

    private Either<FailedOperation, com.witboost.provisioning.model.Component<? extends Specific>> getComponent(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {

        var component = operationRequest.getComponent();
        if (component.isEmpty()) {
            String error =
                    String.format("Invalid operation request: Component is missing. Request: %s", operationRequest);
            logger.error(error);
            return Either.left(new FailedOperation(error, List.of(new Problem(error))));
        }

        return Either.right(component.get());
    }
}
