package com.witboost.provisioning.s3.service.provision;

import com.witboost.provisioning.framework.service.ProvisionService;
import com.witboost.provisioning.model.Specific;
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
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Component
public class StorageAreaProvisionService implements ProvisionService {

    private final Logger logger = LoggerFactory.getLogger(StorageAreaProvisionService.class);

    private final Function<Region, S3Client> s3ClientProvider;
    private final BucketManager bucketManager;

    public StorageAreaProvisionService(Function<Region, S3Client> s3ClientProvider, BucketManager bucketManager) {
        this.s3ClientProvider = s3ClientProvider;
        this.bucketManager = bucketManager;
    }

    @Override
    public Either<FailedOperation, ProvisionInfo> provision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {

        var component = getComponent(operationRequest);
        if (component.isLeft()) return Either.left(component.getLeft());

        Either<FailedOperation, S3Specific> s3Specific = getS3Specific(component.get());
        if (s3Specific.isLeft()) return Either.left(s3Specific.getLeft());

        Region region = Region.of(s3Specific.get().getRegion());
        // Get the S3Client from the provider (cached or new)
        S3Client s3Client = s3ClientProvider.apply(region);

        String bucketName = S3Utils.computeBucketName(operationRequest.getDataProduct(), component.get());

        Either<FailedOperation, Void> bucketCreationResult = bucketManager.createBucket(
                s3Client, bucketName, region.id(), s3Specific.get().getMultipleVersion());

        if (bucketCreationResult.isLeft()) return Either.left(bucketCreationResult.getLeft());

        String[] componentIdParts = component.get().getId().split(":");
        String folderName = componentIdParts[componentIdParts.length - 1];
        var location = new StringBuilder("s3://").append(bucketName).append("/").append(folderName);

        Either<FailedOperation, Void> folderCreationResult =
                bucketManager.createFolder(s3Client, bucketName, folderName);

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
                        "value", folderName),
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

    @Override
    public Either<FailedOperation, ProvisionInfo> unprovision(
            ProvisionOperationRequest<?, ? extends Specific> operationRequest) {

        var component = getComponent(operationRequest);
        if (component.isLeft()) return Either.left(component.getLeft());

        Either<FailedOperation, S3Specific> s3Specific = getS3Specific(component.get());
        if (s3Specific.isLeft()) return Either.left(s3Specific.getLeft());

        Region region = Region.of(s3Specific.get().getRegion());

        // Get the S3Client from the provider (cached or new)
        S3Client s3Client = s3ClientProvider.apply(region);

        String bucketName = S3Utils.computeBucketName(operationRequest.getDataProduct(), component.get());

        Either<FailedOperation, Boolean> bucketExists = bucketManager.doesBucketExist(s3Client, bucketName);
        if (bucketExists.isLeft()) return Either.left(bucketExists.getLeft());

        String[] componentIdParts = component.get().getId().split(":");
        String folderName = componentIdParts[componentIdParts.length - 1];

        if (bucketExists.get()) {
            var folderDeleted = bucketManager.deleteObjectsWithPrefix(s3Client, bucketName, folderName);

            if (folderDeleted.isLeft()) return Either.left(folderDeleted.getLeft());
        }

        var info = Map.of(
                "result",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Operation result",
                        "value",
                        String.format("Folder: %s successfully deleted. Bucket: %s", folderName, bucketName)));

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
