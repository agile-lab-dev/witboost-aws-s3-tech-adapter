package com.witboost.provisioning.s3.model;

import com.witboost.provisioning.model.Specific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

@NoArgsConstructor
@Getter
@Setter
public class S3Specific extends Specific {

    @NotBlank
    private String region;

    private ServerSideEncryption serverSideEncryption;

    private Boolean multipleVersion;

    @Valid
    private LifeCycleConfiguration lifeCycleConfiguration;

    @Valid
    private IntelligentTieringConfiguration intelligentTieringConfiguration;

    private List<@Valid BucketTag> bucketTags;
}
