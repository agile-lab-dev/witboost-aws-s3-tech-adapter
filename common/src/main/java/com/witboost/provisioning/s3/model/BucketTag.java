package com.witboost.provisioning.s3.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BucketTag {

    @NotBlank
    private String key;

    @NotBlank
    private String value;
}
