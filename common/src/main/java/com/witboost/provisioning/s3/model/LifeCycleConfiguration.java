package com.witboost.provisioning.s3.model;

import jakarta.validation.Valid;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LifeCycleConfiguration {

    @Valid
    private LifeCycleConfigurationPermanentlyDelete permanentlyDelete;
}
