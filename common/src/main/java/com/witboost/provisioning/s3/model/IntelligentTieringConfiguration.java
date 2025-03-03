package com.witboost.provisioning.s3.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IntelligentTieringConfiguration {

    private Boolean enabled;
    private Boolean archiveAccessTierEnabled;
    private Boolean deepArchiveAccessTierEnabled;

    @Min(value = 90, message = "archiveAccessTierDays must be at least 90")
    @Max(value = 730, message = "archiveAccessTierDays cannot be greater than 730")
    private int archiveAccessTierDays;

    @Min(value = 180, message = "deepArchiveAccessTierDays must be at least 180")
    @Max(value = 730, message = "deepArchiveAccessTierDays cannot be greater than 730")
    private int deepArchiveAccessTierDays;
}
