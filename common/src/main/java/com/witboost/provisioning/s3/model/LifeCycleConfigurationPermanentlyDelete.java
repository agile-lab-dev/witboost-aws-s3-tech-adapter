package com.witboost.provisioning.s3.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LifeCycleConfigurationPermanentlyDelete {

    @Positive(message = "daysAfterBecomeNonCurrent must be greater than 0")
    private int daysAfterBecomeNonCurrent;

    @Min(value = 1, message = "numberOfVersionsToRetain must be at least 1")
    @Max(value = 100, message = "numberOfVersionsToRetain cannot be greater than 100")
    private int numberOfVersionsToRetain;
}
