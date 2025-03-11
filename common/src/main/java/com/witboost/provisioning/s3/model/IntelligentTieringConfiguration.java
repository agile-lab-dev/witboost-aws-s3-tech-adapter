package com.witboost.provisioning.s3.model;

import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class IntelligentTieringConfiguration {

    private Boolean archiveAccessTierEnabled;
    private Boolean deepArchiveAccessTierEnabled;

    private int archiveAccessTierDays;

    private int deepArchiveAccessTierDays;

    @AssertTrue(
            message =
                    "archiveAccessTierDays must be be at least 90 and cannot be greater than 730 if archiveAccessTierEnabled is true")
    public boolean isArchiveAccessTierDaysValid() {
        return !Boolean.TRUE.equals(archiveAccessTierEnabled)
                || (archiveAccessTierDays >= 90 && archiveAccessTierDays <= 730);
    }

    @AssertTrue(
            message =
                    "deepArchiveAccessTierDays must be must be at least 180 and cannot be greater than 730 if deepArchiveAccessTierEnabled is true")
    public boolean isDeepArchiveAccessTierDaysValid() {
        return !Boolean.TRUE.equals(deepArchiveAccessTierEnabled)
                || (deepArchiveAccessTierDays >= 180 && deepArchiveAccessTierDays <= 730);
    }
}
