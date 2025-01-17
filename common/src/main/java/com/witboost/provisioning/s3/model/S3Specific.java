package com.witboost.provisioning.s3.model;

import com.witboost.provisioning.model.Specific;
import jakarta.validation.constraints.NotBlank;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class S3Specific extends Specific {

    @NotBlank
    private String region;

    private Boolean multipleVersion;
    private String accountId;

    public String getAccountId() {
        return accountId;
    }

    public String getRegion() {
        return region;
    }

    public Boolean getMultipleVersion() {
        return multipleVersion;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public void setMultipleVersion(Boolean multipleVersion) {
        this.multipleVersion = multipleVersion;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
