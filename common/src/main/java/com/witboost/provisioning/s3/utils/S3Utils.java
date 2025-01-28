package com.witboost.provisioning.s3.utils;

import com.witboost.provisioning.model.Component;
import com.witboost.provisioning.model.DataProduct;
import java.util.StringJoiner;

public class S3Utils {

    /**
     * Computes the bucket name from the descriptor
     * @param dp is the data product descriptor
     * @param cp is the component descriptor
     * @return the name of the s3 bucket
     */
    public static String computeBucketName(DataProduct dp, Component cp) {

        String bucketNameWithoutHash = new StringJoiner("-")
                .add(dp.getDomain())
                .add(dp.getName())
                .add(dp.getEnvironment())
                .toString()
                .replaceAll("\\s+", "")
                .toLowerCase();

        int hash = bucketNameWithoutHash.hashCode();
        if (bucketNameWithoutHash.length() > 58)
            return bucketNameWithoutHash.substring(0, 58) + String.valueOf(hash).substring(0, 5);

        return bucketNameWithoutHash + String.valueOf(hash).substring(0, 5);
    }
}
