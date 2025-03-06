package com.witboost.provisioning.s3.utils;

import com.witboost.provisioning.model.Component;
import com.witboost.provisioning.model.DataProduct;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class S3Utils {

    /**
     * Computes a bucket name based on the domain, name, and environment of a DataProduct.
     * The bucket name is truncated to 58 characters if it's too long, and a hash (SHA-256) is appended.
     *
     * @param dp The DataProduct object containing the domain, name, and environment.
     * @param component The Component object containing the component name.
     * @return The computed bucket name, possibly truncated and appended with a hash.
     */
    public static String computeBucketName(DataProduct dp, Component component) {

        String componentName = component.getId().split(":")[6];
        String bucketNameWithoutHash =
                dp.getDomain() + "-" + dp.getName() + "-" + componentName + "-" + dp.getEnvironment();
        bucketNameWithoutHash = bucketNameWithoutHash.replaceAll("\\s+", "").toLowerCase();

        String hash = sha256(bucketNameWithoutHash);

        if (bucketNameWithoutHash.length() > 58) {
            return bucketNameWithoutHash.substring(0, 58) + hash.substring(0, 5);
        }

        return bucketNameWithoutHash + hash.substring(0, 5);
    }

    /**
     * Computes the SHA-256 hash of the input string and returns the first 5 characters of the hash.
     *
     * @param input The input string to hash.
     * @return The first 5 characters of the SHA-256 hash.
     */
    protected static String sha256(String input) {
        try {
            // Create a MessageDigest instance for SHA-256
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes());

            // Convert the byte array into a hexadecimal string
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                hexString.append(String.format("%02x", b)); // Format each byte as a 2-character hex string
            }

            // Return the full hexadecimal hash string
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }
}
