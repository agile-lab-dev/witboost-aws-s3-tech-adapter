# AWS permissions

Below is an example of an IAM policy including the permissions to be assigned to the role assumed by the tech adapter in order for it to function properly.

For simplicity, resources are indicated with `"*"`. Restrict the list according to the principle of least privilege.

```json
{
  "Statement": [
    {
      "Action": [
        "s3:ListBucket",
        "s3:CreateBucket",
        "s3:GetBucketLocation",
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ],
  "Version": "2012-10-17"
}
```
### Including AWS KMS Permissions

If your bucket encryption method for data is AWS Key Management Service (KMS), you need to include the following statement to the IAM policy (or create a new one with these permissions) to allow the Tech Adapter to manage encryption keys and ensure it can operate without issues:

```json
{
  "Action": [
    "kms:CreateKey",
    "kms:CreateAlias",
    "kms:TagResource",
    "kms:PutKeyPolicy",
    "kms:GenerateDataKey"
  ],
  "Effect": "Allow",
  "Resource": [
    "*"
  ]
}
```
