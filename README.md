# DynamoDB Adaptive Capacity Demo
This demo accompanies the AWS blog post: [How Amazon DynamoDB adaptive capacity accommodates uneven data access patterns](https://aws.amazon.com/blogs/database/how-amazon-dynamodb-adaptive-capacity-accommodates-uneven-data-access-patterns-or-why-what-you-know-about-dynamodb-might-be-outdated/) 

## Prerequisites
To run the Census demo, you must have the following:
* Java 1.8 or later
* Maven 3 or later
* an AWS account

Ensure the execution environment has been configured with AWS permissions as described for the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html)

## Running the Demo
Launch the demo using:
```
mvn package exec:java@census-demo
```



## Clean-up
The demo will drive traffic to a table named "CensusDemo" in the region chosen by the [default client](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-region-selection.html#automatically-determine-the-aws-region-from-the-environment).  Feel free to delete the table after observing the associated CloudWatch metrics.
