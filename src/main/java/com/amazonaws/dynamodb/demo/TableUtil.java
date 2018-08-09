/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.dynamodb.demo;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

public class TableUtil {

    // As of the time of publication, DynamoDB uses the formula:
    // initial_partitions = (RCU / 3000) + (WCU / 1000) (rounded-up)
    // Provisioning 750 RCU and 750 WCU for each partition will result in the target number of partitions
    public static void createMultiPartitionTable(
            DynamoDB dynamodb,
            CreateTableRequest partialRequest,
            int partitions,
            long readRate,
            long writeRate) throws InterruptedException {

        System.out.println("Getting Table " + partialRequest.getTableName());
        Table table = dynamodb.getTable(partialRequest.getTableName());
        partialRequest.setProvisionedThroughput(new ProvisionedThroughput(750L * partitions, 750L * partitions));
        try {
            table.describe();
        } catch (ResourceNotFoundException e) {
            System.out.println("Creating Table with " + partitions + " partitions");
            table = dynamodb.createTable(partialRequest);
        }
        System.out.println("Awaiting Active");
        table.waitForActive();
        // Dial Down
        if (table.getDescription().getProvisionedThroughput().getWriteCapacityUnits() != writeRate) {
            System.out.println("Dialing down to " + writeRate + " WCUs");
            table.updateTable(new ProvisionedThroughput(readRate, writeRate));
            System.out.println("Awaiting Active");
            table.waitForActive();
        }
        System.out.println("Table " + table.getTableName() + " is Active");
    }

}
