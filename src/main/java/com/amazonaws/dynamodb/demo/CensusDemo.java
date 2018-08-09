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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CensusDemo {

    private static final int NUMBER_OF_PARTITIONS = 4;
    private static final long PROVISIONED_WRITES = 100;
    private static final long TARGET_WRITE_RATE = 55;
    private static final long PROVISIONED_READS = 100;
    private static final String TABLE_NAME = "CensusDemo";
    private static final String PROVINCE_KEY = "Province";
    private static final String ID_KEY = "ResponseId";

    private final DynamoDB dynamodb;
    private Table table;
    private final RateLimiter writeLimiter = RateLimiter.create(TARGET_WRITE_RATE);
    private final ThreadPoolExecutor pool;
    private Map<Province, AtomicInteger> success = new ConcurrentHashMap<>();
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    public static void main(String[] args) throws Exception {
        CensusDemo example = new CensusDemo();
        example.createTable();
        example.startMonitor();
        example.simulateTraffic(4, TimeUnit.HOURS);
    }

    private CensusDemo() {
        ClientConfiguration clientConfig = new ClientConfiguration().withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(2));
        this.dynamodb = new DynamoDB(AmazonDynamoDBClientBuilder.standard().withClientConfiguration(clientConfig).build());
        this.table = dynamodb.getTable(TABLE_NAME);
        this.pool = new ThreadPoolExecutor(1, 100, 0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(100), new ThreadPoolExecutor.AbortPolicy());
    }

    private void createTable() throws InterruptedException {
        CreateTableRequest partialRequest = new CreateTableRequest();
        partialRequest.setTableName(table.getTableName());
        partialRequest.setKeySchema(Arrays.asList(
                new KeySchemaElement(PROVINCE_KEY, KeyType.HASH),
                new KeySchemaElement(ID_KEY, KeyType.RANGE)));
        partialRequest.setAttributeDefinitions(Arrays.asList(
                new AttributeDefinition(PROVINCE_KEY, ScalarAttributeType.S),
                new AttributeDefinition(ID_KEY, ScalarAttributeType.S)));
        TableUtil.createMultiPartitionTable(dynamodb, partialRequest, NUMBER_OF_PARTITIONS, PROVISIONED_READS, PROVISIONED_WRITES);
    }

    private void startMonitor() {
        stopwatch.start();
        printHeader();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::printMetrics, 10, 10, TimeUnit.SECONDS);
    }

    private void printHeader() {
        System.out.print("Time");
        for (Province province : Province.values()) {
            System.out.print("," + province.name());
        }
        System.out.println();
    }

    private void printMetrics() {
        System.out.print(stopwatch.elapsed(TimeUnit.SECONDS));
        for (Province province : Province.values()) {
            System.out.print("," + success.getOrDefault(province, new AtomicInteger()));
        }
        System.out.println();
        success.clear();
    }

    private void simulateTraffic(long duration, TimeUnit timeUnit) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        while (stopwatch.elapsed(timeUnit) < duration) {
            Province province = Province.getRandom();
            String id = UUID.randomUUID().toString();
            writeLimiter.acquire();
            putItem(province, id);
        }
    }

    private void putItem(Province province, String id) {
        try {
            pool.submit(() -> {
                Item item = new Item()
                        .withString(PROVINCE_KEY, province.name())
                        .withString(ID_KEY, id);
                try {
                    table.putItem(item);
                    success.computeIfAbsent(province, a -> new AtomicInteger()).incrementAndGet();
                } catch (ProvisionedThroughputExceededException e) {
                    // request throttled
                } catch (Exception e) {
                    // All other exceptions
                }
            });
        } catch (RejectedExecutionException e) {
            // queue full
        }
    }
}


