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

import java.util.Random;

enum Province {

    // Source: https://en.wikipedia.org/wiki/List_of_Canadian_provinces_and_territories_by_population_growth_rate
    ON("Ontario", 13_448_494),
    QC("Quebec", 8_164_361),
    BC("British Columbia", 4_648_055),
    AB("Alberta", 4_067_175),
    MB("Manitoba", 1_278_365),
    SK("Saskatchewan", 1_098_352),
    NS("Nova Scotia", 923_598),
    NB("New Brunswick", 747_101),
    NL("Newfoundland and Labrador", 519_716),
    PE("Prince Edward Island", 142_907),
    NT("Northwest Territories", 41_786),
    NU("Nunavut", 35_944),
    YT("Yukon", 35_874);

    static final int TOTAL_POPULATION = 35_151_728;
    static final Random random = new Random();

    int population;
    String name;

    Province(String name, int population) {
        this.name = name;
        this.population = population;
    }

    static Province getRandom() {
        int count = random.nextInt(Province.TOTAL_POPULATION);
        for (Province province : Province.values()) {
            count -= province.population;
            if (count <= 0) {
                return province;
            }
        }
        return ON;
    }

}