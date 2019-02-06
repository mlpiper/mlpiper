package com.parallelmachines.reflex.common.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class StringOpsTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void passwordMask() throws Exception {
        String json = "{\n" +
                "    \"name\" : \"Pi Calculator\",\n" +
                "    \"engineType\": \"PySpark\",\n" +
                "    \"pipe\" : [\n" +
                "        {\n" +
                "            \"name\": \"Comp1\",\n" +
                "            \"parents\": [],\n" +
                "            \"arguments\": {\n" +
                "                \"num_samples\": 33333\n" +
                "            }\n" +
                "\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp2\",\n" +
                "            \"id\": 2,\n" +
                "            \"parents\": [],\n" +
                "            \"arguments\": {\n" +
                "                \"num_samples\": 555555,\n" +
                "                \"password\": \"123qwe\",\n" +
                "                \"rate\": 555555\n" +
                "            }\n" +
                "\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp3\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"Password\": \"123qwe\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp4\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df1\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"key1\": 10,\n" +
                "                \"paSswoRd\": \"123qwe\"\n" +
                "            }\n" +
                "        } ,\n" +
                "        {\n" +
                "            \"name\": \"Comp5\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df1\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"sqlPassword\": \"123qwe\",\n" +
                "                \"key1\": 10\n" +
                "            }\n" +
                "        } ,\n" +
                "        {\n" +
                "            \"name\": \"Comp6\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df2\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"key1\": 10, \"password\": \"123\"qwe\" , \"key2\": 10, \"sqlPassword\": \"123qwe\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        final String expectedJson = "{\n" +
                "    \"name\" : \"Pi Calculator\",\n" +
                "    \"engineType\": \"PySpark\",\n" +
                "    \"pipe\" : [\n" +
                "        {\n" +
                "            \"name\": \"Comp1\",\n" +
                "            \"parents\": [],\n" +
                "            \"arguments\": {\n" +
                "                \"num_samples\": 33333\n" +
                "            }\n" +
                "\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp2\",\n" +
                "            \"id\": 2,\n" +
                "            \"parents\": [],\n" +
                "            \"arguments\": {\n" +
                "                \"num_samples\": 555555,\n" +
                "                \"password\": \"*****\",\n" +
                "                \"rate\": 555555\n" +
                "            }\n" +
                "\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp3\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"Password\": \"*****\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Comp4\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df1\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"key1\": 10,\n" +
                "                \"paSswoRd\": \"*****\"\n" +
                "            }\n" +
                "        } ,\n" +
                "        {\n" +
                "            \"name\": \"Comp5\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df1\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"sqlPassword\": \"*****\",\n" +
                "                \"key1\": 10\n" +
                "            }\n" +
                "        } ,\n" +
                "        {\n" +
                "            \"name\": \"Comp6\",\n" +
                "            \"id\": 3,\n" +
                "            \"type\": \"pi_calc_df2\",\n" +
                "            \"parents\": [\n" +
                "                {\"parent\": 1, \"output\": 0},\n" +
                "                {\"parent\": 2, \"output\": 0}\n" +
                "            ],\n" +
                "            \"arguments\" : {\n" +
                "                \"key1\": 10, \"password\": \"*****\" , \"key2\": 10, \"sqlPassword\": \"*****\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        Assert.assertEquals(expectedJson, StringOps.maskPasswords(json));
    }

    @Test
    public void passwordMaskKeywords() throws Exception {
        String json = "{ \"arguments\": { \"sql-password\": \"123qwe\", \"sqlPassword\": \"456ert;~!\", " +
                "\"Password\": \"2ws\"2d\", \"password\": \"Yp'T$EeB7&8_bt8Z\" }";
        String expectedJson = "{ \"arguments\": { \"sql-password\": \"*****\", \"sqlPassword\": \"*****\", " +
                "\"Password\": \"*****\", \"password\": \"*****\" }";

        Assert.assertEquals(expectedJson, StringOps.maskPasswords(json));
    }
}
