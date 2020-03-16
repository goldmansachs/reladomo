/*
 Copyright 2019 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

package com.gs.reladomo.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.fw.common.mithra.test.MithraTestResource;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

public class SchemaProviderTest extends GraphqlTestBase
{
    @Test
    public void findOne() throws Exception
    {
        assertQuery(
                "{ productById ( productId: 1 ) { productId, productCode } }",
                "{\"productById\":{\"productId\":1,\"productCode\":\"AA\"}}");
    }

    @Test
    public void findManyWithDeepFetch() throws Exception
    {
        assertQuery(
                "{ orders (findMany: {orderId: {in: [2,3,4,10]} }) { orderId description items { state } } }",
                "{\"orders\":[{\"orderId\":2,\"description\":\"Second order\",\"items\":[{\"state\":\"InProgress\"},{\"state\":\"InProgress\"},{\"state\":\"InProgress\"}]}," +
                        "{\"orderId\":3,\"description\":\"Third order\",\"items\":[]}," +
                        "{\"orderId\":4,\"description\":\"Fourth order, different user\",\"items\":[]}]}\n");
    }

    @Test
    public void findManyRelationshipTraversal() throws Exception
    {
        assertQuery(
                "{ orderItems (findMany: {order: { orderId: {in: [2,3,4,10]}} }) { id state order { description orderId } } }",
                "{\"orderItems\":[" +
                        "{\"id\":2,\"state\":\"InProgress\",\"order\":{\"description\":\"Second order\",\"orderId\":2}}," +
                        "{\"id\":3,\"state\":\"InProgress\",\"order\":{\"description\":\"Second order\",\"orderId\":2}}," +
                        "{\"id\":4,\"state\":\"InProgress\",\"order\":{\"description\":\"Second order\",\"orderId\":2}}" +
                        "]}");
    }

    @Test
    public void findWithSimpleExpression() throws Exception
    {
        assertQuery(
                "{ orderItems (filter: {EXPR: {plus: [{originalPrice: {}}, {discountPrice: {}}] greaterThan: 13.0} }) { id originalPrice } }",
                "{\"orderItems\":[{\"id\":4,\"originalPrice\":20.5},{\"id\":3,\"originalPrice\":15.5},{\"id\":2,\"originalPrice\":10.5},{\"id\":1,\"originalPrice\":10.5}]}");

        assertQuery(
                "{ orderItems (filter: {EXPR: {plus: [{originalPrice: {}}, {discountPrice: {}}, {VAL: -13.0}] greaterThan: 0.0} }) { id originalPrice } }",
                "{\"orderItems\":[{\"id\":4,\"originalPrice\":20.5},{\"id\":3,\"originalPrice\":15.5},{\"id\":2,\"originalPrice\":10.5},{\"id\":1,\"originalPrice\":10.5}]}");
    }

    @Test
    public void findWithExpression() throws Exception
    {
        assertQuery(
                "{ orderItems (filter: {EXPR: {plus: [{originalPrice: {}}, { minus: [{discountPrice: {}}, {VAL: -13.0}]} ] greaterThan: 0.0} }) { id originalPrice } }",
                "{\"orderItems\":[{\"id\":4,\"originalPrice\":20.5},{\"id\":3,\"originalPrice\":15.5},{\"id\":2,\"originalPrice\":10.5},{\"id\":1,\"originalPrice\":10.5}]}");
    }

    @Test
    public void findWithAbs() throws Exception
    {
        assertQuery(
                "{ orderItems (filter: {EXPR: {plus: [{originalPrice: {}}, { absoluteValue: {discountPrice: {}} } ] greaterThan: 13.0} }) { id originalPrice } }",
                "{\"orderItems\":[{\"id\":4,\"originalPrice\":20.5},{\"id\":3,\"originalPrice\":15.5},{\"id\":2,\"originalPrice\":10.5},{\"id\":1,\"originalPrice\":10.5}]}");
    }

    @Test
    public void metadata() throws Exception
    {
        assertQuery(
                "{ productById ( productId: 1 ) { productId, productCode } }",
                "{\"productById\":{\"productId\":1,\"productCode\":\"AA\"}}");
    }

    @Test
    public void reladomoFinder() throws Exception
    {
        assertQuery("{ orders (findMany: {orderId: {in: [2,3,4,10]} OR: [{state: {startsWith: \"In\"}}] }) { orderId description items { state } } }",
                "{\"orders\":[" +
                        "{\"orderId\":3,\"description\":\"Third order\",\"items\":[]}," +
                        "{\"orderId\":2,\"description\":\"Second order\",\"items\":[{\"state\":\"InProgress\"},{\"state\":\"InProgress\"},{\"state\":\"InProgress\"}]}," +
                        "{\"orderId\":4,\"description\":\"Fourth order, different user\",\"items\":[]}," +
                        "{\"orderId\":1,\"description\":\"First order\",\"items\":[{\"state\":\"InProgress\"}]}]}\n");
    }

    @Test
    public void reladomoFinderAndOr() throws Exception
    {
        // becomes reladomo operation "Order.orderId in [2, 3, 4, 10] & ( ( Order.state startsWith "In" ) | ( Order.state endsWith "Out" ) )"
        assertQuery("{ orders (findMany: {orderId: {in: [2,3,4,10]} AND: [{state: {startsWith: \"In\"} OR: [{state: {endsWith: \"Out\"}}] }] }) { orderId description items { state } } }",
                "{\"orders\":[" +
                        "{\"orderId\":2,\"description\":\"Second order\",\"items\":[{\"state\":\"InProgress\"},{\"state\":\"InProgress\"},{\"state\":\"InProgress\"}]}," +
                        "{\"orderId\":3,\"description\":\"Third order\",\"items\":[]}," +
                        "{\"orderId\":4,\"description\":\"Fourth order, different user\",\"items\":[]}]}\n");
    }

    @Test
    public void polishNotation() throws Exception
    {
        // becomes reladomo operation "Order.orderId in [2, 3, 4, 10] & ( ( Order.state startsWith "In" ) | ( Order.state endsWith "Out" ) )"
        assertQuery("{ orders (filter: {AND: [{orderId: {in: [2,3,4,10]}}, {OR: [{state: {startsWith: \"In\"}}, {state: {endsWith: \"Out\"}} ]} ]} ) { orderId description items { state } } }",
                "{\"orders\":[" +
                        "{\"orderId\":2,\"description\":\"Second order\",\"items\":[{\"state\":\"InProgress\"},{\"state\":\"InProgress\"},{\"state\":\"InProgress\"}]}," +
                        "{\"orderId\":3,\"description\":\"Third order\",\"items\":[]}," +
                        "{\"orderId\":4,\"description\":\"Fourth order, different user\",\"items\":[]}]}\n");
    }

    @Test
    public void orderByAndLimit() throws Exception
    {
        assertQuery(
                "{ orders (findMany: {orderId: {notIn: [4,10]}} order_by: {orderId: desc}  limit: 2) { orderId description} }",
                "{\"orders\":[" +
                        "{\"orderId\":3,\"description\":\"Third order\"}," +
                        "{\"orderId\":2,\"description\":\"Second order\"}," +
                        "{\"orderId\":1,\"description\":\"First order\"}" +
                        "]}");
    }

    @Test
    public void multipleQueries() throws Exception
    {
        assertQuery(
                "{ productById(productId: 4) {  productId  productCode  } orderById(orderId: 3) { orderId description } }",
                "{\"productById\":{\"productId\":4,\"productCode\":\"AB\"}," +
                        "\"orderById\":{\"orderId\":3,\"description\":\"Third order\"}}\n");
    }

    @Test
    public void aggregateSum() throws Exception
    {
        assertQuery(
                "{ testBalanceNoAcmap_aggregate (filter: {productId: {in: [1, 2, 3, 4, 5]} AND: [ {businessDate: {eq: \"2020-04-12T23:59:00+00:00\"}}] }) { accountId quantity { sum } }} \n",
                "{\"testBalanceNoAcmap_aggregate\":[" +
                        "{\"accountId\":\"123605\",\"quantity\":{\"sum\":600.0}}," +
                        "{\"accountId\":\"123604\",\"quantity\":{\"sum\":500.0}}," +
                        "{\"accountId\":\"123601\",\"quantity\":{\"sum\":700.0}}," +
                        "{\"accountId\":\"123603\",\"quantity\":{\"sum\":400.0}}" +
                        "]}\n");
    }

    @Test
    public void aggregateManyTypes() throws Exception
    {
        assertQuery(
                "{ testBalanceNoAcmap_aggregate (filter: {productId: {in: [1, 2, 3, 4, 5]} AND: [{businessDate: {eq: \"2020-04-12T23:59:00+00:00\"}}] }) { accountId quantity { sum count } }} \n",
                "{\"testBalanceNoAcmap_aggregate\":[" +
                        "{\"accountId\":\"123605\",\"quantity\":{\"sum\":600.0,\"count\":1.0}}," +
                        "{\"accountId\":\"123604\",\"quantity\":{\"sum\":500.0,\"count\":1.0}}," +
                        "{\"accountId\":\"123601\",\"quantity\":{\"sum\":700.0,\"count\":2.0}}," +
                        "{\"accountId\":\"123603\",\"quantity\":{\"sum\":400.0,\"count\":1.0}}" +
                        "]}\n");
    }

    @Test
    public void aggregateWithJoin() throws Exception
    {
        assertQuery(
                "{ testBalanceNoAcmap_aggregate (filter: {productId: {in: [1, 2, 3, 4, 5]} AND: [{businessDate: {eq: \"2020-04-12T23:59:00+00:00\"}}] }) { account { code trialId } quantity { sum } }}",
                "{\"testBalanceNoAcmap_aggregate\":[" +
                        "{\"account\":{\"code\":\"123601\",\"trialId\":\"1111\"},\"quantity\":{\"sum\":700.0}}," +
                        "{\"account\":{\"code\":\"123604\",\"trialId\":\"2222\"},\"quantity\":{\"sum\":500.0}}," +
                        "{\"account\":{\"code\":\"123603\",\"trialId\":\"1111\"},\"quantity\":{\"sum\":400.0}}," +
                        "{\"account\":{\"code\":\"123605\",\"trialId\":\"1111\"},\"quantity\":{\"sum\":600.0}}" +
                        "]}\n");
    }

    @Test
    public void implicitMilestoning() throws Exception
    {
        assertQuery(
                "{ testBalanceNoAcmaps (findMany: {productId: {in: [1, 2, 3, 4, 5]} AND: [{businessDate: {eq: \"2020-04-12T23:59:00+00:00\"}}] }) { account { code trialId } quantity businessDateFrom businessDateTo processingDateFrom processingDateTo }}",
                "{\"testBalanceNoAcmaps\":[" +
                        "{\"account\":{\"code\":\"123601\",\"trialId\":\"1111\"},\"quantity\":300.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-01T05:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123604\",\"trialId\":\"2222\"},\"quantity\":500.0,\"businessDateFrom\":\"2020-01-03T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2020-01-02T15:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123603\",\"trialId\":\"1111\"},\"quantity\":400.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-02T15:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123605\",\"trialId\":\"1111\"},\"quantity\":600.0,\"businessDateFrom\":\"2020-01-03T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2020-01-03T03:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123601\",\"trialId\":\"1111\"},\"quantity\":400.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-01T17:00:00Z\",\"processingDateTo\":\"INFINITY\"}" +
                        "]}\n");
    }

    @Test
    public void findWithSourceAttribute() throws Exception
    {
        assertQuery(
                "{ tinyBalances (findMany: {acmapCode: {eq: \"A\"} AND: [{businessDate: {eq: \"2002-11-30T23:59:00+00:00\"}}] }) { balanceId acmapCode businessDateFrom businessDateTo }}",
                "{\"tinyBalances\":[" +
                        "{\"balanceId\":1,\"acmapCode\":\"A\",\"businessDateFrom\":\"2002-11-29T05:00:00Z\",\"businessDateTo\":\"INFINITY\"}," +
                        "{\"balanceId\":3,\"acmapCode\":\"A\",\"businessDateFrom\":\"2002-02-01T05:00:00Z\",\"businessDateTo\":\"INFINITY\"}," +
                        "{\"balanceId\":5,\"acmapCode\":\"A\",\"businessDateFrom\":\"2002-01-27T05:00:00Z\",\"businessDateTo\":\"INFINITY\"}," +
                        "{\"balanceId\":40,\"acmapCode\":\"A\",\"businessDateFrom\":\"2002-11-29T05:00:00Z\",\"businessDateTo\":\"2003-01-01T05:00:00Z\"}]}\n");
    }

    @Test
    public void bitemporalMilestoning() throws Exception
    {
        assertQuery(
                "{ testBalanceNoAcmaps (findMany: {processingDate: {eq: \"2019-10-13T04:04:39+00:00\"} AND: [{businessDate: {eq: \"2019-10-12T23:59:00+00:00\"}}] }) { account { code trialId } quantity businessDateFrom businessDateTo processingDateFrom processingDateTo }}",
                "{\"testBalanceNoAcmaps\":[" +
                        "{\"account\":{\"code\":\"123601\",\"trialId\":\"1111\"},\"quantity\":300.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-01T05:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123603\",\"trialId\":\"1111\"},\"quantity\":400.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-02T15:00:00Z\",\"processingDateTo\":\"INFINITY\"}," +
                        "{\"account\":{\"code\":\"123601\",\"trialId\":\"1111\"},\"quantity\":400.0,\"businessDateFrom\":\"2012-01-02T04:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateFrom\":\"2012-01-01T17:00:00Z\",\"processingDateTo\":\"INFINITY\"}" +
                        "]}\n");
    }

    @Test
    public void tupleIn() throws Exception
    {
        assertQuery(
                "query { products (findMany: {\n" +
                        "  tupleIn: [\n" +
                        "    {productCode: \"AA\" manufacturerId: 1},\n" +
                        "    {productCode: \"AA\" manufacturerId: 2},\n" +
                        "    {productCode: \"AB\" manufacturerId: 3}\n" +
                        "  ]\n" +
                        "})\n" +
                        "  {\n" +
                        "    productId productCode manufacturerId" +
                        "  }\n" +
                        "}",
                "{\"products\":[" +
                        "{\"productId\":1,\"productCode\":\"AA\",\"manufacturerId\":1}," +
                        "{\"productId\":2,\"productCode\":\"AA\",\"manufacturerId\":1}," +
                        "{\"productId\":4,\"productCode\":\"AB\",\"manufacturerId\":3}]}");
    }

    @Test
    public void insertSimple() throws Exception
    {
        assertQuery(
                "mutation { product_insert(product: {productId: 73 productDescription: \"TPaper\" productCode: \"TP\"}) { productId productDescription productCode }}",
                "{\"product_insert\":{\"productId\":73,\"productDescription\":\"TPaper\",\"productCode\":\"TP\"}}");
        assertQuery(
                "{ products (findMany: {productCode: {endsWith: \"TP\"}}) { productId productDescription productCode }}",
                "{\"products\":[{\"productId\":73,\"productDescription\":\"TPaper\",\"productCode\":\"TP\"}]}");
    }

    @Test
    public void insertBitemporal() throws Exception
    {
        assertQuery(
                "mutation { testBalanceNoAcmap_insert(businessDate: \"2020-04-12\"" +
                        " testBalanceNoAcmap: {productId: 73 accountId: \"324\" positionType: 3}) { productId accountId positionType  businessDateFrom businessDateTo processingDateTo }}",
                "{\"testBalanceNoAcmap_insert\":{\"productId\":73,\"accountId\":\"324\",\"positionType\":3,\"businessDateFrom\":\"2020-04-13T03:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateTo\":\"INFINITY\"}}");
        assertQuery(
                "{ testBalanceNoAcmaps (findMany: {accountId: {eq: \"324\"} AND: [{businessDate: {eq: \"2020-04-12\"}}] }) { productId accountId positionType  businessDateFrom businessDateTo processingDateTo }}",
                "{\"testBalanceNoAcmaps\":[{\"productId\":73,\"accountId\":\"324\",\"positionType\":3,\"businessDateFrom\":\"2020-04-13T03:59:00Z\",\"businessDateTo\":\"INFINITY\",\"processingDateTo\":\"INFINITY\"}]}");
    }

    @Test
    public void insertBitemporalSourceAttribute() throws Exception
    {
        assertQuery(
                "mutation { tinyBalance_insert(businessDate: \"2020-04-12\"" +
                        " tinyBalance: {balanceId: 157 acmapCode: \"A\" quantity: 3123.21}) { balanceId acmapCode quantity }}",
                "{\"tinyBalance_insert\":{\"balanceId\":157,\"acmapCode\":\"A\",\"quantity\":3123.21}}");
        assertQuery(
                "{ tinyBalances (findMany: {AND: [{balanceId: {eq: 157}}, {acmapCode: {eq: \"A\"}}, {businessDate: {eq: \"2020-04-12\"}}] }) { balanceId acmapCode quantity }}",
                "{\"tinyBalances\":[{\"balanceId\":157,\"acmapCode\":\"A\",\"quantity\":3123.21}]}");
    }

    private void assertQuery(String request, String expectedResult) throws Exception
    {
        ExecutionResult executionResult = this.graphQL.execute(request);

        List<GraphQLError> errors = executionResult.getErrors();

        if (!errors.isEmpty())
        {
            Assert.fail(GraphQLErrorKit.toString(errors));
        }
        ObjectMapper mapper = new ObjectMapper();
        String result = mapper.writeValueAsString(executionResult.getData());
        String message = "========= actual: =========\n" + result + "\n=======================";
        try
        {
            JSONAssert.assertEquals(message,
                    expectedResult, result, false);
        } catch (Exception e)
        {
            throw new RuntimeException(message, e);
        }
    }


    private GraphQL graphQL;
    private MithraTestResource mithraTestResource;

    @Before
    public void init()
    {
        this.mithraTestResource = this.initReladomo("ReladomoRuntimeConfig.xml", "test-data.txt");
        GraphQLSchema graphQLSchema = SchemaProvider.forResource("test-schema.graphqls");
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    @After
    public void tearDown()
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
        this.graphQL = null;
    }

}