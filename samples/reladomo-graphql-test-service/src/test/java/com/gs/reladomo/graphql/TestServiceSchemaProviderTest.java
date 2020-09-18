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

import com.gs.fw.common.mithra.test.MithraTestResource;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class TestServiceSchemaProviderTest
{
    @Test
    public void findOne() throws Exception
    {
        assertQuery (
                "{  personById(personId: 1) {  personId  firstName  } }",
                "{personById={personId=1, firstName=Taro}}");
    }

    @Test
    public void findManyWithDeepFetch() throws Exception
    {
        assertQuery (
                "{ courseById(courseId: 101) { courseId name teacher { firstName lastName }}}",
                "{courseById={courseId=101, name=Music, teacher={firstName=Michael, lastName=Barkley}}}");
    }

    @Test
    public void findManyRelationshipTraversal() throws Exception
    {
        assertQuery (
                "{ courses (filter: {courseId: {in: [201, 5, 101]} })\n" +
                        "  {\n" +
                        "    courseId\n" +
                        "    name\n" +
                        "    teacher {\n" +
                        "      firstName\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                "{courses=[{courseId=5, name=Accounting, teacher={firstName=Ying}}, {courseId=101, name=Music, teacher={firstName=Michael}}, {courseId=201, name=Poetry, teacher={firstName=Ying}}]}");
    }

    @Test
    public void metadata() throws Exception
    {
        assertQuery (
                "{ courseById(courseId: 101) { __typename courseId }}",
                "{courseById={__typename=Course, courseId=101}}");
    }

    @Test
    public void reladomoFinder() throws Exception
    {
        assertQuery (
                "{ courses (findMany: {courseId: {eq: 101} OR: [{name: {startsWith: \"Po\"}}] }) { courseId name }}",
                "{courses=[{courseId=101, name=Music}, {courseId=201, name=Poetry}]}");
    }

    @Test
    public void orderByAndLimit() throws Exception
    {
        assertQuery (
                "{ courses (where: {courseId: {notEq: 11}} order_by: {courseId: desc}  limit: 2) { courseId name }}",
                "{courses=[{courseId=201, name=Poetry}, {courseId=101, name=Music}]}");
    }

    @Test
    public void multipleQueries() throws Exception
    {
        assertQuery (
                "{ personById(personId: 1) {  personId  firstName  } courseById(courseId: 101) { courseId name } }",
                "{personById={personId=1, firstName=Taro}, courseById={courseId=101, name=Music}}");
    }

    @Test
    public void aggregateSum() throws Exception
    {
        assertQuery (
                "query { balance_aggregate (filter: {\n" +
                        "    businessDate: {eq: \"2019-10-12\"}}) \n" +
                        "  { \n" +
                        "    accountNum\n" +
                        "    value { sum }\n" +
                        "  }\n" +
                        "}",
                "{balance_aggregate=[{accountNum=100002, value={sum=21211.23}}, {accountNum=100003, value={sum=2121.23}}, {accountNum=100001, value={sum=2122211.23}}]}");
    }

    @Test
    public void aggregateManyTypes() throws Exception
    {
        assertQuery (
                "query { balance_aggregate (filter: {\n" +
                        "    businessDate: {eq: \"2019-10-12\"}} )\n" +
                        "{\n" +
                        "    account{\n" +
                        "        accountNum\n" +
                        "    }\n" +
                        "    accountNum\n" +
                        "    value { sum }\n" +
                        "}}",
                " {balance_aggregate=[{account={accountNum=100001}, accountNum=100001, value={sum=2122211.23}}, {account={accountNum=100002}, accountNum=100002, value={sum=21211.23}}, {account={accountNum=100003}, accountNum=100003, value={sum=2121.23}}]}");
    }

    @Test
    public void tupleIn () throws Exception
    {
        assertQuery (
                "query { persons (findMany: {\n" +
                        "  tupleIn: [\n" +
                        "    {firstName: \"Olga\" lastName: \"Ivanova\"},\n" +
                        "    {firstName: \"John\" lastName: \"Barkley\"},\n" +
                        "    {firstName: \"John\" lastName: \"Smith\"}\n" +
                        "  ]\n" +
                        "})\n" +
                        "  {\n" +
                        "    firstName\n" +
                        "    lastName\n" +
                        "  }\n" +
                        "}",
                "{persons=[{firstName=John, lastName=Smith}, {firstName=Olga, lastName=Ivanova}]}");
    }

    @Test
    public void insert () throws Exception
    {
        assertQuery (
                "mutation { course_insert(course: {name: \"Thermodynamics\"}) { courseId name }}",
                "{course_insert={courseId=202, name=Thermodynamics}}");
        assertQuery (
                "{ courses (findMany: {name: {endsWith: \"dynamics\"}}) { courseId name }}",
                "{courses=[{courseId=202, name=Thermodynamics}]}");
    }

    private void assertQuery (String request, String expectedResult) throws Exception
    {
        ExecutionResult executionResult = this.graphQL.execute (request);

        Assert.assertTrue ("" + executionResult.getErrors (), executionResult.getErrors ().isEmpty ());

        String result = "" + executionResult.getData ();
        System.out.println("result: " + result);
        JSONAssert.assertEquals ("actual: " + result, expectedResult, result, false);
    }


    private GraphQL graphQL;
    private MithraTestResource mithraTestResource;

    @Before
    public void init()
    {
        this.mithraTestResource = SampleLoadServlet.initReladomo("TestReladomoRuntimeConfig.xml", "sample-data.txt");

        mithraTestResource.setUp();

        GraphQLSchema graphQLSchema = SchemaProvider.forResource("sample-schema.graphqls");
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