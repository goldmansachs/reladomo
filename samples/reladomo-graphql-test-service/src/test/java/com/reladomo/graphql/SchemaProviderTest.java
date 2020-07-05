package com.gs.reladomo.graphql;

import com.gs.fw.common.mithra.test.MithraTestResource;
import com.reladomo.graphql.SchemaProvider;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class SchemaProviderTest
{
    @Test
    public void findOne () throws Exception
    {
        assertQuery (
                "{  personById(personId: 1) {  personId  firstName  } }",
                "{personById={personId=1, firstName=Taro}}");
    }

    @Test
    public void findManyWithDeepFetch () throws Exception
    {
        assertQuery (
                "{ courseById(courseId: 101) { courseId name teacher { firstName lastName }}}",
                "{courseById={courseId=101, name=Music, teacher={firstName=Michael, lastName=Barkley}}}");
    }

    @Test
    public void findManyRelationshipTraversal () throws Exception
    {
        assertQuery (
                "{ courses (findMany: {courseId: {in: [201, 5, 101]} and: {teacher: {firstName: {startsWith: \"Y\"}}}})\n" +
                        "  {\n" +
                        "    courseId\n" +
                        "    name\n" +
                        "    teacher {\n" +
                        "      firstName\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                " {courses=[{courseId=5, name=Accounting, teacher={firstName=Ying}}, {courseId=201, name=Poetry, teacher={firstName=Ying}}]}");
    }

    @Test
    public void metadata () throws Exception
    {
        assertQuery (
                "{ courseById(courseId: 101) { __typename courseId }}",
                "{courseById={__typename=Course, courseId=101}}");
    }

    @Test
    public void reladomoFinder () throws Exception
    {
        assertQuery (
                "{ courses (findMany: {courseId: {eq: 101} or: {name: {startsWith: \"Po\"}}}) { courseId name }}",
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
    public void multipleQueries () throws Exception
    {
        assertQuery (
                "{ personById(personId: 1) {  personId  firstName  } courseById(courseId: 101) { courseId name } }",
                "{personById={personId=1, firstName=Taro}, courseById={courseId=101, name=Music}}");
    }

    @Test
    public void aggregateSum () throws Exception
    {
        assertQuery (
                "query { balance_aggregate (filter: {\n" +
                        "    processingDate: {eq: \"2019-10-13T04:04:39+00:00\"} and: {\n" +
                        "    businessDate: {eq: \"2019-10-12\"}}}) \n" +
                        "  { \n" +
                        "    accountNum\n" +
                        "    sumOfValue\n" +
                        "  }\n" +
                        "}",
                " {balance_aggregate=[{accountNum=100002, sumOfValue=21211.23}, {accountNum=100003, sumOfValue=2121.23}, {accountNum=100001, sumOfValue=2122211.23}]}");
    }

    @Test
    public void aggregateSumWithJoin () throws Exception
    {
        assertQuery (
                "query { balance_aggregate (filter: {\n" +
                        "    processingDate: {eq: \"2019-10-13T04:04:39+00:00\"} and: {\n" +
                        "        businessDate: {eq: \"2019-10-12\"}}})\n" +
                        "{\n" +
                        "    account{\n" +
                        "        accountNum\n" +
                        "    }\n" +
                        "    accountNum\n" +
                        "    sumOfValue\n" +
                        "}}",
                "{balance_aggregate=[{account={accountNum=100001}, accountNum=100001, sumOfValue=2122211.23}, {account={accountNum=100002}, accountNum=100002, sumOfValue=21211.23}, {account={accountNum=100003}, accountNum=100003, sumOfValue=2121.23}]}");
    }

    @Test
    public void processingDateMilestone () throws Exception
    {
        assertQuery (
                "query { accounts (findMany: {processingDate: {eq: \"2019-10-13T04:04:39+00:00\"}}) { accountNum processingDateFrom processingDateTo}}",
                "{accounts=[{accountNum=100001, processingDateFrom=\"2019-01-03T08:00:00Z\", processingDateTo=\"INFINITY\"}, {accountNum=100002, processingDateFrom=\"2019-01-03T08:00:00Z\", processingDateTo=\"INFINITY\"}, {accountNum=100003, processingDateFrom=\"2019-01-03T08:00:00Z\", processingDateTo=\"INFINITY\"}]}");
    }

    @Test
    public void implicitMilestoning () throws Exception
    {
        assertQuery (
                "query { balances (findMany: {value: {greaterThan: 100}}) { accountNum } }",
                "{balances=[{accountNum=100001}, {accountNum=100002}, {accountNum=100003}]}");
    }

    @Test
    public void bitemporalMilestone () throws Exception
    {
        assertQuery (
                "query { balances (findMany: {processingDate: {eq: \"2019-10-13T04:04:39+00:00\"} and: {businessDate: {eq: \"2019-10-12T23:59:00+00:00\"}}}) { accountNum processingDateFrom processingDateTo }}",
                "{balances=[" +
                        "{accountNum=100001, processingDateFrom=\"2010-01-03T08:00:00Z\", processingDateTo=\"INFINITY\"}," +
                        " {accountNum=100002, processingDateFrom=\"2010-01-03T08:00:00Z\", processingDateTo=\"INFINITY\"}," +
                        " {accountNum=100003, processingDateFrom=\"2010-01-03T08:10:00Z\", processingDateTo=\"INFINITY\"}]}");
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
    public void init () throws Exception
    {
        this.mithraTestResource = SampleLoadServlet.initReladomo ("TestReladomoRuntimeConfig.xml", "test_data.txt");
        GraphQLSchema graphQLSchema = SchemaProvider.forResource ("test_schema.graphqls");
        this.graphQL = GraphQL.newGraphQL (graphQLSchema).build ();
    }

    @After
    public void teardown ()
    {
        this.mithraTestResource.tearDown ();
        this.mithraTestResource = null;
        this.graphQL = null;
    }

}