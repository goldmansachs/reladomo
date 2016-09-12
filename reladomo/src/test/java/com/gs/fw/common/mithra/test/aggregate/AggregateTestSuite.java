/*
 Copyright 2016 Goldman Sachs.
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

package com.gs.fw.common.mithra.test.aggregate;

import junit.framework.Test;
import junit.framework.TestSuite;


public class AggregateTestSuite extends TestSuite
{

    public static Test suite()
    {
        AggregateTestSuite suite = new AggregateTestSuite();

        suite.addTestSuite(TestSum.class);
        suite.addTestSuite(TestAvg.class);
        suite.addTestSuite(TestMax.class);
        suite.addTestSuite(TestMin.class);
        suite.addTestSuite(TestCount.class);
        suite.addTestSuite(TestAggregateWithNull.class);
        suite.addTestSuite(TestNumericAttribute.class);
        suite.addTestSuite(TestAggregateList.class);
        suite.addTestSuite(TestAggregateListWithOrderBy.class);
        suite.addTestSuite(TestDatedAggregation.class);
        suite.addTestSuite(TestAggregationWithHavingClause.class);
        suite.addTestSuite(TestAggregateBeanList.class);
        suite.addTestSuite(TestAggregateBeanListWithPrimitives.class);
        suite.addTestSuite(TestAggregateBeanListOrderBy.class);
        suite.addTestSuite(TestAggregateBeanListWithHavingClause.class);
        suite.addTestSuite(TestAggregatePrimitiveBeanListWithHavingClause.class);
        suite.addTestSuite(TestAggregateBeanListImmutability.class);
        suite.addTestSuite(TestAggregateBeanListForSubclass.class);
        suite.addTestSuite(TestStandardDeviation.class);
        suite.addTestSuite(TestVariance.class);

        return suite;
    }
}
