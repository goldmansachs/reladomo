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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.test.attribute.MappedAttributeTest;
import com.gs.fw.common.mithra.test.attribute.ToStringFormatterTest;
import com.gs.fw.common.mithra.test.bulkloader.BulkLoaderTestSuite;
import com.gs.fw.common.mithra.test.localtx.TestSingleThreadedLocalTm;
import com.gs.fw.common.mithra.test.mithraTestResource.TestMithraRuntimeConfigVerifier;
import com.gs.fw.common.mithra.test.mithraTestResource.TestMithraTestResource;
import com.gs.fw.common.mithra.test.mithraTestResource.TestMithraTestResourceBackwardCompatibility;
import com.gs.fw.common.mithra.test.tinyproxy.ErrorConditionsTest;
import com.gs.fw.common.mithra.test.tinyproxy.SimplePspServiceTest;
import com.gs.fw.common.mithra.test.util.*;
import junit.framework.Test;
import junit.framework.TestSuite;


public class MithraUtilTestSuite extends TestSuite
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestCollections.class);
        suite.addTestSuite(TestMithraTestDataParser.class);

        //Test new MithraTestResource backward compatibility and new features
        suite.addTestSuite(TestMithraTestResourceBackwardCompatibility.class);
        suite.addTestSuite(TestMithraTestResource.class);
        suite.addTestSuite(TestMithraRuntimeConfigVerifier.class);
        suite.addTestSuite(TestPartialCache.class);
        suite.addTestSuite(TestFullUniqueIndex.class);
        suite.addTestSuite(TestNonUniqueIndex.class);
        suite.addTestSuite(TestNonUniqueIdentityIndex.class);
        suite.addTestSuite(TestFullSemiUniqueDatedIndex.class);
        suite.addTestSuite(PartialDatedCacheTest.class);
        suite.addTestSuite(FullDatedCacheTest.class);
        suite.addTestSuite(FullDatedTransactionalCacheTest.class);
        suite.addTestSuite(TestSingleThreadedLocalTm.class);
        suite.addTestSuite(TestWildcardParser.class);
        suite.addTestSuite(TestThreadExecutor.class);
        suite.addTestSuite(TestImmutableTimestamp.class);
        suite.addTestSuite(TestAttributeMetaData.class);
        suite.addTestSuite(TestConcurrentDatedObjectIndex.class);
        suite.addTestSuite(TestConcurrentWeakPool.class);
        suite.addTestSuite(TestConcurrentOnHeapStringIndex.class);
        suite.addTestSuite(TestConcurrentOffHeapStringIndex.class);
        suite.addTestSuite(TestConcurrentQueryIndex.class);

        suite.addTestSuite(TestCache.class);
        suite.addTestSuite(TestIndexReference.class);
        // Tests for the bulk loader
        suite.addTest(ExecuteTestSuite.suite());
        suite.addTest(BulkLoaderTestSuite.suite());
        suite.addTestSuite(ToStringFormatterTest.class);
        suite.addTestSuite(MappedAttributeTest.class);
        suite.addTestSuite(ExceptionSerializationTest.class);

        suite.addTestSuite(TestNotificationServer.class);
        suite.addTestSuite(TestDualNotificationServers.class);
        suite.addTestSuite(SybaseDatabaseTypeTest.class);
        suite.addTestSuite(MilestoneRectangleTest.class);
        suite.addTestSuite(BooleanFilterTest.class);
        suite.addTestSuite(TrueFilterTest.class);
        suite.addTestSuite(FalseFilterTest.class);
        suite.addTestSuite(ConstantSetsTest.class);
        //tinyproxy tests
        suite.addTestSuite(SimplePspServiceTest.class);
        suite.addTestSuite(ErrorConditionsTest.class);

        return suite;
    }
}
