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

package com.gs.fw.common.mithra.test.generator;

import com.gs.fw.common.mithra.portal.MithraAbstractObjectPortal;
import com.gs.fw.common.mithra.test.*;
import com.gs.fw.common.mithra.test.aggregate.AggregateTestSuite;
import com.gs.fw.common.mithra.test.evo.TestEmbeddedValueObjects;
import com.gs.fw.common.mithra.test.inherited.TestReadOnlyInherited;
import com.gs.fw.common.mithra.test.inherited.TestTxInherited;
import com.gs.fw.common.mithra.test.pure.TestPureObjects;
import com.gs.fw.common.mithra.test.util.TestVerboseSerializer;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import junit.framework.Test;
import junit.framework.TestSuite;


public class GeneratorTestSuite
        extends TestSuite
{
    public static Test suite()
    {
        MithraCpuBoundThreadPool.setParallelThreshold(2);
        MithraAbstractObjectPortal.setTransitiveThreshold(2);
        TestSuite suite = new TestSuite();
        suite.addTestSuite(MaxLenValidatorTest.class);
        suite.addTestSuite(TableInfoTest.class);
        suite.addTestSuite(DatabaseIndexValidatorTest.class);
        suite.addTestSuite(DatabaseTableValidatorTest.class);
        return suite;
    }
}
