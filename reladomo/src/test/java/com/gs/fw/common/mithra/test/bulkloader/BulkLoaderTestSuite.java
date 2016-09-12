
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

package com.gs.fw.common.mithra.test.bulkloader;

import junit.framework.Test;
import junit.framework.TestSuite;


public class BulkLoaderTestSuite extends TestSuite
{

    public static Test suite()
    {
        BulkLoaderTestSuite suite = new BulkLoaderTestSuite();

        suite.addTestSuite(DateFormatterTest.class);
        suite.addTestSuite(DbCharFormatterTest.class);
        suite.addTestSuite(DecimalPlaceFormatterTest.class);
        suite.addTestSuite(SybaseBcpFileTest.class);
        suite.addTestSuite(TimestampFormatterTest.class);
        suite.addTestSuite(TimeZoneTimestampFormatterTest.class);
        suite.addTestSuite(VarCharFormatterTest.class);

        return suite;
    }

}
