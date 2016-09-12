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

import com.gs.fw.common.mithra.test.domain.AllJavaTypesInPk;


/*
 this test is not part of the suite as derby doesn't support VALUES clauses the same way DB2 does.
 the test is used to visually inspect the results after modifing DerbyDatabaseType with this method: 
        public boolean supportsMultiValueInClause()
        {
            return true;
        }
*/
public class TestMultiUpdateUsingDerby extends MithraTestAbstractUsingDerby
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        AllJavaTypesInPk.class
                };
    }

    public void testMultiUpdateChangeIntDerby()
    {
        TestJavaTypesInPk test = new TestJavaTypesInPk();
        test.testMultiUpdateChangeInt();
    }

    public void testMultiUpdateChangeIntLongDerby()
    {
        TestJavaTypesInPk test = new TestJavaTypesInPk();
        test.testMultiUpdateChangeIntLong();
    }

    public void testMultiUpdateChangeLongCharDerby()
    {
        TestJavaTypesInPk test = new TestJavaTypesInPk();
        test.testMultiUpdateChangeLongChar();
    }
}
