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

import com.gs.fw.common.mithra.finder.DeepRelationshipUtility;


public class TestRelationshipsNoInClause extends TestRelationships
{
    public TestRelationshipsNoInClause(String s)
    {
        super(s);
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        DeepRelationshipUtility.getInstance().setMaxSimplifiedIn(0);
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        DeepRelationshipUtility.getInstance().setMaxSimplifiedIn(1000);
    }

    public void testMultiLevelSimplifiedInClause()
    {
        // do nothing, test is only for in-clause
    }

    public void testDeepFetchInClauseWithBypassCache()
    {
        // do nothing, test is only for in-clause
    }
}
