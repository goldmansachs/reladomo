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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.cacheloader.CacheLoaderConfig;
import junit.framework.TestCase;


public class CacheLoaderConfigTest extends TestCase
{
    public void testNewInstace()
    {
        Object anInstance = CacheLoaderConfig.newInstance(CacheLoaderConfig.class.getName());
        assertTrue(anInstance instanceof CacheLoaderConfig);

        anInstance = CacheLoaderConfig.newInstance(TestParameterConstractor.class.getName() + "(paramValue)    ");
        assertEquals("paramValue", ((TestParameterConstractor) anInstance).param);
    }

    public static class TestParameterConstractor
    {
        String param;

        public TestParameterConstractor(String aString)
        {
            param = aString;
        }
    }
}
