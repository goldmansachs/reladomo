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

package com.gs.fw.common.mithra.generator;


public class BeanState
{
    private int intCount = 1;
    private int longCount = 1;
    private int objectCount = 1;

    public int getIntCount()
    {
        return intCount;
    }

    public int getLongCount()
    {
        return longCount;
    }

    public int getObjectCount()
    {
        return objectCount;
    }

    public void increment(AbstractAttribute attribute)
    {
        if (attribute.isBeanIntType())
        {
            intCount++;
        }
        else if (attribute.isBeanLongType())
        {
            longCount++;
        }
        else if (attribute.isBeanObjectType())
        {
            objectCount++;
        }
    }
}
