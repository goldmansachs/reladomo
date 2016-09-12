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

package com.gs.fw.common.mithra.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;


public class StringToIntMap
{
    private static Field STRING_HASH32_FIELD;

    private AtomicInteger end = new AtomicInteger(1);
    private String[] list = new String[32000];

    public StringToIntMap()
    {
        instantiateStringReflection();
    }

    public int store(String aString)
    {
        int pos = end.incrementAndGet();
        if (pos == list.length)
        {
            //we own the resize
            String[] newArray = new String[pos << 1];
            System.arraycopy(list, 0, newArray, 0, list.length);
            synchronized (this)
            {
                list = newArray;
                this.notifyAll();
            }
        }
        else if (pos > list.length)
        {
            synchronized (this)
            {
                while(pos > list.length)
                {
                    try
                    {
                        this.wait();
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
            }
        }
        try
        {
            STRING_HASH32_FIELD.setInt(aString, pos);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("could not store reference in hash32 field");
        }
        list[pos] = aString;
        return pos;
    }

    public int getPtrOfPooledString(String pooledString)
    {
        try
        {
            return STRING_HASH32_FIELD.getInt(pooledString);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot read from string " + pooledString, e);
        }
    }

    public String getPooledString(int ptr)
    {
        return list[ptr];
    }


    private static void instantiateStringReflection()
    {
        try
        {
            STRING_HASH32_FIELD = String.class.getDeclaredField("hash32");
            STRING_HASH32_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException("could not get String fields", e);
        }
    }

    protected long getStringCount()
    {
        return end.get() - 1;
    }
}
