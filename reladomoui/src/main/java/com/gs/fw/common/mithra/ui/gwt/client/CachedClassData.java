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

package com.gs.fw.common.mithra.ui.gwt.client;

import java.io.Serializable;


public class CachedClassData implements Serializable
{

    public static final byte SQL_IS_OFF = (byte) 10;
    public static final byte SQL_IS_ON = (byte) 20;
    public static final byte SQL_IS_MAX_ON = (byte) 30;

    private boolean isPartialCache;
    private byte sqlDebugLevel;
    private String className;
    private int cacheSize;

    public CachedClassData()
    {
        // for gwt serializable
    }

    public CachedClassData(int cacheSize, String className, boolean partialCache, byte sqlDebugLevel)
    {
        this.cacheSize = cacheSize;
        this.className = className;
        isPartialCache = partialCache;
        this.sqlDebugLevel = sqlDebugLevel;
    }

    public int getCacheSize()
    {
        return cacheSize;
    }

    public String getClassName()
    {
        return className;
    }

    public boolean isPartialCache()
    {
        return isPartialCache;
    }

    public boolean isSqlOff()
    {
        return this.sqlDebugLevel == SQL_IS_OFF;
    }

    public boolean isSqlOn()
    {
        return this.sqlDebugLevel == SQL_IS_ON;
    }

    public boolean isSqlMaxOn()
    {
        return this.sqlDebugLevel == SQL_IS_MAX_ON;
    }
}
