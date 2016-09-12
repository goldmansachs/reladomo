
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


public class ColumnInfo implements Comparable
{

    private final String name;
    private final int type;
    private final int size;
    private final int precision;
    private final int scale;
    private final int ordinalPosition;
    private final boolean nullable;

    public ColumnInfo(String name, int type, int size, int precision, int scale, int ordinalPosition, boolean nullable)
    {
        this.name = name;
        this.type = type;
        this.size = size;
        this.precision = precision;
        this.scale = scale;
        this.ordinalPosition = ordinalPosition;
        this.nullable = nullable;
    }

    public String getName()
    {
        return this.name;
    }

    public int getType()
    {
        return this.type;
    }

    public int getSize()
    {
        return this.size;
    }

    public int getPrecision()
    {
        return this.precision;
    }

    public int getScale()
    {
        return this.scale;
    }

    public int getOrdinalPosition()
    {
        return this.ordinalPosition;
    }

    public boolean isNullable()
    {
        return this.nullable;
    }

    public int compareTo(Object o)
    {
        ColumnInfo other = (ColumnInfo) o;
        return this.ordinalPosition - other.ordinalPosition;
    }
}
