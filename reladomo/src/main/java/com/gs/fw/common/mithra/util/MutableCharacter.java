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


public class MutableCharacter implements java.io.Serializable,
                                      Comparable<MutableCharacter>, Nullable
{

    private char value;
    private boolean nullFlag = true;
    private boolean initialized = false;

    public MutableCharacter()
    {
    }

    public MutableCharacter(char value)
    {
        this.value = value;
        setInitializedAndNotNull();
    }

    public Object getAsObject()
    {
        if (isNull()) return null;
        return Character.valueOf(value);
    }

    public char getValue()
    {
        checkForNull();
        return value;
    }

    public int compareTo(MutableCharacter b)
    {
        return charCompare(b.value);
    }

    public int charCompare(char b)
    {
        if (isNull() || value < b)
            return -1;
        if (value > b)
            return 1;
        return 0;
    }

    public void replace(char value)
    {
        this.value = value;
        setInitializedAndNotNull();
    }

    public void checkForNull()
    {
        if(nullFlag)
        {
            throw new RuntimeException("Null Value");
        }
    }

    public boolean isNull()
    {
        return nullFlag;
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    protected void setInitializedAndNotNull()
    {
        this.nullFlag = false;
        this.initialized = true;
    }

    public void setValueNull()
    {
        if(nullFlag)
            this.nullFlag = true;
        this.initialized = true;
    }
    @Override
    public int hashCode()
    {
        if (this.isNull()) return 0;
        return this.value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableCharacter that = (MutableCharacter) o;

        return this.isNull() ?  that.isNull() : this.value == that.value;
    }
}
