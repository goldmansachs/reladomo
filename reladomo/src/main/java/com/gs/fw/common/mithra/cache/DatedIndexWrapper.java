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

package com.gs.fw.common.mithra.cache;



import org.slf4j.Logger;

public abstract class DatedIndexWrapper implements Index
{
    @Override
    public boolean isInitialized()
    {
        return true;
    }

    @Override
    public Index getInitialized(IterableIndex iterableIndex)
    {
        return this;
    }

    public Object get(Object indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(int indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(byte[] indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(long indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(double indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(boolean indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(float indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object get(char indexValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Object getNulls()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isUnique()
    {
        return true;
    }

    public int getAverageReturnSize()
    {
        return 1;
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        return multiplier;
    }

    public Object put(Object businessObject)
    {
        throw new RuntimeException("not implemented");
    }

    public Object putUsingUnderlying(Object businessObject, Object underlying)
    {
        throw new RuntimeException("not implemented");
    }

    public Object remove(Object businessObject)
    {
        throw new RuntimeException("not implemented");
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void destroy()
    {
        //nothing to do
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {

    }

    @Override
    public void ensureExtraCapacity(int size)
    {

    }
}
