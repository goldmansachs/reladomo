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

public final class TrueFilter implements BooleanFilter
{

    private static final TrueFilter INSTANCE = new TrueFilter();
    private static final String TRUE = "True";

    public static TrueFilter instance()
    {
        return INSTANCE;
    }

    private TrueFilter()
    {
    }

    @Override
    public boolean matches(Object o)
    {
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof TrueFilter;
    }

    @Override
    public BooleanFilter and(Filter that)
    {
        return that instanceof BooleanFilter ? (BooleanFilter) that : new AbstractBooleanFilter.BooleanFilterAdapter(that);
    }

    @Override
    public BooleanFilter or(Filter that)
    {
        return this;
    }

    @Override
    public BooleanFilter negate()
    {
        return FalseFilter.instance();
    }

    @Override
    public int hashCode()
    {
        return 3;
    }

    @Override
    public String toString()
    {
        return TRUE;
    }
}
