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

import com.gs.collections.impl.set.mutable.UnifiedSet;

import java.util.Collection;

public class ConstantStringSet extends UnifiedSet<String>
{
    static final long serialVersionUID = 3699750099184281793L;
    private int hashCode;

    public ConstantStringSet()
    {
    }

    public ConstantStringSet(Collection<String> collection)
    {
        super(collection);
    }

    public boolean equals(Object other)
    {
        if (this == other) return true;
        return super.equals(other);
    }

    public int hashCode()
    {
        if (this.hashCode == 0)
        {
            this.hashCode = super.hashCode();
        }
        return this.hashCode;
    }
}
