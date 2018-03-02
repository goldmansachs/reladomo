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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.util;

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.Set;


public class AnalyzedWildcardPattern
{
    private Set<String> plain;
    private Set<String> endsWith;
    private Set<String> contains;
    private Set<String> wildcard;
    private IntObjectHashMap<Set<String>> substring;

    public Set<String> getPlain()
    {
        return plain;
    }

    public Set<String> getEndsWith()
    {
        return endsWith;
    }

    public Set<String> getContains()
    {
        return contains;
    }

    public Set<String> getWildcard()
    {
        return wildcard;
    }

    public IntObjectHashMap<Set<String>> getSubstring()
    {
        return substring;
    }

    public void addPlain(String s)
    {
        if (this.plain == null)
        {
            this.plain = UnifiedSet.newSet(); 
        }
        this.plain.add(s);
    }
        
    public void addContains(String s)
    {
        if (this.contains == null)
        {
            this.contains = UnifiedSet.newSet(); 
        }
        this.contains.add(s);
    }
        
    public void addWildcard(String s)
    {
        if (this.wildcard == null)
        {
            this.wildcard = UnifiedSet.newSet(); 
        }
        this.wildcard.add(s);
    }
        
    public void addSubstring(int length, String s)
    {
        if (this.substring == null)
        {
            this.substring = new IntObjectHashMap<Set<String>>();
        }
        Set<String> stringSet = this.substring.get(length);
        if (stringSet == null)
        {
            stringSet = UnifiedSet.newSet();
            this.substring.put(length, stringSet);
        }
        stringSet.add(s);
    }

    public void addEndsWith(String s)
    {
        if (this.endsWith == null)
        {
            this.endsWith = UnifiedSet.newSet(); 
        }
        this.endsWith.add(s);
    }
}
