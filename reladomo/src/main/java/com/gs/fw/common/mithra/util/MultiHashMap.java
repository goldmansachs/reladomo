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

import com.gs.collections.impl.map.mutable.UnifiedMap;

import java.util.*;


/**
 * this class does not implement Map<KEY, List<VALUE>> because the interfaces are not compatible for put.
 * put can be overloaded, but that may cause the compiler to choose the wrong version if MultiHashMap is used
 * without specifying KEY VALUE (that is, without generics) and the intent is to store list of lists.
 * It also allows us to fix the generic problems in the Map interface with regards to containsKey, containsValue, etc.
 */
public class MultiHashMap<KEY, VALUE>
{

    private Map<KEY, List<VALUE>> delegate;

    public MultiHashMap(Map delegate)
    {
        this.delegate = delegate;
    }

    public MultiHashMap()
    {
        this.delegate = new UnifiedMap();
    }

    public void clear()
    {
        delegate.clear();
    }

    public boolean containsKey(KEY key)
    {
        return delegate.containsKey(key);
    }

    public boolean containsValue(VALUE value)
    {
        Iterator<Map.Entry<KEY, List<VALUE>>> i = entrySet().iterator();
        while (i.hasNext())
        {
            Map.Entry<KEY, List<VALUE>> e = i.next();
            if (e.getValue().contains(value))
                return true;
        }
        return false;
    }

    public Set<Map.Entry<KEY, List<VALUE>>> entrySet()
    {
        return delegate.entrySet();
    }

    public boolean equals(Object o)
    {
        return delegate.equals(o);
    }

    public List<VALUE> get(KEY key)
    {
        return delegate.get(key);
    }

    public int hashCode()
    {
        return delegate.hashCode();
    }

    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }

    public Set<KEY> keySet()
    {
        return delegate.keySet();
    }

    public void put(KEY key, VALUE value)
    {
        List<VALUE> existing = this.delegate.get(key);
        if (existing == null)
        {
            existing = new ArrayList<VALUE>();
            this.delegate.put(key, existing);
        }
        existing.add(value);
    }

    public void putAll(Map<? extends KEY, ? extends List<VALUE>> t)
    {
        delegate.putAll(t);
    }

    public List<VALUE> remove(KEY key)
    {
        return delegate.remove(key);
    }

    public int size()
    {
        return delegate.size();
    }

    public Collection<List<VALUE>> values()
    {
        return delegate.values();
    }

    public List<List<VALUE>> valuesAsList()
    {
        return new ArrayList(this.delegate.values());
    }
}
