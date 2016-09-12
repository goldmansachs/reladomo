
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

import java.util.IdentityHashMap;
import java.util.Map;

public class ThreadLocalRegistry
{
    private static ThreadLocalRegistry instance = new ThreadLocalRegistry();
    private ThreadLocal<Map> inheritableMap = new MapInheritableThreadLocal();

    public static ThreadLocalRegistry getInstance()
    {
        return instance;
    }

    public Object getThreadLocalValueFor(Object instance)
    {
        Map map = inheritableMap.get();
        if (map == null)
        {
            return null;
        }
        return map.get(instance);
    }

    public void setThreadLocalValueFor(Object instance, Object value)
    {
        Map map = inheritableMap.get();
        if (map == null)
        {
            map = new IdentityHashMap();
            inheritableMap.set(map);
        }
        map.put(instance, value);
    }

    public void clearAllInstancesForThread()
    {
        inheritableMap.set(null);
    }

    public void clear(Object instance)
    {
        Map map = inheritableMap.get();
        if (map != null)
        {
            map.remove(instance);
        }
    }

    public Map getLocalStateCopy()
    {
        Map map = inheritableMap.get();
        return map == null || map.size() == 0 ? null : new IdentityHashMap(map);
    }

    public void setLocalState(Map inheritedState)
    {
        this.inheritableMap.set(inheritedState);
    }

    private static class MapInheritableThreadLocal extends InheritableThreadLocal<Map>
    {
        @Override
        protected Map childValue(Map parentValue)
        {
            if (parentValue == null || parentValue.size() == 0)
            {
                return null;
            }
            return new IdentityHashMap(parentValue);
        }
    }
}
