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

package com.gs.fw.common.mithra.util.serializer;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeserializableMethodCache
{
    private static Logger logger = LoggerFactory.getLogger(DeserializableMethodCache.class.getName());
    private static DeserializableMethodCache ourInstance = new DeserializableMethodCache();

    public static DeserializableMethodCache getInstance()
    {
        return ourInstance;
    }

    private ConcurrentHashMap<Class, Map<String, Method>> cache = ConcurrentHashMap.newMap();

    private DeserializableMethodCache()
    {
    }

    public Map<String, Method> getAll(Class clazz)
    {
        Map<String, Method> methods = cache.get(clazz);
        if (methods == null)
        {
            methods = findMethods(clazz);
            cache.put(clazz, methods);
        }
        return methods;
    }

    public Method get(Class clazz, String name)
    {
        return getAll(clazz).get(name);
    }

    private Map<String, Method> findMethods(Class clazz)
    {
        Method[] allMethods = clazz.getMethods();
        Map<String, Method> result = UnifiedMap.newMap();
        for(Method method: allMethods)
        {
            ReladomoDeserialize annotation = method.getAnnotation(ReladomoDeserialize.class);
            if (annotation != null)
            {
                if (method.getParameterTypes().length == 1)
                {
                    result.put(method.getName(), method);
                }
                else
                {
                    logger.warn("Incorrect method annotation in class " + clazz.getName() + " method " + method.getName() + " @ReladomoSerialize can only be used with methods that have no parameters");
                }
            }
        }
        return result;
    }

}
