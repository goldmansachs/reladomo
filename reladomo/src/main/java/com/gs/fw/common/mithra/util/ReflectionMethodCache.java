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


import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.block.function.Function3;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.lang.reflect.Method;

public class ReflectionMethodCache
{
    private static final Function0<ConcurrentHashMap<String,Method>> CONCURRENT_HASH_MAP_CONSTRUCTOR = new Function0<ConcurrentHashMap<String, Method>>()
    {
        @Override
        public ConcurrentHashMap<String, Method> value()
        {
            return ConcurrentHashMap.newMap();
        }
    };

    private static final Function2<String, Method, String> PASS_THRU = new Function2<String, Method, String>()
    {
        @Override
        public String value(String key, Method value)
        {
            return key;
        }
    };

    private static final Function3<Class,Object,String,Method> METHOD_FACTORY = new Function3<Class, Object, String, Method>()
    {
        @Override
        public Method value(Class c, Object ignored, String methodName)
        {
            try
            {
                return c.getMethod(methodName, (Class[]) null);
            }
            catch (NoSuchMethodException e)
            {
                throw new ReflectionMethodCacheException("Could not find method "+methodName+" in class "+c.getName(),e);
            }

        }
    };

    private static ConcurrentHashMap<Class, ConcurrentHashMap<String, Method>> cache = ConcurrentHashMap.newMap();

    public static Method getZeroArgMethod(Class c, String methodName) throws ReflectionMethodCacheException
    {
        ConcurrentHashMap<String, Method> classToMethodMap = cache.getIfAbsent(c, CONCURRENT_HASH_MAP_CONSTRUCTOR);
        Method method = classToMethodMap.putIfAbsentGetIfPresent(methodName, PASS_THRU, METHOD_FACTORY, c, null);
        if (method == null)
        {
            method = classToMethodMap.get(methodName);
        }
        return method;
    }

    public static class ReflectionMethodCacheException extends RuntimeException
    {
        public ReflectionMethodCacheException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
