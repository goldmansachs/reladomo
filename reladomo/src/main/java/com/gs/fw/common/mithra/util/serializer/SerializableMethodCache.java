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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class SerializableMethodCache
{
    private static final Comparator<? super Method> METHOD_NAME_COMPARATOR = new Comparator<Method>()
    {
        @Override
        public int compare(Method o1, Method o2)
        {
            return o1.getName().compareTo(o2.getName());
        }
    };
    private static Logger logger = LoggerFactory.getLogger(SerializableMethodCache.class.getName());
    private static SerializableMethodCache ourInstance = new SerializableMethodCache();

    public static SerializableMethodCache getInstance()
    {
        return ourInstance;
    }

    private ConcurrentHashMap<ClassAndContextName, List<Method>> cache = ConcurrentHashMap.newMap();

    private SerializableMethodCache()
    {
    }

    public List<Method> get(Class clazz, Set<Class> contextNames)
    {
        ClassAndContextName key = new ClassAndContextName(clazz, contextNames);
        List<Method> methods = cache.get(key);
        if (methods == null)
        {
            methods = findMethods(clazz, contextNames);
            cache.put(key, methods);
        }
        return methods;
    }

    private List<Method> findMethods(Class clazz, Set<Class> contextNames)
    {
        Method[] allMethods = clazz.getMethods();
        FastList<Method> result = FastList.newList();
        for(Method method: allMethods)
        {
            ReladomoSerialize annotation = method.getAnnotation(ReladomoSerialize.class);
            if (annotation != null)
            {
                Class[] names = annotation.serialViews();
                for (Class name : names)
                {
                    if (contextNames.contains(name))
                    {
                        if (method.getParameterTypes().length == 0)
                        {
                            if (method.getReturnType().equals(Void.TYPE))
                            {
                                logger.warn("Incorrect method annotation in class " + clazz.getName() + " method " + method.getName() + " @ReladomoSerialize can only be used with methods that return something");
                            }
                            else
                            {
                                result.add(method);
                                break;
                            }
                        }
                        else
                        {
                            logger.warn("Incorrect method annotation in class " + clazz.getName() + " method " + method.getName() + " @ReladomoSerialize can only be used with methods that have no parameters");
                        }
                    }
                }
            }
        }
        result.trimToSize();
        result.sortThis(METHOD_NAME_COMPARATOR);
        return result;
    }

    private static class ClassAndContextName
    {
        private final Class clazz;
        private final Set<Class> contextNames;

        public ClassAndContextName(Class clazz, Set<Class> contextNames)
        {
            this.clazz = clazz;
            this.contextNames = contextNames;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClassAndContextName that = (ClassAndContextName) o;

            if (!clazz.equals(that.clazz)) return false;
            return contextNames.equals(that.contextNames);

        }

        @Override
        public int hashCode()
        {
            int result = clazz.hashCode();
            result = 31 * result + contextNames.hashCode();
            return result;
        }
    }
}
