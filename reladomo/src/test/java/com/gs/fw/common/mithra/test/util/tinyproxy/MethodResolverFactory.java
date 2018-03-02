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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.lang.reflect.Method;

public class MethodResolverFactory
{
    private final MutableMap<String, MethodResolver> methodResolvers = UnifiedMap.newMap();

    public Method resolveMethodForClassName(String className, String mangledMethodName)
    {
        return this.getOrCreateMethodResolver(className).getMethodFromMangledName(mangledMethodName);
    }

    private MethodResolver getOrCreateMethodResolver(String className)
    {
        MethodResolver resolver = this.methodResolvers.get(className);
        try
        {
            if (resolver == null)
            {
                resolver = new MethodResolver(Class.forName(className));
                this.methodResolvers.put(className, resolver);
            }
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        return resolver;
    }
}
