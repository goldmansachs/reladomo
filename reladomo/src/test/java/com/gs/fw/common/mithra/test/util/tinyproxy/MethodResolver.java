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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.lang.reflect.Method;
import java.util.Map;

import com.gs.collections.impl.map.mutable.UnifiedMap;

public class MethodResolver
{
    private final Map<Method, String> methodToNameMap = new UnifiedMap<Method, String>();
    private final Map<String, Method> nameToMethodMap = new UnifiedMap<String, Method>();
    private final Map<Method, Integer> methodToTimeoutMap = new UnifiedMap<Method, Integer>();

    private final Class serviceClass;

    public MethodResolver(Class serviceClass)
    {
        this.serviceClass = serviceClass;
        Method[] methodList = serviceClass.getMethods();

        for (Method method : methodList)
        {
            String mangledName = this.mangleName(method);
            this.methodToNameMap.put(method, mangledName);
            this.nameToMethodMap.put(mangledName, method);

            Integer timeout = this.buildTimeoutFromProperty(method);
            if (timeout != null)
            {
                this.methodToTimeoutMap.put(method, timeout);
            }
        }
    }

    public String getMangledMethodName(Method method)
    {
        return this.methodToNameMap.get(method);
    }

    public Method getMethodFromMangledName(String mangledName)
    {
        return this.nameToMethodMap.get(mangledName);
    }

    public Integer getMethodTimeout(Method method)
    {
        return this.methodToTimeoutMap.get(method);
    }

    protected String mangleName(Method method)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(method.getName());

        Class[] params = method.getParameterTypes();
        for (int i = 0; i < params.length; i++)
        {
            sb.append('_');
            this.mangleClass(sb, params[i]);
        }

        return sb.toString();
    }

    protected Integer buildTimeoutFromProperty(Method method)
    {
        String methodName = this.mangleName(method);

        String propertyName = "psp.timeout." + this.getServiceClass().getName() + "." + methodName;
        String methodTimeoutSetting = System.getProperty(propertyName);

        Integer timeout = null;
        if (methodTimeoutSetting != null)
        {
            try
            {
                timeout = Integer.valueOf(methodTimeoutSetting);
            }
            catch (Exception ex)
            {
                throw new PspRuntimeException(
                        "Failed to parse method timeout setting, method: "
                                + propertyName + " timeout: " + methodTimeoutSetting);
            }
        }

        return timeout;
    }

    /**
     * Mangles a classname.
     */
    protected void mangleClass(StringBuilder sb, Class cl)
    {
        String name = cl.getName();

        if (cl.isArray())
        {
            sb.append("array_");
            this.mangleClass(sb, cl.getComponentType());
        }
        else
        {
            sb.append(name);
        }
    }

    public Class getServiceClass()
    {
        return this.serviceClass;
    }
}
