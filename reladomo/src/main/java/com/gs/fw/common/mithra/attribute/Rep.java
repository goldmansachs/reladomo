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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.util.ReflectionMethodCache;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Method;


/**
 * this class is used to optimize the size of serialized Attribute objects.
 * The class name is kept short for this reason.
 * With this class, the size of the written bytes goes down to 118 bytes from 378
 */
public class Rep implements Externalizable
{

    private static final long serialVersionUID = 7865112612537628103L;

    private String businessObjectClassName;
    private String attributeName;

    public Rep()
    {
    }

    public Rep(String businessObjectClassName, String attributeName)
    {
        this.businessObjectClassName = businessObjectClassName;
        this.attributeName = attributeName;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(businessObjectClassName);
        out.writeUTF(attributeName);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.businessObjectClassName = in.readUTF();
        this.attributeName = in.readUTF();
    }

    protected Object invokeStaticMethod(Class classToInvoke, String methodName) throws ClassNotFoundException
    {
        try
        {
            Method method = ReflectionMethodCache.getZeroArgMethod(classToInvoke, methodName);
            return method.invoke(null, (Object[]) null);
        }
        catch (Exception e)
        {
            throw new ClassNotFoundException("could not resolve attribute "+this.businessObjectClassName+" "+this.attributeName, e);
        }
    }
    public Object readResolve() throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        // 6 == "Finder".length()
        Class finderClass = Class.forName(this.businessObjectClassName+"Finder");

        return invokeStaticMethod(finderClass, this.attributeName);
    }
}
