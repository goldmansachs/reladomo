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

package com.gs.fw.common.mithra.remote;


import com.gs.fw.common.mithra.MithraDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MithraSerialUtil
{
    static private Logger logger = LoggerFactory.getLogger(MithraSerialUtil.class.getName());

    public static Class getDataClassToInstantiate(String dataClassName) throws IOException
    {
        try
        {
            return getDataClassWithException(dataClassName);
        }
        catch (ClassNotFoundException e)
        {
            logger.error("unexpected exception", e);
            IOException ioException = new IOException("could not find data class for "+dataClassName);
            ioException.initCause(e);
            throw ioException;
        }
    }

    public static Class getDataClassWithException(String dataClassName) throws ClassNotFoundException
    {
        Class c = Class.forName(dataClassName);
        if (c.isInterface())
        {
            // has both on/off heap
            c = Class.forName(c.getName()+"$"+c.getSimpleName()+"OnHeap");
        }
        return c;
    }

    public static String getDataClassNameToSerialize(MithraDataObject data)
    {
        return data.zGetSerializationClassName();
    }

    public static MithraDataObject instantiateData(Class dataClass) throws IOException
    {
        Exception problem = null;
        try
        {
            return (MithraDataObject) dataClass.newInstance();
        }
        catch (InstantiationException e)
        {
            problem = e;
        }
        catch (IllegalAccessException e)
        {
            problem = e;
        }
        logger.error("unexpected exception", problem);
        IOException ioException = new IOException("could not instantiate "+dataClass.getName());
        ioException.initCause(problem);
        throw ioException;
    }

    public static Class getDataClassForFinder(String finderClassname) throws ClassNotFoundException
    {
        String className = finderClassname.substring(0, finderClassname.length() - "Finder".length());
        return getDataClassWithException(className+"Data");
    }

    public static Object safeInstantiate(String className) throws IOException
    {
        Exception problem = null;
        try
        {
            return Class.forName(className).newInstance();
        }
        catch (InstantiationException e)
        {
            problem = e;
        }
        catch (IllegalAccessException e)
        {
            problem = e;
        }
        catch (ClassNotFoundException e)
        {
            problem = e;
        }
        logger.error("unexpected exception", problem);
        IOException ioException = new IOException("could not instantiate "+className);
        ioException.initCause(problem);
        throw ioException;
    }
}
