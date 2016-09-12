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

package com.gs.fw.common.mithra.test;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.LoadOperationProvider;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.mithraruntime.*;
import junit.framework.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

public class MithraRuntimeConfigVerifier
{
    private static final Class[] NO_PARAMS = {};

    private static final Class[] GET_INSTANCE_PARAMETER_TYPES = new Class[] { Properties.class };

    private MithraRuntime mithraRuntime;

    public MithraRuntimeConfigVerifier(String filename) throws IOException
    {
        MithraRuntimeUnmarshaller unmarshaller = new MithraRuntimeUnmarshaller();
        this.mithraRuntime = unmarshaller.parse(filename);
    }
    
    public MithraRuntimeConfigVerifier(InputStream configStream, String diagnosticMessage) throws IOException
    {
        MithraRuntimeUnmarshaller unmarshaller = new MithraRuntimeUnmarshaller();
        this.mithraRuntime = unmarshaller.parse(configStream, diagnosticMessage);
    }
    
    public void verifyClasses()
    {
        List<String> errors = FastList.newList();
        this.verifyConnectionManagers(this.mithraRuntime.getConnectionManagers(), errors);

        if (this.mithraRuntime.getPureObjects() != null)
        {
            this.verifyPureObjects(this.mithraRuntime.getPureObjects().getMithraObjectConfigurations(), errors);
        }
        
        this.verifyRemoteServers(this.mithraRuntime.getRemoteServers(), errors);
        if (errors.size() > 0)
        {
            StringBuilder allErrors = new StringBuilder(errors.size() * 20);
            for(String error: errors)
            {
                allErrors.append('\n').append(error);
            }
            Assert.fail("Runtime configuration failed with the following errors:"+allErrors);
        }
    }

    private void verifyRemoteServers(List<RemoteServerType> remoteServers, List<String> errors)
    {
        for(RemoteServerType remoteServer: remoteServers)
        {
            loadClass(remoteServer.getClassName(), errors, "RemoteServer");
            verifyHasGetInstance(remoteServer.getClassName(), errors, "RemoteServer", false);
            verifyMithraObjectConfigs(remoteServer.getMithraObjectConfigurations(), errors);
        }
    }

    private void verifyPureObjects(List<MithraPureObjectConfigurationType> pureObjects, List<String> errors)
    {
        for(MithraPureObjectConfigurationType pureObject: pureObjects)
        {
            loadClass(pureObject.getClassName(), errors, "MithraPureObject");
        }
    }

    private void verifyConnectionManagers(List<ConnectionManagerType> connectionManagers, List<String> errors)
    {
        for(ConnectionManagerType connectionManagerType: connectionManagers)
        {
            if (this.verifyClassExists(connectionManagerType.getClassName(), errors, "Connection Manager"))
            {
                this.verifyHasGetInstance(connectionManagerType.getClassName(), errors, "Connection Manager", connectionManagerType.getProperties().isEmpty());
            }
            this.verifyClassExistsWithEmptyConstructor(connectionManagerType.getLoadOperationProvider(), LoadOperationProvider.class, errors, "Connection Manager Load Operation Provider");
            this.verifyMithraObjectConfigs(connectionManagerType.getMithraObjectConfigurations(), errors);
            this.verifyTempObjectConfigs(connectionManagerType.getMithraTemporaryObjectConfigurations(), errors);
            this.verifySchemas(connectionManagerType.getSchemas(), errors);
        }
    }

    private void verifySchemas(List<SchemaType> schemas, List<String> errors)
    {
        for(SchemaType schema: schemas)
        {
            verifyMithraObjectConfigs(schema.getMithraObjectConfigurations(), errors);
        }
    }

    private void verifyTempObjectConfigs(List<MithraTemporaryObjectConfigurationType> temporaryObjectConfigs, List<String> errors)
    {
        for(MithraTemporaryObjectConfigurationType tempConfig: temporaryObjectConfigs)
        {
            loadClass(tempConfig.getClassName(), errors, "MithraTempObject");
        }
    }

    private void verifyMithraObjectConfigs(List<MithraObjectConfigurationType> mithraObjectConfigurations, List<String> errors)
    {
        for(MithraObjectConfigurationType mithraObjectType : mithraObjectConfigurations)
        {
            loadClass(mithraObjectType.getClassName(), errors, "MithraObject");
            if (mithraObjectType.getFinalRelationshipCacheTimeToLive(mithraRuntime) != 0 && mithraObjectType.getFinalCacheTimeToLive(mithraRuntime) == 0)
            {
                errors.add("relationshipCacheTimeToLive cannot be set without cacheTimeToLive being set for object " + mithraObjectType.getClassName());
            }
            this.verifyClassExistsWithEmptyConstructor(mithraObjectType.getLoadOperationProvider(), LoadOperationProvider.class, errors, mithraObjectType.getClassName()+" Load Operation Provider");
        }
    }

    private void verifyHasGetInstance(String className, List<String> errors, String msg, boolean noArgs)
    {
        Class c = loadClass(className, errors, msg);
        if (noArgs)
        {
            Method method = getMethodByReflection(c, "getInstance", NO_PARAMS);
            if (method == null)
            {
                errors.add(msg+" does not a have zero-argument getInstance() method");
            }
        }
        else
        {
            Method method = getMethodByReflection(c, "getInstance", GET_INSTANCE_PARAMETER_TYPES);
            if (method == null)
            {
                errors.add(msg+" does not a have getInstance(Properties p) method");
            }
        }
    }

    private boolean verifyClassExistsWithEmptyConstructor(String className, Class interfaceClass, List<String> errors, String msg)
    {
        if (className == null) return true;
        Class c = loadClass(className, errors, msg);
        if (c == null) return false;
        try
        {
            c.getConstructor(NO_PARAMS);
        }
        catch (NoSuchMethodException e)
        {
            errors.add(msg+" "+className+" does not have a zero argument constructor "+e.getMessage());
            return false;
        }
        if (!interfaceClass.isAssignableFrom(c))
        {
            errors.add(msg+" "+className+" does not implement "+interfaceClass.getName());
            return false;
        }
        return true;
    }

    private boolean verifyClassExists(String className, List<String> errors, String diagnosticMsg)
    {
        return loadClass(className, errors, diagnosticMsg) != null;
    }

    private Method getMethodByReflection(Class clazz, String methodName, Class[] params)
    {
        Method method = null;
        try
        {
            method = clazz.getMethod(methodName, params);
        }
        catch (NoSuchMethodException e)
        {
            //ignore
        }
        return method;
    }

    private Class loadClass(String className, List<String> errors, String msg)
    {
        Class c = null;
        try
        {
            c =  MithraManager.class.getClassLoader().loadClass(className); //IMPORTANT: loadClass does not invoke static initializers

        }
        catch (ClassNotFoundException e)
        {
            errors.add(msg+ " class not found: "+className);
        }
        return c;
    }

}
