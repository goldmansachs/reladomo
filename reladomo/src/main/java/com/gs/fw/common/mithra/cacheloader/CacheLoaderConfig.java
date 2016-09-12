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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.cacheloader.xmlbinding.*;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CacheLoaderConfig
{
    static private Logger logger = LoggerFactory.getLogger(CacheLoaderConfig.class);
    private CacheLoaderType cacheLoaderConfig;
    public static final String NO_SOURCE_ATTRIBUTES_TOKEN = "Global";

    public void parseConfiguration(InputStream configFile)
    {
        parseConfiguration(configFile, "");
    }

    public void parseConfiguration(InputStream configFile, String diagnosticMessage)
    {
        try
        {
            this.cacheLoaderConfig = new MithraCacheLoaderUnmarshaller().parse(configFile, diagnosticMessage);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Exceptions during parsing CacheLoader config file", e);
        }
        finally
        {
            try
            {
                configFile.close();
            }
            catch (Exception e)
            {
                logger.error("Could not close input stream", e);
            }
        }
    }

    public static Object newInstance(String className)
    {
        try
        {
            className = className.trim();
            int n = className.indexOf('(');
            if (n > 0 && className.charAt(className.length() - 1) == ')')
            {
                String justClassName = className.substring(0, n);
                String param = className.substring(n + 1, className.length() - 1);
                return Class.forName(justClassName).getConstructor(String.class).newInstance(param);
            }
            else
            {
                return Class.forName(className).newInstance();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("cannot create instance " + className, e);
        }
    }

    public ConfigValues readConfigValues()
    {
        return new ConfigValues(
                Integer.valueOf(this.cacheLoaderConfig.getReportedSlowSQLTime()),
                Integer.valueOf(this.cacheLoaderConfig.getReportedSlowSQLPerRowTime()),
                Boolean.valueOf(this.cacheLoaderConfig.getCaptureLoadingTaskDetails()),
                Integer.valueOf(this.cacheLoaderConfig.getThreadsPerDbServer()),
                Integer.valueOf(this.cacheLoaderConfig.getSyslogCheckThreshold()),
                Integer.valueOf(this.cacheLoaderConfig.getSyslogCheckWaitTime())
        );
    }

    public List<TopLevelLoaderFactory> readTopLevelLoaderFactories()
    {
        String defaultClassName = FullyMilestonedTopLevelLoaderFactory.class.getName();
        if (this.cacheLoaderConfig.getDefaultTopLevelLoaderFactory() != null)
        {
            defaultClassName = this.cacheLoaderConfig.getDefaultTopLevelLoaderFactory();
        }

        List<TopLevelLoaderFactory> factories = FastList.newList();
        for (TopLevelLoaderType each : cacheLoaderConfig.getTopLevelLoaders())
        {
            String classToLoad = each.getClassToLoad();
            try
            {
                String factoryClassName = each.getFactoryClass() != null ? each.getFactoryClass() : defaultClassName;
                TopLevelLoaderFactory loaderFactory = (TopLevelLoaderFactory) CacheLoaderConfig.newInstance(factoryClassName);
                loaderFactory.setClassToLoad(classToLoad);

                String sourceAttributeList = each.getSourceAttributes() == null ? NO_SOURCE_ATTRIBUTES_TOKEN : each.getSourceAttributes();
                loaderFactory.setSourceAttributes(FastList.newListWith(sourceAttributeList.split(",")));

                if (each.getPrerequisiteClasses() != null)
                {
                    loaderFactory.addPrerequisiteClassNames(FastList.newListWith(each.getPrerequisiteClasses().split(",")));
                }
                loaderFactory.setParams(this.readParams(each.getParams()));
                factories.add(loaderFactory);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Cannot read top level loader factory for " + each.getFactoryClass(), e);
            }
        }

        return factories;
    }

    public List<DependentLoaderFactory> readDependentLoaderFactories()
    {
        String defaultClassName = this.cacheLoaderConfig.getDefaultDependentLoaderFactory();
        if (defaultClassName == null)
            defaultClassName = DependentLoaderFactoryImpl.class.getName();

        String defaultHelperFactoryClassName = this.cacheLoaderConfig.getDefaultDependentLoaderHelperFactory();
        if (defaultHelperFactoryClassName == null)
            defaultHelperFactoryClassName = FullyMilestonedTopLevelLoaderFactory.class.getName();

        List<DependentLoaderFactory> factories = FastList.newList();

        for (DependentLoaderType each : cacheLoaderConfig.getDependentLoaders())
        {
            String factoryClassName = each.getFactoryClass();
            if (factoryClassName == null)
                factoryClassName = defaultClassName;
            DependentLoaderFactory loaderFactory = (DependentLoaderFactory) newInstance(factoryClassName);

            loaderFactory.setHelperFactory(this.buildHelperFactory(each, defaultHelperFactoryClassName));

            try
            {
                loaderFactory.setRelationship(each.getRelationship());
            }
            catch (Exception e)
            {
                throw new RuntimeException("Errors setting up relationship " + each.getRelationship(), e);
            }

            factories.add(loaderFactory);
            loaderFactory.setParams(this.readParams(each.getParams()));
        }

        return factories;
    }

    private AbstractLoaderFactory buildHelperFactory(DependentLoaderType each, String defaultClassName)
    {
        String helperFactoryName = each.getHelperFactoryClass();
        if (helperFactoryName == null)
            helperFactoryName = defaultClassName;

        AbstractLoaderFactory helperFactory = (AbstractLoaderFactory) newInstance(helperFactoryName);

        String sourceAttributeList = each.getSourceAttributes();
        if (sourceAttributeList != null)
        {
            helperFactory.setSourceAttributes(FastList.newListWith(sourceAttributeList.split(",")));
        }
        return helperFactory;
    }

    private List<ConfigParameter> readParams(List<ParamType> params)
    {
        List<ConfigParameter> list = FastList.newList();
        for (ParamType each : params)
        {
            list.add(new ConfigParameter(each.getName(), each.getValue()));
        }

        return list;
    }

    public static MithraRuntimeCacheController createRuntimeCacheController(String className)
    {
        if (className.endsWith("Impl"))
        {
            className = className.substring(0, className.length() - "Impl".length());
        }
        String finderName = className + "Finder";
        try
        {
            return new MithraRuntimeCacheController(Class.forName(finderName));
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("cannot load class " + finderName);
        }
    }

    public static List getNoSourceAttributeList()
    {
        return FastList.newListWith(NO_SOURCE_ATTRIBUTES_TOKEN);
    }

    public static boolean isSourceAttribute(Object sourceAttribute)
    {
        return !NO_SOURCE_ATTRIBUTES_TOKEN.equals(sourceAttribute);
    }
}
