
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

package com.gs.fw.common.mithra.generator;

import com.gs.fw.common.mithra.generator.metamodel.MithraInterfaceType;
import com.gs.fw.common.mithra.generator.util.AwaitingThreadExecutor;
import com.gs.fw.common.mithra.generator.util.ChopAndStickResource;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractMithraGenerator extends Task implements Logger, GenerationLogger
{
    public static final String DATED = BaseMithraGenerator.DATED;
    public static final String READ_ONLY = BaseMithraGenerator.READ_ONLY;
    public static final String DATED_READ_ONLY = BaseMithraGenerator.DATED_READ_ONLY;
    public static final String TRANSACTIONAL = BaseMithraGenerator.TRANSACTIONAL;
    public static final String DATED_TRANSACTIONAL = BaseMithraGenerator.DATED_TRANSACTIONAL;
    public static final String EMBEDDED_VALUE = BaseMithraGenerator.EMBEDDED_VALUE;
    public static final String ENUMERATION = BaseMithraGenerator.ENUMERATION;
    public static final String MITHRA_INTERFACE = BaseMithraGenerator.MITHRA_INTERFACE;

    private int logLevel = Project.MSG_WARN;
    protected GenerationLog oldGenerationLog;
    protected GenerationLog newGenerationLog;

    private StringBuilder errorLogs = new StringBuilder();
    protected BaseMithraGenerator baseGenerator;

    protected AbstractMithraGenerator()
    {
        this(new BaseMithraGenerator());
    }

    protected AbstractMithraGenerator(BaseMithraGenerator gen)
    {
        this.baseGenerator = gen;
        this.baseGenerator.setLogger(this);
        this.baseGenerator.setGenerationLogger(this);
    }

    public void setIgnorePackageNamingConvention(boolean ignorePackageNamingConvention)
    {
        this.baseGenerator.setIgnorePackageNamingConvention(ignorePackageNamingConvention);
    }

    public void setForceOffHeap(boolean forceOffHeap)
    {
        this.baseGenerator.setForceOffHeap(forceOffHeap);
    }

    protected String getMd5()
    {
        return this.baseGenerator.getMd5();
    }

    protected String getCrc()
    {
        return baseGenerator.getCrc();
    }

    protected List<MithraGeneratorImport> getImports() 
    {
        return this.baseGenerator.getImports();
    }

    public String getXml()
	{
		return this.baseGenerator.getXml();
	}

	public void setXml(String xml)
	{
		this.baseGenerator.setXml(xml);
	}

    @Override
    public GenerationLog getOldGenerationLog()
    {
        return this.oldGenerationLog;
    }

    @Override
    public GenerationLog getNewGenerationLog()
    {
        return this.newGenerationLog;
    }

    @Override
    public void setOldGenerationLog(GenerationLog log)
    {
        this.oldGenerationLog = log;
    }

    @Override
    public void setNewGenerationLog(GenerationLog log)
    {
        this.newGenerationLog = log;
    }

    public int getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(int logLevel)
    {
        this.logLevel = logLevel;
    }

    public String getGeneratedDir()
	{
		return this.baseGenerator.getGeneratedDir();
	}

	public void setGeneratedDir(String generatedDir)
	{
		this.baseGenerator.setGeneratedDir(generatedDir);
	}

    public String getNonGeneratedDir()
    {
        return this.baseGenerator.getNonGeneratedDir();
    }

    public void setNonGeneratedDir(String nonGeneratedDir)
    {
        this.baseGenerator.setNonGeneratedDir(nonGeneratedDir);
    }

    public void setIgnoreNonGeneratedAbstractClasses(boolean ignoreNonGeneratedAbstractClasses)
    {
        this.baseGenerator.setIgnoreNonGeneratedAbstractClasses(ignoreNonGeneratedAbstractClasses);
    }

    public void setIgnoreTransactionalMethods(boolean ignoreTransactionalMethods)
    {
        this.baseGenerator.setIgnoreTransactionalMethods(ignoreTransactionalMethods);
    }

    public boolean isDefaultFinalGetters()
    {
        return this.baseGenerator.isDefaultFinalGetters();
    }

    public void setDefaultFinalGetters(boolean defaultFinalGetters)
    {
        this.baseGenerator.setDefaultFinalGetters(defaultFinalGetters);
    }

    public Map<String, MithraObjectTypeWrapper> getMithraObjects()
    {
        return this.baseGenerator.getMithraObjects();
    }

    public Map<String, MithraEmbeddedValueObjectTypeWrapper> getMithraEmbeddedValueObjects()
    {
        return this.baseGenerator.getMithraEmbeddedValueObjects();
    }

    public Map<String, MithraEnumerationTypeWrapper> getMithraEnumerations()
    {
        return this.baseGenerator.getMithraEnumerations();
    }

    public Map<String, MithraInterfaceType> getMithraInterfaces()
    {
        return this.baseGenerator.getMithraInterfaces();
    }

    protected boolean extractMithraInterfaceRelationshipsAndSuperInterfaces()
    {
        return this.baseGenerator.extractMithraInterfaceRelationshipsAndSuperInterfaces();
    }

    protected void validateXml()
    {
        boolean mithraInterfaceSuccess = this.extractMithraInterfaceRelationshipsAndSuperInterfaces();
        boolean mithraEmbeddedValueObjectXmlSuccess = this.baseGenerator.validateMithraEmbeddedValueObjectXml();
        boolean mithraObjectXmlSuccess = this.baseGenerator.validateMithraObjectXml();
        if (!(mithraInterfaceSuccess && mithraEmbeddedValueObjectXmlSuccess && mithraObjectXmlSuccess))
        {
            String errors = errorLogs.toString();
            errorLogs.setLength(0);
            throw new BuildException("One or more error(s) while validating mithra xml\n"+ errors);
        }
    }

    public MithraGeneratorImport createMithraImport()
    {
        return new MithraGeneratorImport();
    }

    public void addConfiguredMithraImport(MithraGeneratorImport importElement)
    {
        this.baseGenerator.addConfiguredMithraImport(importElement);
    }

    public abstract void execute() throws BuildException;

    public void log(String s, int level)
    {
        super.log(s, level);
        if (level == Project.MSG_ERR)
        {
            this.errorLogs.append(s).append("\n");
        }
    }

    private void logForLevel(String msg, int level)
    {
        if (this.logLevel >= level)
        {
            this.log(msg, level);
        }
    }

    private void logForLevel(String msg, Throwable t, int level)
    {
        if (this.logLevel >= level)
        {
            this.log(msg+": "+t.getClass().getName()+": "+t.getMessage(), level);
        }
    }

    public void info(String msg)
    {
        logForLevel(msg, Project.MSG_INFO);
    }

    public void info(String msg, Throwable t)
    {
        logForLevel(msg, t, Project.MSG_INFO);
    }

    public void warn(String msg)
    {
        logForLevel(msg, Project.MSG_WARN);
    }

    public void warn(String msg, Throwable t)
    {
        logForLevel(msg, t, Project.MSG_WARN);
    }

    public void error(String msg)
    {
        logForLevel(msg, Project.MSG_ERR);
    }

    public void error(String msg, Throwable t)
    {
        logForLevel(msg, t, Project.MSG_ERR);
    }

    public void debug(String msg)
    {
        logForLevel(msg, Project.MSG_DEBUG);
    }

    public void debug(String msg, Throwable t)
    {
        logForLevel(msg, t, Project.MSG_DEBUG);
    }

    public StringBuilder getErrorLogs()
    {
        return errorLogs;
    }

    public ChopAndStickResource getChopAndStickResource()
    {
        return this.baseGenerator.getChopAndStickResource();
    }

    public AwaitingThreadExecutor getExecutor()
    {
        return this.baseGenerator.getExecutor();
    }

    protected abstract class GeneratorTask implements Runnable
    {
        private int resourceNumber;

        protected GeneratorTask(int resourceNumber)
        {
            this.resourceNumber = resourceNumber;
        }

        public void acquireSerialResource()
        {
            AbstractMithraGenerator.this.getChopAndStickResource().acquireSerialResource(resourceNumber);
        }

        public void releaseSerialResource()
        {
            AbstractMithraGenerator.this.getChopAndStickResource().releaseSerialResource();
        }
    }
}