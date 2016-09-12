

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

package com.gs.fw.common.mithra.generator.dbgenerator;

import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.generator.AbstractMithraGenerator;
import org.apache.tools.ant.BuildException;

import java.util.List;

public class MithraDbDefinitionGenerator extends AbstractMithraGenerator
{
    private CoreMithraDbDefinitionGenerator coreDbDefinitionGenerator;

    public MithraDbDefinitionGenerator()
    {
        this(new CoreMithraDbDefinitionGenerator());
    }

    public MithraDbDefinitionGenerator(CoreMithraDbDefinitionGenerator gen)
    {
        super(gen);
        this.coreDbDefinitionGenerator = gen;
    }

    public String getDatabaseType()
    {
        return this.coreDbDefinitionGenerator.getDatabaseType();
    }

    public void setDatabaseType(String databaseType) throws BuildException
    {
        this.coreDbDefinitionGenerator.setDatabaseType(databaseType);
    }

    public void setBuildList(String buildList)
    {
        this.coreDbDefinitionGenerator.setBuildList(buildList);
    }

    public String getBuildList()
    {
        return this.coreDbDefinitionGenerator.getBuildList();
    }

    public List getBuildListArray()
    {
        return this.coreDbDefinitionGenerator.getBuildListArray();
    }

    public DatabaseType getDatabaseTypeObject()
    {
        return this.coreDbDefinitionGenerator.getDatabaseTypeObject();
    }

    public void execute() throws BuildException
    {
        this.coreDbDefinitionGenerator.execute();
    }

    public static void main(String[] args)
    {
        MithraDbDefinitionGenerator gen = new MithraDbDefinitionGenerator() {
            public void log(String s)
            {
                System.out.println(s);
            }

            public void log(String s, int i)
            {
                System.out.println(s);
            }
        };
        long startTime = System.currentTimeMillis();
        gen.setGeneratedDir("C:/users/proj/PARA4.0/generatedsrc/db");
        gen.setXml("C:/users/proj/PARA4.0/xml/mithra/para_config/ParaConfigClassList.xml");
        gen.setNonGeneratedDir("C:/users/proj/PARA4.0/src");
        gen.setDatabaseType("sybase");
        gen.execute();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
     }
}
