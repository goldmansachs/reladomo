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
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.databasetype.Udb82DatabaseType;
import com.gs.fw.common.mithra.generator.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CoreMithraDbDefinitionGenerator extends BaseMithraGenerator
{
    private String databaseType;

    private boolean executed = false;
    private boolean generateConcreteClasses = true;

    private String buildList;
    private ArrayList buildListArray = null;

    private AbstractGeneratorDatabaseType abstractGeneratorDatabaseType;

    private final String DB_NAME_SYBASE = "sybase";
    private final String DB_NAME_UDB82 = "udb82";
    private final String DB_NAME_MSSQL = "mssql";
    private final String DB_NAME_POSTGRES = "postgres";
    private final String DB_NAME_ORACLE = "oracle";

    public String getDatabaseType()
    {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) throws MithraGeneratorException
    {
        this.databaseType = databaseType;

        if (databaseType == null)
        {
            this.logger.error("No database type specified.");
            throw new MithraGeneratorException(new Exception("No database type specified."));
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_SYBASE) || databaseType.equalsIgnoreCase(DB_NAME_MSSQL)) // same syntax
        {
            abstractGeneratorDatabaseType = new SybaseGeneratorDatabaseType();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_UDB82))
        {
            abstractGeneratorDatabaseType = new Udb82GeneratorDatabaseType();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_POSTGRES))
        {
            abstractGeneratorDatabaseType = new PostgresGeneratorDatabaseType();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_ORACLE))
        {
            abstractGeneratorDatabaseType = new OracleGeneratorDatabaseType();
        }
        else
        {
            this.logger.error("No database type specified.");
            throw new MithraGeneratorException(new Exception("Invalid database type specified."));
        }
    }

    public void setBuildList(String buildList)
    {
        this.buildList = buildList;

        if (buildList != null)
        {
            String[] buildListString = buildList.split(",");
            this.buildListArray = new ArrayList();

            for (int i = 0; i < buildListString.length; i++)
            {
                buildListString[i] = buildListString[i].trim();
                this.buildListArray.add(buildListString[i]);
            }
        }
        else
        {
            buildListArray = null;
        }
    }

    public String getBuildList()
    {
        return this.buildList;
    }

    public List getBuildListArray()
    {
        return buildListArray;
    }

    public DatabaseType getDatabaseTypeObject()
    {
        if (this.databaseType.equalsIgnoreCase(DB_NAME_SYBASE))
        {
            return SybaseDatabaseType.getInstance();
        }
        else if (this.databaseType.equalsIgnoreCase(DB_NAME_UDB82))
        {
            return Udb82DatabaseType.getInstance();
        }

        return null;
    }

    public boolean generateDbDefinition(MithraObjectTypeWrapper wrapper, boolean replaceIfExists) throws MithraGeneratorException
    {
        try
        {
            String targetDir = replaceIfExists ? this.getGeneratedDir() : this.getNonGeneratedDir();
            File outDir = new File(targetDir);


            boolean outFileExists = false;// outDdl.exists() || outIdx.exists();

            if (!replaceIfExists)
            {
                return false;
            }

            if (!replaceIfExists && !generateConcreteClasses)
            {
                this.logger.warn("does not exist and generateConcreteClasses flag is turned off. This might lead to compilation errors");
                return false;
            }

            if (outFileExists)
            {
                if (this.getGenerationLogger().getNewGenerationLog().isSame(this.getGenerationLogger().getOldGenerationLog()))
                {
                    this.logger.debug("skipping " + wrapper.getDefaultTable());
                    return false;
                }
            }

            outDir.mkdirs();

            abstractGeneratorDatabaseType.generateDdlFile(wrapper, outDir);
            abstractGeneratorDatabaseType.generateIdxFile(wrapper, outDir);
            abstractGeneratorDatabaseType.generateFkFile(wrapper, outDir);

            return true;
        }
        catch (IOException e)
        {
            throw new MithraGeneratorException(e);
        }
    }

    public void execute() throws MithraGeneratorException
    {
        if (!executed)
        {
            try
            {
                parseAndValidate();
                for (MithraObjectTypeWrapper mithraObjectTypeWrapper: getSortedMithraObjects())
                {
                    /* Check for wrapper in build list, if specified (build list is comma-separated class names) */
                    if ((buildListArray != null) && (!buildListArray.contains(mithraObjectTypeWrapper.getClassName())))
                    {
                        continue;
                    }

                    /* Check if wrapper is abstract */
                    if (mithraObjectTypeWrapper.getDefaultTable() == null)
                    {
                        continue;
                    }

                    generateDbDefinition(mithraObjectTypeWrapper, true);
                }

                executed = true;
            }
            catch (Exception e)
            {
                throw new MithraGeneratorException("Exception in mithra code generation", e);
            }
        }
        else
        {
            this.logger.info("skipped");
        }
    }


    public static void main(String[] args)
    {
        CoreMithraDbDefinitionGenerator gen = new CoreMithraDbDefinitionGenerator();
        gen.setLogger(new StdOutLogger());

        long startTime = System.currentTimeMillis();
        gen.setGeneratedDir("C:/users/proj/PARA4.0/generatedsrc/db");
        gen.setXml("C:/users/proj/PARA4.0/xml/mithra/para_config/ParaConfigClassList.xml");
        gen.setNonGeneratedDir("C:/users/proj/PARA4.0/src");
        gen.setDatabaseType("sybase");
        gen.execute();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
    }
}
