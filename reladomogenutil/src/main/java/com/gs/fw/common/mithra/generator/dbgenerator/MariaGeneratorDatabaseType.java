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
import com.gs.fw.common.mithra.databasetype.MariaDatabaseType;
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.Index;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class MariaGeneratorDatabaseType extends AbstractGeneratorDatabaseType
{
    private static final int MARIA_MAX_INDEX_NAME = 64;

    @Override
    protected DatabaseType getDatabaseType()
    {
        return MariaDatabaseType.getInstance();
    }

    @Override
    protected CommonDatabaseType getGeneratorDatabaseType()
    {
        return com.gs.fw.common.mithra.generator.databasetype.MariaDatabaseType.getInstance();
    }

    @Override
    public void generateDdlFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        String tableName = wrapper.getDefaultTable();
        PrintWriter writer = getDdlPrintWriter(wrapper, outDir);

        writer.println("drop table if exists " + tableName + ";");
        writer.println();
        writer.println("create table " + tableName);
        writer.println("(");

        Attribute[] attributes = wrapper.getAttributes();

        generateDdlColumnList(attributes, writer);

        writer.println(");");
        writer.println();

        writer.close();
    }

    @Override
    public void generateIdxFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        PrintWriter writer = getIdxPrintWriter(wrapper, outDir);
        List indices = wrapper.getPrefixFreeIndices();
        String tableName = wrapper.getDefaultTable();

        boolean firstPk = true;

        for (int i = 0; i < indices.size(); i++)
        {
            Index index = (Index) indices.get(i);

            if (index.isPk() && firstPk)
            {
                writer.println("alter table " + tableName + " add constraint "+getFixedIndexName(index)+" primary key ("
                        + index.getIndexColumns() + ");");
                firstPk = false;
            }
            else
            {
                writer.println("drop index if exists " + getFixedIndexName(index) + ";");

                writer.println("create " + (index.isUnique() ? "unique " : "") + "index " + getFixedIndexName(index) + " on " + tableName
                        + "(" + index.getIndexColumns() + ");");
            }
            writer.println();
        }
        writer.close();
    }

    private String getFixedIndexName(Index index)
    {
        String name = index.getName();
        if (name.length() > MARIA_MAX_INDEX_NAME)
        {
            int targetLength = MARIA_MAX_INDEX_NAME;
            int idxIndex = name.toUpperCase().lastIndexOf("_IDX");
            if (idxIndex <=0 ) idxIndex = name.toUpperCase().lastIndexOf("_PK");
            if (idxIndex > 0)
            {
                name = name.substring(0, idxIndex);
                targetLength = MARIA_MAX_INDEX_NAME - (index.getName().length() - idxIndex);
            }
            StringBuilder builder = fixStringLength(name, targetLength);
            if (idxIndex > 0)
            {
                builder.append(index.getName().substring(idxIndex));
            }
            name = builder.toString();
        }
        return name;
    }

    @Override
    public void generateFkFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        printFkFile(wrapper, outDir, this);
    }

    @Override
    public String getStatementTerminator()
    {
        return ";";
    }

    @Override
    protected int getMaxConstraintLength()
    {
        return MARIA_MAX_INDEX_NAME;
    }
}
