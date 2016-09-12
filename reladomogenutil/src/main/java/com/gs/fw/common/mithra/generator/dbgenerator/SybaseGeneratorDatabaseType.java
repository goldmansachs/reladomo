

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
import com.gs.fw.common.mithra.generator.Attribute;
import com.gs.fw.common.mithra.generator.Index;
import com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper;
import com.gs.fw.common.mithra.generator.databasetype.CommonDatabaseType;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class SybaseGeneratorDatabaseType extends AbstractGeneratorDatabaseType
{

    protected DatabaseType getDatabaseType()
    {
        return SybaseDatabaseType.getInstance();
    }

    @Override
    protected CommonDatabaseType getGeneratorDatabaseType()
    {
        return com.gs.fw.common.mithra.generator.databasetype.SybaseDatabaseType.getInstance();
    }

    public String getStatementTerminator()
    {
        return "\nGO";
    }

    public void generateDdlFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        String tableName = wrapper.getDefaultTable();
        PrintWriter writer = getDdlPrintWriter(wrapper, outDir);

        writer.println("if exists (select * from sysobjects where name = '" + tableName + "' and type='U')");
        writer.println("    drop table " + tableName);
        writer.println("GO");

        writer.println();

        writer.println("create table " + tableName);
        writer.println("(");

        Attribute[] attributes = wrapper.getAttributes();

        generateDdlColumnList(attributes, writer);

        writer.println(")");
        writer.println("GO");
        writer.println();

        writer.close();
    }

    public void generateIdxFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        PrintWriter writer = getIdxPrintWriter(wrapper, outDir);
        List indices = wrapper.getPrefixFreeIndices();
        String tableName = wrapper.getDefaultTable();

        for (int i = 0; i < indices.size(); i++)
        {
            Index index = (Index) indices.get(i);

            writer.println("if exists (select * from sysindexes where name = '" + index.getName() + "' and id = object_id('"
                    + tableName + "'))");
            writer.println("    drop index " + tableName + "." + index.getName());
            writer.println("GO");
            writer.println();

            writer.println("create " + (index.isUnique() ? "unique " : "") + "index " + index.getName() + " on " + tableName
                    + "(" + index.getIndexColumns() + ")");
            writer.println("GO");
            writer.println();
        }

        writer.close();
    }

    public void generateFkFile(MithraObjectTypeWrapper wrapper, File outDir) throws IOException
    {
        printFkFile(wrapper, outDir, this);
    }

    protected void generateNullStatement(PrintWriter writer, Attribute[] attributes, String attributeSqlType, int i)
    {
        writer.println("    " + attributes[i].getColumnName() + " " + attributeSqlType +
                    (attributes[i].isNullable() ? " null" : " not null") + ((i < attributes.length - 1) ? "," : ""));
    }

    @Override
    protected int getMaxConstraintLength()
    {
        return 255;
    }
}
