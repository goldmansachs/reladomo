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

import com.gs.fw.common.mithra.generator.Attribute;
import junit.framework.TestCase;

import java.io.PrintWriter;
import java.io.StringWriter;

public class GenerateNullStatementTest extends TestCase
{
    private static class ReservedWordAttribute extends Attribute
    {
        private ReservedWordAttribute()
        {
            super((com.gs.fw.common.mithra.generator.MithraObjectTypeWrapper) null);
        }

        @Override
        public String getColumnName()
        {
            return "\"ORDER\"";
        }

        @Override
        public String getColumnNameWithEscapedQuote()
        {
            return "\\\"ORDER\\\"";
        }

        @Override
        public boolean isNullable()
        {
            return false;
        }
    }

    private String generateNullStatement(AbstractGeneratorDatabaseType generator)
    {
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        Attribute[] attributes = { new ReservedWordAttribute() };
        generator.generateNullStatement(writer, attributes, "varchar(256)", 0);
        writer.flush();
        return sw.toString();
    }

    public void testPostgresDdlColumnDoesNotBackslashEscapeQuotes()
    {
        String ddl = generateNullStatement(new PostgresGeneratorDatabaseType());
        assertFalse("DDL column definition must not contain a backslash-escaped quote: " + ddl,
                ddl.contains("\\\""));
        assertTrue("DDL column definition should still quote the reserved word: " + ddl,
                ddl.contains("\"ORDER\""));
    }

    public void testSybaseDdlColumnDoesNotBackslashEscapeQuotes()
    {
        String ddl = generateNullStatement(new SybaseGeneratorDatabaseType());
        assertFalse("DDL column definition must not contain a backslash-escaped quote: " + ddl,
                ddl.contains("\\\""));
        assertTrue("DDL column definition should still quote the reserved word: " + ddl,
                ddl.contains("\"ORDER\""));
    }
}
