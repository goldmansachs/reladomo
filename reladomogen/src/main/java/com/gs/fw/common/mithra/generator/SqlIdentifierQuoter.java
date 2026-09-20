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
//Portions copyright Zeyt Ates. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.generator;

/**
 * decides whether a SQL name (table or column) needs to be wrapped in double
 * quotes and wraps it
 *
 * a name needs quotes when it has a space in it or when it is the same as a
 * reserved SQL word. names the user has already wrapped themselves with an
 * escaped quote or with brackets as MS SQL uses are left as they are
 */
public final class SqlIdentifierQuoter
{
    private SqlIdentifierQuoter()
    {
    }

    public static boolean requiresQuotes(String identifier)
    {
        return identifier != null
                && !identifier.startsWith("\\")
                && !identifier.startsWith("[")
                && (identifier.contains(" ") || isSqlKeyword(identifier));
    }

    private static boolean isSqlKeyword(String identifier)
    {
        return SqlKeywords.isKeyword(identifier.toUpperCase());
    }

    public static String quote(String identifier)
    {
        if (requiresQuotes(identifier))
        {
            return "\"" + identifier + "\"";
        }
        return identifier;
    }

    public static String quoteTableName(String tableName)
    {
        if (requiresQuotes(tableName) && isUpperCase(tableName))
        {
            return "\"" + tableName + "\"";
        }
        return tableName;
    }

    private static boolean isUpperCase(String identifier)
    {
        return identifier.equals(identifier.toUpperCase());
    }
}