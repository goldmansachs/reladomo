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

package com.gs.fw.common.mithra.connectionmanager;


import com.gs.fw.common.mithra.util.HashUtil;

public class StatementKey
{
    private final String sql;
    private final String catalog;

    public StatementKey(String sql, String catalog)
    {
        this.sql = sql;
        this.catalog = catalog;
    }

    public boolean equals(Object that)
    {
        StatementKey key = (StatementKey) that;
        return sql.equals(key.sql)
                && (catalog == key.catalog || (catalog != null && catalog.equals(key.catalog)));
    }

    public String getSql()
    {
        return sql;
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(sql.hashCode(), HashUtil.hash(catalog));
    }
}