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

package com.gs.fw.common.mithra.database;

import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.TimeZone;



public interface MithraCodeGeneratedDatedDatabaseObject
        extends MithraCodeGeneratedDatabaseObject
{
    public String getAsOfAttributeWhereSql(MithraDataObject firstDataToUpdate);

    public abstract Timestamp[] getAsOfDates();

    public abstract ObjectWithMapperStack[] getAsOfOpWithStacks(SqlQuery query, AnalyzedOperation analyzedOperation);

    public int setPrimaryKeyAttributesWithoutDates(PreparedStatement stm, int pos, MithraDataObject dataObj,
            TimeZone databaseTimeZone, DatabaseType dt) throws SQLException;

}
