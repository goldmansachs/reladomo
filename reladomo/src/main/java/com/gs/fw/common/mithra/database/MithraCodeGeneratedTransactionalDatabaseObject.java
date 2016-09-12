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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;



public interface MithraCodeGeneratedTransactionalDatabaseObject
        extends MithraCodeGeneratedDatabaseObject
{
    public String getInsertFields();

    public String getInsertQuestionMarks();

    public void setInsertAttributes(PreparedStatement stm, MithraDataObject dataObj, TimeZone databaseTimeZone, int pos, DatabaseType dt) throws SQLException;

    public String getPrimaryKeyWhereSql();

    public String getOptimisticLockingWhereSql();

    public String getPkColumnList(String databaseAlias);

}
