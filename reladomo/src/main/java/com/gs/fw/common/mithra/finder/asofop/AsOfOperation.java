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

package com.gs.fw.common.mithra.finder.asofop;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.AtomicOperation;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.Operation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;



public interface AsOfOperation
{

    public void generateSql(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack);

    public int getClauseCount(SqlQuery query);

    public int populateAsOfDateFromResultSet(MithraDataObject inflatedData, ResultSet rs, int resultSetPosition,
            Timestamp[] asOfDates, int asOfDatePosition, ObjectWithMapperStack asOfOperationStack,
            TimeZone databaseTimeZone, DatabaseType dt) throws SQLException ;

    public Timestamp inflateAsOfDate(MithraDataObject inflatedData);

    public boolean addsToAsOfOperationWhereClause(ObjectWithMapperStack asOfAttributeWithMapperStack, ObjectWithMapperStack asOfOperationStack);

    public boolean requiresResultSetToPopulate(ObjectWithMapperStack asOfOperationStack);

    public AtomicOperation createAsOfOperationCopy(TemporalAttribute rightAttribute, Operation op);

    public int zGetAsOfOperationPriority();
}
