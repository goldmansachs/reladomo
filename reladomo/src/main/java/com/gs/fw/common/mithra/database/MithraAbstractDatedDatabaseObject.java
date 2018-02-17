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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.database;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraDatedObjectFactory;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.MithraFastList;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.IOException;
import java.io.ObjectInput;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;



public abstract class MithraAbstractDatedDatabaseObject
        extends MithraAbstractDatabaseObject
        implements MithraCodeGeneratedDatedDatabaseObject,
        MithraDatedObjectFactory
{

    private AsOfAttribute[] asOfAttributes;

    protected MithraAbstractDatedDatabaseObject(String loggerClassName, String fullyQualifiedFinderClassName,
                int totalColumnsInResultSet, int totalColumnsInInsert, String columnListWithoutPK,
                String columnListWithoutPkWithAlias, boolean hasOptimisticLocking, boolean hasNullablePrimaryKeys,
                boolean hasSourceAttribute, String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        super(loggerClassName, fullyQualifiedFinderClassName, totalColumnsInResultSet, totalColumnsInInsert,
                columnListWithoutPK, columnListWithoutPkWithAlias, hasOptimisticLocking, hasNullablePrimaryKeys,
                hasSourceAttribute, primaryKeyWhereSqlWithDefaultAlias, primaryKeyIndexColumns);
    }

    protected AsOfAttribute[] getAsOfAttributes()
    {
        AsOfAttribute[] ofAttributes = this.asOfAttributes;
        if (ofAttributes == null)
        {
            ofAttributes = this.getFinder().getAsOfAttributes();
            asOfAttributes = ofAttributes;
        }
        return ofAttributes;
    }

    public List deserializeList(Operation op, ObjectInput in, boolean weak) throws IOException, ClassNotFoundException
    {
        Cache cache = this.getMithraObjectPortal().getCache();
        int size = in.readInt();
        FastList result = new FastList(size);
        Timestamp[] asOfDates = this.getAsOfDates();
        for (int i = 0; i < size; i++)
        {
            MithraDataObject data = this.deserializeFullData(in);
            deserializeAsOfAttributes(in, asOfDates);
            if (weak)
            {
                result.add(cache.getObjectFromDataWithoutCaching(data, asOfDates));
            }
            else
            {
                result.add(cache.getObjectFromData(data, asOfDates));
            }
        }
        return result;
    }

    public MithraDataObject refresh(MithraDataObject data, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        throw new RuntimeException("not implemented");
    }

    protected class DatedCursor extends DatabaseCursor
    {
        private Timestamp asOfDates[];
        private ObjectWithMapperStack asOfOpWithStacks[];

        public DatedCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean forceImplicitJoin)
        {
            super(analyzedOperation, postLoadFilter, orderby, rowcount, forceImplicitJoin);
            asOfDates = getAsOfDates();
            asOfOpWithStacks = getAsOfOpWithStacks(query, analyzedOperation);
        }

        protected Object getObject(ResultSet res, Object source) throws SQLException
        {
            MithraDataObject newData = inflateDataGenericSource(res, source, this.getDatabaseType());
            if (!this.matchesPostLoadOperation(newData))
            {
                return null;
            }
            inflateAsOfDatesGenericSource(newData, res, getTotalColumnsInResultSet() + 1, asOfDates, asOfOpWithStacks, source, this.getDatabaseType());
            return cache.getObjectFromDataWithoutCaching(newData, asOfDates);
        }

        protected String getStatement(DatabaseType dt, SqlQuery query, AnalyzedOperation analyzedOperation, int rowCount)
        {
            return MithraAbstractDatedDatabaseObject.this.findGetStatement(dt, query, analyzedOperation, rowCount);
        }
    }

    @Override
    public Cursor findCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        return new DatedCursor(analyzedOperation, postLoadFilter, orderby, rowcount, forceImplicitJoin);
    }

    protected boolean processResultSet(ResultSet res, MithraFastList result, Object source,
                                       ObjectWithMapperStack[] asOfOpWithStacks, Cache cache, DatabaseType dt, int rowcount, TimeZone timeZone)
            throws SQLException
    {
        boolean canUseManyCacheLookup = true;
        for (ObjectWithMapperStack asOfOpWithStack : asOfOpWithStacks)
        {
            // this can be null for the None operation
            if (asOfOpWithStack == null || ((AsOfOperation) asOfOpWithStack.getObject()).requiresResultSetToPopulate(asOfOpWithStack))
            {
                canUseManyCacheLookup = false;
                break;
            }
        }
        if (canUseManyCacheLookup && rowcount <= 0)
        {
            Object[] dataArray = getDataArray();
            int len = 0;
            while (res.next())
            {
                dataArray[len] = inflateDataGenericSource(res, source, dt);
                len++;
                if (len == DATA_ARRAY_SIZE)
                {
                    getManyObjects(cache, dataArray, len, result, asOfOpWithStacks);
                    len = 0;
                }
            }
            if (len > 0)
            {
                getManyObjects(cache, dataArray, len, result, asOfOpWithStacks);
            }
            returnDataArray(dataArray);
            return false;
        }
        else
        {
            return processResultSetOneByOne(res, result, source, asOfOpWithStacks, cache, dt, rowcount, timeZone);
        }
    }

    private void getManyObjects(Cache cache, Object[] dataArray, int len, MithraFastList result, ObjectWithMapperStack[] asOfOpWithStacks)
    {
        cache.getManyDatedObjectsFromData(dataArray, len, asOfOpWithStacks);
        result.zEnsureCapacity(result.size() + len);
        for (int i = 0; i < len; i++)
        {
            result.add(dataArray[i]);
        }
    }

    private boolean processResultSetOneByOne(ResultSet res, FastList result, Object source, ObjectWithMapperStack[] asOfOpWithStacks, Cache cache, DatabaseType dt, int rowcount, TimeZone timeZone)
            throws SQLException
    {
        Timestamp[] asOfDates = this.getAsOfDates();
        boolean reachedMaxRowCount = false;
        while (res.next())
        {
            MithraDataObject newData = inflateDataGenericSource(res, source, dt);
            inflateAsOfDatesGenericSource(newData, res, getTotalColumnsInResultSet() + 1, asOfDates, asOfOpWithStacks, dt, timeZone);
            result.add(cache.getObjectFromData(newData, asOfDates));
            if (rowcount > 0 && result.size() >= rowcount)
            {
                reachedMaxRowCount = true;
                break;
            }
        }
        return reachedMaxRowCount;
    }

    protected String findGetStatement(DatabaseType dt, SqlQuery query, AnalyzedOperation analyzedOperation, int rowCount)
    {
        return dt.getSelect(this.getColumnListWithPk(SqlQuery.DEFAULT_DATABASE_ALIAS) + query.getExtraColumns(),
                query, null, MithraManagerProvider.getMithraManager().isInTransaction(), rowCount);
    }

    public void analyzeChangeForReload(PrimaryKeyIndex fullUniqueIndex, MithraDataObject data, List newDataList, List updatedDataList)
    {
        MithraDataObject existingData = fullUniqueIndex == null ? null : (MithraDataObject) fullUniqueIndex.removeUsingUnderlying(data);
        if (existingData == null)
        {
            newDataList.add(data);
        }
        else
        {
            if (existingData.changed(data))
            {
                updatedDataList.add(data);
            }
        }
    }

    public void inflateAsOfDatesGenericSource(MithraDataObject data, ResultSet rs, int pos, Timestamp[] asOfDates,
                                              ObjectWithMapperStack[] asOfOpWithStacks, DatabaseType dt, TimeZone timesZone) throws SQLException
    {
        for (int i = 0; i < asOfOpWithStacks.length; i++)
        {
            AsOfOperation asOfOperation = (AsOfOperation) asOfOpWithStacks[i].getObject();
            pos += asOfOperation.populateAsOfDateFromResultSet(data, rs, pos, asOfDates, i, asOfOpWithStacks[i],
                    timesZone, dt);
        }
    }

    public void inflateAsOfDatesGenericSource(MithraDataObject data, ResultSet rs, int pos, Timestamp[] asOfDates,
                                              ObjectWithMapperStack[] asOfOpWithStacks, Object source, DatabaseType dt) throws SQLException
    {
        for (int i = 0; i < asOfOpWithStacks.length; i++)
        {
            AsOfOperation asOfOperation = (AsOfOperation) asOfOpWithStacks[i].getObject();
            pos += asOfOperation.populateAsOfDateFromResultSet(data, rs, pos, asOfDates, i, asOfOpWithStacks[i],
                    getDatabaseTimeZoneGenericSource(source), dt);
        }
    }

    public ObjectWithMapperStack[] getAsOfOpWithStacks(SqlQuery query, AnalyzedOperation analyzedOperation)
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        ObjectWithMapperStack[] asOfOpWithStacks = new ObjectWithMapperStack[asOfAttributes.length];

        if (!(analyzedOperation.getOriginalOperation().zIsNone()))
        {
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                asOfOpWithStacks[i] = analyzedOperation.getAsOfOperationForTopLevel(asOfAttributes[i]);
            }
        }
        return asOfOpWithStacks;
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject mithraObject, boolean lockInDatabase) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraDataObject data = mithraObject.zGetCurrentData();
        Attribute sourceAttribute = this.getFinder().getSourceAttribute();

        Object source = null;
        if (sourceAttribute != null)
        {
            source = sourceAttribute.valueOf(data);
        }
        String fromClause = this.getFullyQualifiedTableNameGenericSource(source) + " t0";

        StringBuffer whereClause = new StringBuffer(this.getSqlWhereClauseForRefresh(data));
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        whereClause.append(" and ");
        int count0 = asOfAttributes[0].appendWhereClauseForValue(asOfAttributes[0].timestampValueOf(mithraObject), whereClause);
        int count1 = 0;
        if (asOfAttributes.length > 1)
        {
            whereClause.append(" and ");
            count1 = asOfAttributes[1].appendWhereClauseForValue(asOfAttributes[1].timestampValueOf(mithraObject), whereClause);
        }
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        String statement = dt.getSelect(this.getColumnListWithoutPkWithAliasOrOne(), fromClause, whereClause.toString(), lockInDatabase);
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement stm = null;
        MithraDataObject refreshData = null;
        try
        {
            con = this.getConnectionForReadGenericSource(source, lockInDatabase);
            TimeZone databaseTimeZone = getDatabaseTimeZoneGenericSource(source);
            DatabaseType databaseType = getDatabaseTypeGenericSource(source);

            if (getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                int pos = this.setPrimaryKeyAttributesWithoutDates(pps, 1, data, databaseTimeZone, dt);
                for (int i = 0; i < count0; i++)
                {
                    asOfAttributes[0].getToAttribute().setSqlParameter(pos++, pps, asOfAttributes[0].timestampValueOf(mithraObject), databaseTimeZone, databaseType);
                }
                for (int i = 0; i < count1; i++)
                {
                    asOfAttributes[1].getToAttribute().setSqlParameter(pos++, pps, asOfAttributes[1].timestampValueOf(mithraObject), databaseTimeZone, databaseType);
                }
                if (sourceAttribute != null)
                {
                    getSqlLogger().debug("source '" + source + "': refresh with: " + pps.getPrintableStatement());
                }
                else
                {
                    getSqlLogger().debug("refresh with: " + pps.getPrintableStatement());
                }
            }
            stm = con.prepareStatement(statement);
            int pos = this.setPrimaryKeyAttributesWithoutDates(stm, 1, data, databaseTimeZone, dt);
            for (int i = 0; i < count0; i++)
            {
                asOfAttributes[0].getToAttribute().setSqlParameter(pos++, stm, asOfAttributes[0].timestampValueOf(mithraObject), databaseTimeZone, databaseType);
            }
            for (int i = 0; i < count1; i++)
            {
                asOfAttributes[1].getToAttribute().setSqlParameter(pos++, stm, asOfAttributes[1].timestampValueOf(mithraObject), databaseTimeZone, databaseType);
            }
            rs = stm.executeQuery();
            if (rs.next())
            {
                refreshData = data.copy(false);

                inflateNonPkDataGenericSource(refreshData, rs, source, dt);
            }
            if (refreshData != null && rs.next())
            {
                throw new MithraDatabaseException("the primary key for " + refreshData.getClass().getName() + " " + refreshData.zGetPrintablePrimaryKey() + " is not unique!");
            }
            rs.close();
            rs = null;
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            analyzeAndWrapSqlExceptionGenericSource("refresh failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            closeDatabaseObjects(con, stm, rs);
        }
        this.getPerformanceData().recordTimeForRefresh(startTime);
        return refreshData;
    }
}
