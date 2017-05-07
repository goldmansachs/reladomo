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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;
import com.gs.fw.common.mithra.util.MithraFastList;

import java.util.List;
import java.util.TimeZone;
import java.sql.*;



public abstract class MithraAbstractDatedTransactionalDatabaseObject
        extends MithraAbstractDatedDatabaseObject
        implements MithraDatedTransactionalDatabaseObject
{
    protected MithraAbstractDatedTransactionalDatabaseObject(String loggerClassName, String fullyQualifiedFinderClassName,
            int totalColumnsInResultSet, int totalColumnsInInsert, String columnListWithoutPK, String columnListWithoutPkWithAlias,
            boolean hasOptimisticLocking, boolean hasNullablePrimaryKeys, boolean hasSourceAttribute,
            String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        super(loggerClassName, fullyQualifiedFinderClassName, totalColumnsInResultSet, totalColumnsInInsert, columnListWithoutPK,
                columnListWithoutPkWithAlias, hasOptimisticLocking, hasNullablePrimaryKeys, hasSourceAttribute,
                primaryKeyWhereSqlWithDefaultAlias, primaryKeyIndexColumns);
    }

    public void batchDelete(List mithraObjects) throws MithraDatabaseException
    {
        this.zBatchDelete(mithraObjects, true);
    }

    @Override
    public void batchDeleteQuietly(List mithraObjects) throws MithraDatabaseException
    {
        this.zBatchDelete(mithraObjects, false);
    }

    public void update(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper)
    throws MithraDatabaseException
    {
        this.zUpdate(mithraObject, wrapper);
    }

    public void insert(MithraDataObject dataToInsert) throws MithraDatabaseException
    {
        this.zInsert(dataToInsert);
    }

    public void delete(MithraDataObject dataToDelete) throws MithraDatabaseException
    {
        this.zDelete(dataToDelete);
    }

    public void batchUpdate(BatchUpdateOperation batchUpdateOperation)
    {
        this.zBatchUpdate(batchUpdateOperation);
    }

    public void batchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException
    {
        this.zBatchInsert(mithraObjects, bulkInsertThreshold);
    }

    public void update(MithraTransactionalObject mithraObject, List updateWrappers)
    throws MithraDatabaseException
    {
        this.zUpdate(mithraObject, updateWrappers);
    }

    public void deleteUsingOperation(Operation op)
    {
        this.zDeleteUsingOperation(op);
    }

    public int deleteBatchUsingOperation(Operation op, int batchSize)
    {
        return this.zDeleteUsingOperation(op, batchSize);
    }

    public void multiUpdate(MultiUpdateOperation multiUpdateOperation)
    {
        this.zMultiUpdate(multiUpdateOperation);
    }

    public MithraDataObject enrollDatedObject(MithraDatedTransactionalObject mithraObject)
    {
        // delegate to the portal to keep the retrieval count consistent
        MithraObjectPortal portal = mithraObject.zGetPortal();
        return portal.refreshDatedObject(mithraObject, portal.getTxParticipationMode().mustLockOnRead());
    }

    public List getForDateRange(MithraDataObject obj, Timestamp start, Timestamp end, AsOfAttribute businessDate,
            AsOfAttribute processingDate) throws MithraDatabaseException
    {
        Attribute sourceAttribute = this.getFinder().getSourceAttribute();

        Object source = null;
        if (sourceAttribute != null)
        {
            source = sourceAttribute.valueOf(obj);
        }
        String fromClause = this.getFullyQualifiedTableNameGenericSource(source)+" t0";

        StringBuffer whereClause = new StringBuffer(this.getSqlWhereClauseForRefresh(obj));

        whereClause.append(" and ");
        int rangeParamNum = businessDate.appendWhereClauseForRange(start, end, whereClause);
        if (processingDate != null)
        {
            whereClause.append(" and ");
            processingDate.appendInfinityWhereClause(whereClause);
        }
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        String statement = dt.getSelect(this.getColumnListWithoutPkWithAliasOrOne(),
            fromClause, whereClause.toString(), obj.zGetMithraObjectPortal().getTxParticipationMode().mustLockOnRead());
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement stm = null;
        List result = new MithraFastList(3);
        Cache cache = this.getFinder().getMithraObjectPortal().getCache();
        TimeZone databaseTimeZone = null;
        DatabaseType databaseType = null;

        try
        {
            con = this.getConnectionForReadGenericSource(source, obj.zGetMithraObjectPortal().getTxParticipationMode().mustLockOnRead());
            databaseTimeZone = getDatabaseTimeZoneGenericSource(source);
            databaseType = getDatabaseTypeGenericSource(source);

            if(this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                int pos = 1;
                pos = this.setPrimaryKeyAttributesWithoutDates(pps, 1, obj, databaseTimeZone, dt);

                if (rangeParamNum > 1)
                {
                    businessDate.getToAttribute().setSqlParameter(pos++, pps, end, databaseTimeZone, databaseType);
                }
                businessDate.getToAttribute().setSqlParameter(pos++, pps, start, databaseTimeZone, databaseType);

                if (processingDate != null && !processingDate.isInfinityNull())
                {
                    processingDate.getToAttribute().setSqlParameter(pos, pps, processingDate.getInfinityDate(), databaseTimeZone, databaseType);
                }
                if (sourceAttribute != null)
                {
                    getSqlLogger().debug("source '" + source + "': find datarange with: " + pps.getPrintableStatement());
                }
                else
                {
                    getSqlLogger().debug("find datarange with: " + pps.getPrintableStatement());
                }
            }
            stm = con.prepareStatement(statement);
            int pos = 1;
            pos = this.setPrimaryKeyAttributesWithoutDates(stm, 1, obj, databaseTimeZone, dt);

            // to do: question about these below line.  how do we find out what type of database conversion is needed?
            if (rangeParamNum > 1)
            {
                businessDate.getToAttribute().setSqlParameter(pos++, stm, end, databaseTimeZone, databaseType);
            }
            businessDate.getToAttribute().setSqlParameter(pos++, stm, start, databaseTimeZone, databaseType);

            if (processingDate != null && !processingDate.isInfinityNull())
            {
                processingDate.getToAttribute().setSqlParameter(pos, stm, processingDate.getInfinityDate(), databaseTimeZone, databaseType);
            }
            rs = stm.executeQuery();
            while (rs.next())
            {
                MithraDataObject newData = obj.copy(false);

                this.inflateNonPkDataGenericSource(newData, rs, source, dt);
                result.add(cache.getTransactionalDataFromData(newData));
            }
            rs.close();
            rs = null;
            stm.close();
            stm = null;
        }
        catch(SQLException e)
        {
            analyzeAndWrapSqlExceptionGenericSource("get data range failed "+e.getMessage(), e, source, con);
        }
        finally
        {
            closeDatabaseObjects(con, stm, rs);
        }
        return result;
    }

    protected String getSqlWhereClauseForUpdate(MithraDataObject firstDataToUpdate)
    {
        String sql = super.getSqlWhereClauseForUpdate(firstDataToUpdate);
        sql += this.getAsOfAttributeWhereSql(firstDataToUpdate);
        sql += this.getOptimisticLockingWhereSqlIfNecessary();
        return sql;
    }

    protected MithraDataObject getMithraDataObjectForUpdate(MithraTransactionalObject mithraObject, List updateWrappers)
    {
        return mithraObject.zGetCurrentData();
    }

    protected String getSqlWhereClauseForDelete(MithraDataObject dataToDelete)
    {
        String sql = super.getSqlWhereClauseForDelete(dataToDelete);
        sql += " " + this.getAsOfAttributeWhereSql(dataToDelete);
        sql += this.getOptimisticLockingWhereSqlIfNecessary();
        return sql;
    }

    protected MithraDataObject getDataForUpdate(MithraTransactionalObject obj)
    {
        return obj.zGetCurrentData();
    }

    protected void batchPurgeForSameSourceAttribute(List mithraObjects) throws MithraDatabaseException
    {
        MithraDataObject firstData = ((MithraTransactionalObject) mithraObjects.get(0)).zGetTxDataForRead();
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        final Attribute[] primaryKeyAttributes = this.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes();

        if (databaseType.getDeleteViaInsertAndJoinThreshold() >= 0 && mithraObjects.size() > databaseType.getDeleteViaInsertAndJoinThreshold())
        {
            this.batchDeleteForSameSourceAttributeViaTempJoin(mithraObjects, source, primaryKeyAttributes);
            return;
        }

        String sql = "delete from " + this.getFullyQualifiedTableNameGenericSource(source);
        sql += " where " + this.getPrimaryKeyWhereSql();
        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "batch deleting with: " + sql + " for " + mithraObjects.size() + " objects ");
            }
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(sql);
            }
            stm = con.prepareStatement(sql);
            int batchSize = databaseType.getMaxPreparedStatementBatchCount(primaryKeyAttributes.length);
            if (batchSize <= 0) batchSize = mithraObjects.size();
            int objectsInBatch = 0;
            for (int i = 0; i < mithraObjects.size(); i++)
            {
                MithraDataObject data = ((MithraTransactionalObject) mithraObjects.get(i)).zGetTxDataForRead();
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps.clearParameters();
                    this.setPrimaryKeyAttributesWithoutDates(pps, 1, data, databaseTimeZone, databaseType);
                    this.logWithSource(this.getBatchSqlLogger(), source, "batch deleting with: " + pps.getPrintableStatement());
                }
                this.setPrimaryKeyAttributesWithoutDates(stm, 1, data, databaseTimeZone, databaseType);
                stm.addBatch();
                objectsInBatch++;

                if (objectsInBatch == batchSize)
                {
                    objectsInBatch = 0;
                    executeBatch(stm, false);

                }
            }
            if (objectsInBatch > 0)
            {
                executeBatch(stm, false);
            }
            stm.close();
            stm = null;

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            this.getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.DELETE, mithraObjects, source);  // todo diff
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch delete failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    public void purge(MithraDataObject dataToDelete) throws MithraDatabaseException
     {
         Object source =  this.getSourceAttributeValueFromObjectGeneric(dataToDelete);
         String sql = "delete from "+this.getFullyQualifiedTableNameGenericSource(source);
         sql += " where "+this.getPrimaryKeyWhereSql();
         Connection con = null;
         PreparedStatement stm = null;
         TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);
         DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
         try
         {
             con = this.getConnectionForWriteGenericSource(source);
             //TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

             if(this.getSqlLogger().isDebugEnabled())
             {
                 PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                 this.setPrimaryKeyAttributesWithoutDates(pps, 1, dataToDelete, databaseTimeZone, databaseType);
                 this.logWithSource(this.getSqlLogger(),source,"deleting with: " + pps.getPrintableStatement());
             }
             stm = con.prepareStatement(sql);
             this.setPrimaryKeyAttributesWithoutDates(stm, 1, dataToDelete, databaseTimeZone, databaseType);
             stm.executeUpdate();
             stm.close();
             stm = null;

             String dbid = this.getDatabaseIdentifierGenericSource(source);
             MithraManager.getInstance().getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.DELETE, dataToDelete, source);
         }
         catch(SQLException e)
         {
             this.analyzeAndWrapSqlExceptionGenericSource("delete failed "+e.getMessage(), e, source, con);
         }
         finally
         {
             this.closeStatementAndConnection(con, stm);
         }
     }

    public void batchPurge(List mithraObjects) throws MithraDatabaseException
    {
        Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateBySourceAttribute(mithraObjects, sourceAttribute);
            for(int i=0;i < segregated.size(); i++)
            {
                batchPurgeForSameSourceAttribute((List)segregated.get(i));
            }
        }
        else
        {
            batchPurgeForSameSourceAttribute(mithraObjects);
        }
     }

    public List findForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("not implemented");
    }
}
