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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;

import java.sql.*;
import java.util.TimeZone;


public class TemporaryObjectDatabaseObject
{
    private static Logger batchSqlLogger = new SqlLogSnooper(LoggerFactory.getLogger("com.gs.fw.common.mithra.batch.sqllogs.temp.TemporaryObject"));
    private static Logger sqlLogger = new SqlLogSnooper(LoggerFactory.getLogger("com.gs.fw.common.mithra.sqllogs.temp.TemporaryObject"));

    private String nominalName;
    private DatabaseType databaseType;
    private Attribute[] attributes;
    private String tableName;
    private int bulkInsertThreshold;
    private TimeZone databaseTimeZone;

    public TemporaryObjectDatabaseObject(Attribute[] attributes, DatabaseType databaseType, String nominalName, TimeZone databaseTimeZone)
    {
        this.attributes = attributes;
        this.databaseType = databaseType;
        this.nominalName = nominalName;
        this.databaseTimeZone = databaseTimeZone;
        this.tableName = this.databaseType.getTableNameForNonSharedTempTable(this.nominalName);
    }

    public String getTableName()
    {
        return tableName;
    }

    public void createTable(Connection con) throws SQLException
    {
        String sql = this.databaseType.getSqlPrefixForNonSharedTempTableCreation(nominalName);
        sql += " ( c0 ";
        sql += attributes[0].zGetSqlForDatabaseType(this.databaseType)+" not null";
        for(int i=1;i<attributes.length;i++)
        {
            sql += ",c"+i+ ' ' +attributes[i].zGetSqlForDatabaseType(this.databaseType)+" not null";
        }
        sql += ")";
        sql += this.databaseType.getSqlPostfixForNonSharedTempTableCreation();
        Statement stm = null;
        try
        {
            stm = con.createStatement();
            if (this.sqlLogger.isDebugEnabled())
            {
                this.sqlLogger.debug("creating temp table with: "+sql);
            }
            stm.executeUpdate(sql);
        }
        finally
        {
            this.closeStatement(stm);
        }
    }

    public void dropTable(Connection con)
    {
        Statement stm = null;
        try
        {
            stm = con.createStatement();
            String sql = "drop table " + tableName;
            if (this.sqlLogger.isDebugEnabled())
            {
                this.sqlLogger.debug("dropping temp table with: "+sql);
            }
            stm.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            this.sqlLogger.error("could not drop temporary table "+tableName+" please drop it manually", e);
        }
        finally
        {
            this.closeStatement(stm);
        }
    }

    protected void closeStatement(Statement statement)
    {
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
                this.sqlLogger.error("Could not close Statement", e);
            }
        }
    }


    protected void reportWarnings(PreparedStatement stm) throws SQLException
    {
        SQLWarning w = stm.getWarnings();
        while (w != null)
        {
            this.sqlLogger.warn("SQL warning: ", w);
            w = w.getNextWarning();
        }
    }

    private int[] executeBatchAndHandleBatchException(PreparedStatement stm)
            throws SQLException
    {
        int [] results;
        try
        {
            results = stm.executeBatch();
        }
        catch (BatchUpdateException e)
        {
            this.reportWarnings(stm);
            results = e.getUpdateCounts();
            for (int i = 0; i < results.length; i++)
            {
                if (results[i] == Statement.EXECUTE_FAILED)
                {
                    this.sqlLogger.error("failed in batch execute. See warnings above. Failure in batched statement number " + (i + 1));
                }
            }
            throw e;
        }
        return results;
    }

    protected void executeBatch(PreparedStatement stm, boolean checkUpdateCount) throws SQLException
    {
        if (checkUpdateCount)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (tx != null)
            {
                tx.setExpectedExecuteBatchReturn(1);
            }
        }
        int[] results = executeBatchAndHandleBatchException(stm);
        if (checkUpdateCount)
        {
            checkUpdateCount(results);
        }
        stm.clearBatch();
    }

    private void checkUpdateCount(int[] results)
    {
        for (int i = 0; i < results.length; i++)
        {
            if (results[i] != Statement.SUCCESS_NO_INFO && results[i] != 1)
            {
                this.sqlLogger.warn("batch command did not insert/update/delete the correct number of rows " + results[i] + " at position " + (i + 1));
            }
        }
    }

}
