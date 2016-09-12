
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

package com.gs.fw.common.mithra.test;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;

import java.sql.SQLException;
import java.util.List;

public class H2DatabaseTypeForTests extends H2DatabaseType
{
    private int tempTableCreationRequestCount = 0;
    private List<Boolean> tempTableCreationSuppressionSequence = FastList.newList();
    private boolean simulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure;
    private UnifiedSet<String> suppressedTableNames = UnifiedSet.newSet();

    public static final int TABLE_OR_VIEW_NOT_FOUND_1 = 42102;
    public static final int OBJECT_CLOSED = 90007;

    private static H2DatabaseTypeForTests instance = new H2DatabaseTypeForTests();

    public static H2DatabaseTypeForTests getInstance()
    {
        return instance;
    }

    /**
     * A test can enable this behaviour to cause 'create table' statement for temp tables to be suppressed.
     * The create table command itself will not fail, but the table will not be created.
     * It basically works by commenting out the create table SQL statement.
     *
     * For each temp table creation operation, the respective true/false flag in the list specifies whether it should be
     * suppressed (true) or not (false). Once the end of the list is reached, any further temp table creations will succeed.
     *
     */
    public void setTempTableCreationSuppressionSequence(Boolean... tempTableCreationSuppressionSequence)
    {
        this.tempTableCreationSuppressionSequence = FastList.wrapCopy(tempTableCreationSuppressionSequence);
        this.tempTableCreationRequestCount = 0;
    }

    /**
     * A test can enable this behaviour to cause isConnectionDead() to evaluate to true if we receive a 'table not found' SQL exception.
     * This is useful to simulate the behaviour which exists in SybaseDatabaseType. This behaviour exists because a 'create table'
     * on a temp table sometimes fails inexplicably without any error, and is only detected when we try to insert into the non-existent
     * temp table. Marking the connection as dead causes Mithra to retry.
     */
    public void setSimulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure(boolean simulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure)
    {
        this.simulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure = simulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure;
    }

    @Override
    public boolean isConnectionDeadWithoutRecursion(SQLException e)
    {
        if (this.simulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure)
        {
            int code = e.getErrorCode();

            return TABLE_OR_VIEW_NOT_FOUND_1 == code
                    || OBJECT_CLOSED == code;
        }
        return super.isConnectionDeadWithoutRecursion(e);
    }

    private boolean currentTempTableCreationRequestShouldBeSuppressed()
    {
        if (this.tempTableCreationRequestCount < this.tempTableCreationSuppressionSequence.size())
        {
            return Boolean.TRUE.equals(this.tempTableCreationSuppressionSequence.get(tempTableCreationRequestCount));
        }
        return false;
    }

    private void prefixCommentToSuppressCreateTableIfRequired(StringBuilder sb, String tempTableName)
    {
        if (currentTempTableCreationRequestShouldBeSuppressed())
        {
            this.suppressedTableNames.add(tempTableName);
            sb.append("-- create temp table and index suppressed for this test - skipping create table: ");
        }
        else
        {
            // Would not expect this table name to be present in the blacklist as it should be a unique name not seen before, but just in case...
            // This also means createIndexSql can rely on suppressedTableNames.contains() to check the behaviour for the current create table attempt.
            this.suppressedTableNames.remove(tempTableName);
        }
        this.tempTableCreationRequestCount++;
    }

    @Override
    public String appendNonSharedTempTableCreatePreamble(StringBuilder sb, String tempTableName)
    {
        prefixCommentToSuppressCreateTableIfRequired(sb, tempTableName);
        return super.appendNonSharedTempTableCreatePreamble(sb, tempTableName);
    }

    @Override
    public String appendSharedTempTableCreatePreamble(StringBuilder sb, String nominalTableName)
    {
        prefixCommentToSuppressCreateTableIfRequired(sb, nominalTableName);
        return super.appendSharedTempTableCreatePreamble(sb, nominalTableName);
    }

    @Override
    protected String createIndexSql(String fullTableName, CharSequence indexColumns)
    {
        if (this.suppressedTableNames.contains(fullTableName))
        {
            return "-- create temp table and index suppressed for this test - skipping create index for " + fullTableName;
        }
        else
        {
            return super.createIndexSql(fullTableName, indexColumns);
        }
    }
}