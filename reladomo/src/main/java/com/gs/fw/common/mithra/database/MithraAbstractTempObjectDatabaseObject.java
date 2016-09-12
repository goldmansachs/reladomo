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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.tempobject.MithraTemporaryContext;
import com.gs.fw.common.mithra.tempobject.MithraTemporaryDatabaseObject;
import com.gs.fw.common.mithra.util.TempTableNamer;
import com.gs.fw.common.mithra.notification.MithraNotificationEventManager;
import com.gs.fw.common.mithra.notification.UninitializedNotificationEventManager;
import com.gs.fw.common.mithra.MithraDatabaseException;

import java.sql.SQLException;


public abstract class MithraAbstractTempObjectDatabaseObject extends MithraAbstractTransactionalDatabaseObject implements MithraTemporaryDatabaseObject
{

    private static final MithraNotificationEventManager UNINITIALIZED_NOTIFICATION_MANAGER = new UninitializedNotificationEventManager();

    private String tempTableName;
    private String tempTablePostfix = this.getClass().getSimpleName().substring(0, 9);
    private volatile SingleColumnAttribute[] persistedPkAttributes;

    protected MithraAbstractTempObjectDatabaseObject(String loggerClassName, String fullyQualifiedFinderClassName, int totalColumnsInResultSet,
            int totalColumnsInInsert, String columnListWithoutPK, String columnListWithoutPkWithAlias, boolean hasOptimisticLocking,
            boolean hasNullablePrimaryKeys, boolean hasSourceAttribute, String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        super(loggerClassName, fullyQualifiedFinderClassName, totalColumnsInResultSet, totalColumnsInInsert, columnListWithoutPK,
                columnListWithoutPkWithAlias, hasOptimisticLocking, hasNullablePrimaryKeys, hasSourceAttribute,
                primaryKeyWhereSqlWithDefaultAlias, primaryKeyIndexColumns);
    }

    public String getTableNameGenericSource(Object source) throws MithraDatabaseException
    {
        return tempTableName;
    }

    public String getFullyQualifiedTableNameGenericSource(Object source)
    {
        return tempTableName;
    }

    protected void setTempTableName(Object source, String tempTableName)
    {
        this.tempTableName = tempTableName;
    }

    public String getColumnsForBulkInsertCreation(DatabaseType dt)
    {
        StringBuilder sb = new StringBuilder(50);
        appendColumnDefinitions(sb, dt, true);
        return sb.toString();
    }

    public String getTableNamePrefixGenericSource(Object source)
    {
        return tempTablePostfix;
    }

    protected String getTempTablePostFix()
    {
        return tempTablePostfix;
    }

    public void dropTempTable(final Object genericSource)
    {
        try
        {
            this.dropTempTable(genericSource, getFullyQualifiedTableNameGenericSource(genericSource), true);
        }
        finally
        {
            try
            {
                getConnectionManagerWrapper().unbindConnection(genericSource);
            }
            catch (SQLException e)
            {
                getLogger().error("Could not return connection to pool", e);
            }
        }
    }

    protected SingleColumnAttribute[] getPkAttributes()
    {
        SingleColumnAttribute[] result = persistedPkAttributes;
        if (result == null)
        {
            Attribute[] primaryKeyAttributes = this.getFinder().getPrimaryKeyAttributes();
            Attribute sourceAttribute = this.getFinder().getSourceAttribute();
            int len = primaryKeyAttributes.length;
            if (sourceAttribute != null)
            {
                len--;
            }
            result = new SingleColumnAttribute[len];
            int count = 0;
            for(int i=0;i<primaryKeyAttributes.length;i++)
            {
                if (!primaryKeyAttributes[i].isSourceAttribute())
                {
                    result[count++] = (SingleColumnAttribute) primaryKeyAttributes[i];
                }
            }
            persistedPkAttributes = result;
        }
        return result;
    }

    public void createNonSharedTempTable(final Object genericSource)
    {
        String nominalName = "T"+ TempTableNamer.getNextTempTableName() + getTempTablePostFix();
        setTempTableName(genericSource, createNonSharedTempTable(genericSource, nominalName, this.getPkAttributes(), true));
    }

    public void createSharedTempTable(Object genericSource, MithraTemporaryContext tempContext)
    {
        String nominalName = "T"+TempTableNamer.getNextTempTableName() + getTempTablePostFix();
        String tempTable;
        if (getDatabaseTypeGenericSource(genericSource).supportsSharedTempTable())
        {
            tempTable = createSharedTempTable(genericSource, nominalName, this.getPkAttributes(), true);
        }
        else
        {
            getConnectionManagerWrapper().bindConnection(tempContext, genericSource, this.getConnectionGenericSource(genericSource));
            tempTable = createNonSharedTempTable(genericSource, nominalName, this.getPkAttributes(), true);
        }
        setTempTableName(genericSource, tempTable);
    }

    @Override
    protected MithraNotificationEventManager getNotificationEventManager()
    {
        return UNINITIALIZED_NOTIFICATION_MANAGER;
    }
}
