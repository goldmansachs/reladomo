/*
 Copyright 2017 Goldman Sachs.
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.connectionmanager.IntSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;



public class SyslogChecker
{
    private static Logger logger = LoggerFactory.getLogger(SyslogChecker.class);

    // settings
    private double sysLogPercentThreshold = 45.0;  // 100% means don't check
    private long sysLogMaxWaitTimeMillis = 30 * 60000;    // Wait a maximum of 20 minutes
    private long nextTimeToCheck = 0L;

    private AtomicLong totalSyslogWaitTime = new AtomicLong();

    public SyslogChecker()
    {
    }

    public SyslogChecker(double sysLogPercentThreshold, long sysLogMaxWaitTimeMillis)
    {
        this.sysLogPercentThreshold = sysLogPercentThreshold;
        this.sysLogMaxWaitTimeMillis = sysLogMaxWaitTimeMillis;
    }

    public void setSysLogPercentThreshold(double sysLogPercentThreshold)
    {
        this.sysLogPercentThreshold = sysLogPercentThreshold;
    }

    public void setSysLogMaxWaitTimeMillis(long sysLogMaxWaitTimeMillis)
    {
        this.sysLogMaxWaitTimeMillis = sysLogMaxWaitTimeMillis;
    }

    public long getSysLogMaxWaitTimeMillis()
    {
        return sysLogMaxWaitTimeMillis;
    }

    public double getSysLogPercentThreshold()
    {
        return sysLogPercentThreshold;
    }

    public long getTotalSyslogWaitTime()
    {
        return totalSyslogWaitTime.get();
    }

    /**
     * synchronized so it does one check for all involved threads.
     */
    public synchronized void checkAndWaitForSyslogSynchronized(Object sourceAttribute, String schema, MithraDatabaseObject databaseObject)
    {
        long now = System.currentTimeMillis();
        if (now > nextTimeToCheck)
        {
            this.checkAndWaitForSyslog(sourceAttribute, schema, databaseObject);
        }
    }

    public void checkAndWaitForSyslog(MithraTransactionalObject txObject)
    {
        final MithraObjectPortal mithraObjectPortal = txObject.zGetPortal();
        final MithraDatabaseObject databaseObject = mithraObjectPortal.getDatabaseObject();

        SourceAttributeType sourceAttributeType = mithraObjectPortal.getFinder().getSourceAttributeType();
        Object sourceAttributeValue = null;
        String schema;
        if (sourceAttributeType == null)
        {
            schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(null);

        }
        else
        {
            if (sourceAttributeType.isIntSourceAttribute())
            {
                IntegerAttribute sourceAttribute = (IntegerAttribute) txObject.zGetPortal().getFinder().getSourceAttribute();
                sourceAttributeValue = Integer.valueOf(sourceAttribute.valueOf(txObject));
                schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(sourceAttributeValue);
            }
            else
            {
                Attribute sourceAttribute = txObject.zGetPortal().getFinder().getSourceAttribute();
                sourceAttributeValue = sourceAttribute.valueOf(txObject);
                schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(sourceAttributeValue);
            }
        }
        this.checkAndWaitForSyslog(sourceAttributeValue, schema, databaseObject);
    }

    public void checkAndWaitForSyslog(Object sourceAttribute, String schema, MithraDatabaseObject databaseObject)
    {
        Connection connection = null;
        Object connectionManager = databaseObject.getConnectionManager();

        if (!requiresCheck() || connectionManager == null)
        {
            return;
        }

        long timer = System.currentTimeMillis();
        try
        {
            DatabaseType databaseType;
            if (databaseObject.getMithraObjectPortal().getFinder().getSourceAttributeType() == null)
            {
                databaseType = ((SourcelessConnectionManager) connectionManager).getDatabaseType();
                connection = ((SourcelessConnectionManager) connectionManager).getConnection();
            }
            else if (sourceAttribute instanceof Integer)
            {
                int sourceAttributeValue = (Integer) sourceAttribute;
                databaseType = ((IntSourceConnectionManager) connectionManager).getDatabaseType(sourceAttributeValue);
                connection = ((IntSourceConnectionManager) connectionManager).getConnection(sourceAttributeValue);
            }
            else
            {
                databaseType = ((ObjectSourceConnectionManager) connectionManager).getDatabaseType(sourceAttribute);
                connection = ((ObjectSourceConnectionManager) connectionManager).getConnection(sourceAttribute);
            }
            while (System.currentTimeMillis() - timer < sysLogMaxWaitTimeMillis)
            {
                try
                {
                    double percentFull = databaseType.getSysLogPercentFull(connection, schema);

                    this.nextTimeToCheck = System.currentTimeMillis() +
                            (percentFull < 10.0 ? 60000L : (percentFull < 30.0 ? 20000L : 3000L));

                    logger.info("syslog on " + schema + " is " + percentFull);
                    if (percentFull > sysLogPercentThreshold)
                    {
                        logger.warn("Syslog on " + schema + " is " + percentFull + "% full). Waiting 1 minute to get below " + sysLogPercentThreshold + "%.");
                        try
                        {
                            Thread.sleep(60000);
                        }
                        catch (InterruptedException e)
                        {
                            // unexpected
                        }
                    }
                    else
                    {
                        return;
                    }
                }
                catch (SQLException e)
                {
                    throw new MithraBusinessException("Error checking syslog percent full.", e);
                }
            }
            logger.warn("Reached sysLogMaxWaitTimeMillis=" + sysLogMaxWaitTimeMillis + "ms for syslog. Will try to proceed anyway.");
        }
        finally
        {
            closeConnection(connection, "Unexpected exception closing the connection after checking syslogs");
            totalSyslogWaitTime.addAndGet(System.currentTimeMillis() - timer);
        }
    }

    private static void closeConnection(Connection connection, String msg)
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                logger.warn(msg, e);
            }
        }
    }

    public boolean requiresCheck()
    {
        return getSysLogPercentThreshold() < 100.0 && getSysLogMaxWaitTimeMillis() > 0;
    }
}
