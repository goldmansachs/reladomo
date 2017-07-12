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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.connectionmanager.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.integer.IntegerResultSetParser;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.notification.MithraNotificationEventManager;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.*;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;
import com.gs.fw.common.mithra.transaction.UpdateOperation;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import java.io.IOException;
import java.io.ObjectInput;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;



public abstract class MithraAbstractDatabaseObject
        implements MithraDatabaseObject,
        MithraCodeGeneratedDatabaseObject, MithraTuplePersister
{

    private static final String SELECT_STRING_FOR_COUNT_ALL = "count(*)";
    private static final ConcurrentLinkedQueue<Object[]> arrayPool = new ConcurrentLinkedQueue<Object[]>();
    protected static final int DATA_ARRAY_SIZE = 32;
    private static final Logger staticLogger = LoggerFactory.getLogger(MithraAbstractDatabaseObject.class);
    private static final Set<String> NO_RETRY_THREAD_NAMES = new UnifiedSet();

    private static final String[] questionMarks = new String[4];
    private static MithraStatsListenerFactory statsListenerFactory = MithraAbstractDatabaseObject.createStatsListenerFactory();

    private static final String STATS_LISTENER_PROPERTY_NAME = "mithra.databaseObject.statsListenerFactory";

    private String defaultSchema = null;
    private boolean reloading = false;
    private SingleColumnAttribute[] persistentAttributes;
    protected LoadOperationProvider loadOperationProvider = new DefaultLoadOperationProvider();
    private MithraStatsListener statsListener;
    protected ConnectionManagerWrapper connectionManagerWrapper;

    private final String fullyQualifiedFinderClassName;
    private final int totalColumnsInResultSet;
    private final int totalColumnsInInsert;
    private final String columnListWithoutPK;
    private final String columnListWithoutPkWithAlias;
    private final boolean hasOptimisticLocking;
    private final boolean hasNullablePrimaryKeys;
    private final boolean hasSourceAttribute;
    private final String primaryKeyWhereSqlWithDefaultAlias;
    private final String primaryKeyIndexColumns;

    protected final Logger logger;
    protected final Logger testSqlLogger;
    protected final Logger batchSqlLogger;
    protected final Logger sqlLogger;

    protected final boolean checkNullOnInsert;

    static
    {
        questionMarks[1] = "?";
        questionMarks[2] = "?,?";
        questionMarks[3] = "?,?,?";
    }



    protected MithraAbstractDatabaseObject(String loggerClassName, String fullyQualifiedFinderClassName,
            int totalColumnsInResultSet, int totalColumnsInInsert, String columnListWithoutPK,
            String columnListWithoutPkWithAlias, boolean hasOptimisticLocking, boolean hasNullablePrimaryKeys,
            boolean hasSourceAttribute, String primaryKeyWhereSqlWithDefaultAlias, String primaryKeyIndexColumns)
    {
        if (MithraAbstractDatabaseObject.statsListenerFactory != null)
        {
            this.statsListener = MithraAbstractDatabaseObject.statsListenerFactory.createListener();
        }
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.testSqlLogger = new SqlLogSnooper(LoggerFactory.getLogger("com.gs.fw.common.mithra.test.sqllogs."+loggerClassName));
        this.batchSqlLogger = new SqlLogSnooper(LoggerFactory.getLogger("com.gs.fw.common.mithra.batch.sqllogs."+loggerClassName));
        this.sqlLogger = new SqlLogSnooper(LoggerFactory.getLogger("com.gs.fw.common.mithra.sqllogs."+loggerClassName));
        this.fullyQualifiedFinderClassName = fullyQualifiedFinderClassName;
        this.totalColumnsInResultSet = totalColumnsInResultSet;
        this.totalColumnsInInsert = totalColumnsInInsert;
        this.columnListWithoutPK = columnListWithoutPK;
        this.columnListWithoutPkWithAlias = columnListWithoutPkWithAlias;
        this.hasOptimisticLocking = hasOptimisticLocking;
        this.hasNullablePrimaryKeys = hasNullablePrimaryKeys;
        this.hasSourceAttribute = hasSourceAttribute;
        this.primaryKeyWhereSqlWithDefaultAlias = primaryKeyWhereSqlWithDefaultAlias;
        this.primaryKeyIndexColumns = primaryKeyIndexColumns;
        this.checkNullOnInsert = !"false".equals(System.getProperty("mithra.checkNullOnInsert"));
    }

    public Logger getLogger()
    {
        return this.logger;
    }

    public Logger getSqlLogger()
    {
        return this.sqlLogger;
    }

    public Logger getBatchSqlLogger()
    {
        return this.batchSqlLogger;
    }

    public Logger getTestSqlLogger()
    {
        return this.testSqlLogger;
    }

    public String getFullyQualifiedFinderClassName()
    {
        return this.fullyQualifiedFinderClassName;
    }

    public int getTotalColumnsInResultSet()
    {
        return totalColumnsInResultSet;
    }

    public int getTotalColumnsInInsert()
    {
        return totalColumnsInInsert;
    }

    public String getColumnListWithoutPk()
    {
        return columnListWithoutPK;
    }

    public String getColumnListWithoutPkWithAlias()
    {
        return columnListWithoutPkWithAlias;
    }

    public boolean hasOptimisticLocking()
    {
      return hasOptimisticLocking;
    }

    public boolean hasNullablePrimaryKeys()
    {
        return hasNullablePrimaryKeys;
    }

    public boolean hasSourceAttribute()
    {
        return hasSourceAttribute;
    }

    protected ConnectionManagerWrapper getConnectionManagerWrapper()
    {
        return this.connectionManagerWrapper;
    }

    public String getPrimaryKeyWhereSqlWithDefaultAlias()
    {
        return primaryKeyWhereSqlWithDefaultAlias;
    }

    public String getNotificationEventIdentifier()
    {
        return getFullyQualifiedFinderClassName();
    }

    public boolean isReplicated()
    {
        return false;
    }

    public String getPrimaryKeyIndexColumns()
    {
        return primaryKeyIndexColumns;
    }

    public static void addNoRetryThreadNames(Collection<String> threadNames)
    {
        synchronized (NO_RETRY_THREAD_NAMES)
        {
            NO_RETRY_THREAD_NAMES.addAll(threadNames);
        }
    }

    public static void resetNoRetryThreadNames()
    {
        synchronized (NO_RETRY_THREAD_NAMES)
        {
            NO_RETRY_THREAD_NAMES.clear();
        }
    }

    private static MithraStatsListenerFactory createStatsListenerFactory()
    {
        String factoryClassName = System.getProperty(STATS_LISTENER_PROPERTY_NAME);
        if (factoryClassName == null)
        {
            return null;
        }
        else
        {
            try
            {
                return (MithraStatsListenerFactory) Class.forName(factoryClassName).newInstance();
            }
            catch (Exception e)
            {
                staticLogger.error("Exception creating factory " + factoryClassName + ". Will return null", e);
                return null;
            }
        }
    }

    public void setLoadOperationProvider(LoadOperationProvider loadOperationProvider)
    {
        this.loadOperationProvider = loadOperationProvider;
    }

    protected String getQuestionMarks(int number)
    {
        if (number < questionMarks.length)
        {
            return questionMarks[number];
        }
        StringBuilder builder = new StringBuilder(number * 2 - 1);
        builder.append(questionMarks[questionMarks.length - 1]);
        for (int i = questionMarks.length; i <= number; i++)
        {
            builder.append(',').append('?');
        }
        return builder.toString();
    }

    protected String getTupleColumnNames(SingleColumnAttribute[] attributes)
    {
        StringBuilder builder = new StringBuilder(attributes.length * 3);
        for (int i = 0; i < attributes.length; i++)
        {
            if (i > 0) builder.append(',');
            builder.append(attributes[i].getColumnName());
        }
        return builder.toString();
    }

    protected MithraPerformanceData getPerformanceData()
    {
        return this.getMithraObjectPortal().getPerformanceData();
    }

    public void setDefaultSchema(String schema)
    {
        this.defaultSchema = schema;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    public int count(Operation op)
    {
        List matches = this.protectedComputeFunction(op, null, SELECT_STRING_FOR_COUNT_ALL, new IntegerResultSetParser());
        int result = 0;
        for (int i = 0; i < matches.size(); i++)
        {
            result += (Integer) matches.get(i);
        }
        return result;
    }

    protected void reportNextException(SQLException e)
    {
        if (e.getNextException() != null)
        {
            int nestLevel = 0;
            while (e != null)
            {
                this.getSqlLogger().error("SQL Exception (nest level " + nestLevel + "), code: " + e.getErrorCode() + " state: " + e.getSQLState(), e);
                e = e.getNextException();
                nestLevel++;
            }
        }
    }

    protected void reportNextWarning(SQLWarning warn)
    {
        if (warn != null)
        {
            int nestLevel = 0;
            while (warn != null)
            {
                this.getSqlLogger().error("SQL Warning (nest level " + nestLevel + "), code: " + warn.getErrorCode() + " state: " + warn.getSQLState(), warn);
                warn = warn.getNextWarning();
            }
        }
    }

    public void closeDatabaseObjects(Connection connection, Statement statement, ResultSet resultSet)
    {
        if (resultSet != null)
        {
            try
            {
                resultSet.close();
            }
            catch (SQLException e)
            {
                this.getLogger().error("Could not close ResultSet", e);
            }
        }
        closeStatementAndConnection(connection, statement);
    }

    protected void closeStatementAndConnection(Connection connection, Statement statement)
    {
        closeStatement(statement);
        closeConnection(connection);
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
                this.getLogger().error("Could not close Statement", e);
            }
        }
    }

    protected void closeConnection(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                if ("Already closed.".equals(e.getMessage()))
                {
                    this.getLogger().debug("Connection already closed", e);
                }
                else
                {
                    this.getLogger().error("Could not close connection", e);
                }
            }
        }
    }

    protected boolean limitRowCount(int rowcount, DatabaseType dt, Connection con)
            throws SQLException
    {
        if (rowcount > 0)
        {
            if (dt.hasTopQuery())
            {
                this.getLogger().debug("using top query");
            }
            else if (dt.hasSetRowCount())
            {
                if (this.getLogger().isDebugEnabled())
                {
                    this.getLogger().debug("limiting row count to " + rowcount);
                }
                dt.setRowCount(con, rowcount + 1);
                return true;
            }
            else
            {
                if (this.getLogger().isDebugEnabled())
                {
                    this.getLogger().debug("ignoring rows after " + rowcount);
                }
            }
        }
        return false;
    }

    public String createQuestionMarks(int numberOfQuestions)
    {
        int questionLength = ((numberOfQuestions - 1) * 2) + 1;
        StringBuilder bunchOfQuestionMarks = new StringBuilder(questionLength);
        bunchOfQuestionMarks.append('?');
        for (int k = 1; k < numberOfQuestions; k++)
        {
            bunchOfQuestionMarks.append(",?");
        }
        return bunchOfQuestionMarks.toString();
    }

    public void deleteReplicationNotificationData(int minEventId, int maxEventId)
    {
        String fullyQualifiedChildQueueTableName = this.getFullyQualifiedChildQueueTableNameGenericSource(null);

        StringBuilder deleteSql = new StringBuilder("delete from ");
        deleteSql.append(fullyQualifiedChildQueueTableName);

        String whereClause = " where event_seq_no <= ? and event_seq_no >= ?";
        deleteSql.append(whereClause);

        PreparedStatement stm = null;
        String statement = deleteSql.toString();
        Connection con = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(null);
            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                pps.setInt(1, maxEventId);
                pps.setInt(2, minEventId);
                this.getSqlLogger().debug("delete with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(statement);
            stm.setInt(1, maxEventId);
            stm.setInt(2, minEventId);
            stm.execute();
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("deleting replication notification failed ", e, null, con);
        }
        finally
        {
            closeStatementAndConnection(con, stm);
        }
    }

    protected void prepareTransactionalOperation(MithraTransaction.OperationMode operationMode)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null)
        {
            tx.setWriteOperationMode(operationMode);
        }
    }

    protected void prepareForQuery(SqlQuery query)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null)
        {
            Operation originalOperation = query.getAnalyzedOperation().getOriginalOperation();
            MithraObjectPortal targetPortal = originalOperation.getResultObjectPortal();
            boolean locking = targetPortal.getTxParticipationMode(tx).mustLockOnRead();
            if (!locking)
            {
                UnifiedSet portals = new UnifiedSet(3);
                originalOperation.addDependentPortalsToSet(portals);
                Iterator it = portals.iterator();
                while (it.hasNext() && !locking)
                {
                    MithraObjectPortal portal = (MithraObjectPortal) it.next();
                    locking = portal.getTxParticipationMode(tx).mustLockOnRead();
                }
            }
            tx.setWriteOperationMode(locking ? MithraTransaction.OperationMode.TRANSACTIONAL_READ : MithraTransaction.OperationMode.READ);
        }
    }

    protected void reportWarnings(PreparedStatement stm) throws SQLException
    {
        SQLWarning w = stm.getWarnings();
        while (w != null)
        {
            this.getSqlLogger().warn("SQL warning: ", w);
            w = w.getNextWarning();
        }
    }

    public static String convertTimestampToString(java.util.Date timestamp, DatabaseType dt)
    {
        return dt.convertDateToString(timestamp);
    }

    public static String convertDateOnlyToString(java.util.Date timestamp, DatabaseType dt)
    {
        return dt.convertDateOnlyToString(timestamp);
    }

    protected String createNonSharedTempTable(final Object genericSource, String nominalName, SingleColumnAttribute[] pkAttributes, final boolean isForQuery)
    {
        return createNonSharedTempTable(genericSource, nominalName, this.getPersistentAttributes(), pkAttributes, false, isForQuery);
    }

    protected String createNonSharedTempTable(final Object genericSource, String nominalName,
            SingleColumnAttribute[] persistentAttributes, SingleColumnAttribute[] pkAttributes,
            boolean forceOnSameThread, final boolean isForQuery)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(genericSource);
        StringBuilder sb = new StringBuilder(50);
        String fullTableName = dt.appendNonSharedTempTableCreatePreamble(sb, nominalName);
        sb.append(" (");
        this.appendColumnDefinitions(sb, dt, persistentAttributes, true);
        sb.append(") ");
        sb.append(dt.getSqlPostfixForNonSharedTempTableCreation());

        final String sql = sb.toString();
        final String indexSql = this.createNonSharedIndexSql(fullTableName, pkAttributes, dt);
        if (!dt.supportsSharedTempTable() || forceOnSameThread || (MithraManagerProvider.getMithraManager().isInTransaction() && dt.createTempTableAllowedInTransaction()))
        {
            executeCreateTable(genericSource, sql, indexSql, isForQuery);
        }
        else
        {
            int retry = 10;
            while (true)
            {
                try
                {
                    ExceptionCatchingThread.executeTask(new ExceptionHandlingTask()
                    {
                        @Override
                        public void execute()
                        {
                            executeCreateTable(genericSource, sql, indexSql, isForQuery);
                        }
                    });
                    break;
                }
                catch (MithraBusinessException e)
                {
                    retry = e.ifRetriableWaitElseThrow("create table failed with retriable exception, will retry", retry, this.getSqlLogger());
                }
            }
        }
        return fullTableName;
    }

    private void executeCreateTable(Object genericSource, String sql, String indexSql, boolean isForQuery)
    {
        Connection con = null;
        Statement stm = null;
        try
        {
            con = this.getConnectionForTempWriteGenericSource(genericSource, isForQuery);
            stm = con.createStatement();
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.getSqlLogger().debug("connection:"+System.identityHashCode(con)+" creating temp table with: " + sql);
            }
            stm.executeUpdate(sql);
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.getSqlLogger().debug("connection:"+System.identityHashCode(con)+" creating temp table index with: " + indexSql);
            }
            stm.executeUpdate(indexSql);
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.getSqlLogger().debug("creating temp index with: " + indexSql);
            }
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("create temp table failed " + e.getMessage(), e, genericSource, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    protected String createSharedTempTable(Object genericSource, String nominalName, SingleColumnAttribute[] pkAttributes, boolean isForQuery)
    {
        return createSharedTempTable(genericSource, nominalName, this.getPersistentAttributes(), pkAttributes, isForQuery);
    }

    protected String createSharedTempTable(Object genericSource, String nominalName,
            SingleColumnAttribute[] persistentAttributes, SingleColumnAttribute[] pkAttributes, boolean isForQuery)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(genericSource);
        StringBuilder sb = new StringBuilder(50);
        String fullTableName = dt.appendSharedTempTableCreatePreamble(sb, nominalName);
        sb.append(" (");
        this.appendColumnDefinitions(sb, dt, persistentAttributes, true);
        sb.append(") ");
        sb.append(dt.getSqlPostfixForSharedTempTableCreation());

        String sql = sb.toString();
        String indexSql = this.createSharedIndexSql(fullTableName, pkAttributes, dt);
        int retry = 10;
        while (true)
        {
            try
            {
                executeCreateTable(genericSource, sql, indexSql, isForQuery);
                break;
            }
            catch (MithraBusinessException e)
            {
                retry = e.ifRetriableWaitElseThrow("create table failed with retriable exception, will retry", retry, this.getSqlLogger());
            }
        }
        return fullTableName;
    }

    private String createNonSharedIndexSql(String fullTableName, SingleColumnAttribute[] indexAttributes, DatabaseType dt)
    {
        CharSequence indexColumns = getPrimaryKeyIndexColumns(indexAttributes);
        return dt.createNonSharedIndexSql(fullTableName, indexColumns);
    }

    private String createSharedIndexSql(String fullTableName, SingleColumnAttribute[] indexAttributes, DatabaseType dt)
    {
        CharSequence indexColumns = getPrimaryKeyIndexColumns(indexAttributes);
        return dt.createSharedIndexSql(fullTableName, indexColumns);
    }

    private CharSequence getPrimaryKeyIndexColumns(SingleColumnAttribute[] indexAttributes)
    {
        StringBuilder builder = new StringBuilder(indexAttributes.length * 3);
        for (int i = 0; i < indexAttributes.length - 1; i++)
        {
            builder.append(indexAttributes[i].getColumnName()).append(',');
        }
        builder.append(indexAttributes[indexAttributes.length - 1].getColumnName());
        return builder;
    }

    protected void dropTempTable(final Object genericSource, String tempTableName, boolean isForQuery)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(genericSource);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx != null && dt.nonSharedTempTablesAreDroppedAutomatically())
        {
            return;
        }
        Connection con = null;
        Statement stm = null;
        try
        {
            con = this.getConnectionForTempWriteGenericSource(genericSource, isForQuery);
            stm = con.createStatement();
            boolean drop = true;
            if (tx != null && !dt.dropTableAllowedInTransaction())
            {
                drop = false;
                DropSynchronization synchronization = new DropSynchronization(genericSource, tempTableName, isForQuery);
                synchronization.setSynchronous(dt.dropTempTableSyncAfterTransaction());
                if (dt.dropTempTableSyncAfterTransaction() && con instanceof PostTransactionExecutor)
                {
                    ((PostTransactionExecutor)con).addPostTransactionAction(synchronization);

                }
                else
                {
                    tx.registerSynchronization(synchronization);
                }
            }
            if (drop && dt.truncateBeforeDroppingTempTable())
            {
                String sql = "truncate table " + tempTableName;
                if (this.getSqlLogger().isDebugEnabled())
                {
                    this.getSqlLogger().debug("connection:"+System.identityHashCode(con)+" truncating temp table with: " + sql);
                }
                stm.executeUpdate(sql);
            }
            String sql = (drop ? "drop table " : "delete from ") + tempTableName;
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.getSqlLogger().debug("connection:"+System.identityHashCode(con)+" dropping temp table with: " + sql);
            }
            stm.executeUpdate(sql);
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            this.getSqlLogger().error("IGNORING drop temp table failed " + e.getMessage(), e);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    public Synchronization createDropBulkTempTableSynchronization(Object source, String tempTableName)
    {
        return new DropBulkTempTableSynchronization(source, tempTableName);
    }

    protected class DatabaseCursor implements Cursor
    {
        private long startTime;

        protected final Cache cache;
        private DatabaseType dt;

        private Connection currentConnection = null;
        public final AnalyzedOperation analyzedOperation;
        private final Filter postLoadFilter;
        private PreparedStatement currentStatement = null;
        private ResultSet currentResultSet = null;
        private Object nextDataObject;
        private final List portalList;
        private int sourceCount = 0;
        private int currentSourceNum = 0;
        private int queryCount = 0;
        private int currentQueryNumber = -1;
        protected SqlQuery query;
        private String statement = null;

        private int currentMaxRowCount = 0;
        private final int maxRowCount;
        private int rowCount = 0;

        private static final int READY = 1;
        private static final int HASNEXT = 2;

        private int state = READY;
        private boolean currentHasNext;
        private boolean mustResetRowCount;

        private Object source = null;

        private Logger sqlLogger = getSqlLogger();

        public DatabaseCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean forceImplicitJoin)
        {
            this.startTime = System.currentTimeMillis();
            this.analyzedOperation = analyzedOperation;
            this.postLoadFilter = postLoadFilter;

            this.query = new SqlQuery(analyzedOperation, orderby, forceImplicitJoin);
            this.query.setForceServerSideOrderBy(true);

            this.sourceCount = query.getNumberOfSources();
            this.cache = getMithraObjectPortal().getCache();

            CachedQuery cachedQuery = new CachedQuery(analyzedOperation.getOriginalOperation(), orderby);
            this.portalList = cachedQuery.getPortalList();

            this.maxRowCount = rowcount;
            this.currentMaxRowCount = rowcount;
        }

        protected boolean matchesPostLoadOperation(Object dataObject)
        {
            return this.postLoadFilter == null || this.postLoadFilter.matches(dataObject);
        }

        protected DatabaseType getDatabaseType()
        {
            return dt;
        }

        private void prepareResultSet()
        {
            if (MithraManagerProvider.getMithraManager().isInTransaction())
            {
                prepareResultSetWithoutRetry();
            }
            else
            {
                prepareResultSetWithRetry();
            }
        }

        private void prepareResultSetWithRetry()
        {
            int retriesLeft = MithraTransaction.DEFAULT_TRANSACTION_RETRIES;
            while (true)
            {
                try
                {
                    prepareResultSetWithoutRetry();
                    return;
                }
                catch (MithraBusinessException e)
                {
                    this.cleanUpDbConnections(true);
                    retriesLeft = e.ifRetriableWaitElseThrow("cursor failed with retriable error. retrying.", retriesLeft, logger);
                    this.currentQueryNumber = -1;
                }
            }
        }

        private void prepareResultSetWithoutRetry()
        {
            this.cleanUpDbConnectionsWithExceptionHandling(currentQueryNumber == -1);

            if (currentQueryNumber == -1)
            {
                this.source = getSourceAttributeValueForSelectedObjectGeneric(this.query, this.currentSourceNum);
                this.currentQueryNumber = 0;

                String databaseIdentifier = getDatabaseIdentifierGenericSource(this.source);
                for (int p = 0; p < portalList.size(); p++)
                {
                    MithraObjectPortal portal = (MithraObjectPortal) portalList.get(p);
                    portal.registerForNotification(databaseIdentifier);
                }

                this.dt = MithraAbstractDatabaseObject.this.getDatabaseTypeGenericSource(source);
                this.queryCount = this.query.prepareQueryForSource(this.currentSourceNum, this.dt, getDatabaseTimeZoneGenericSource(this.source));
            }
            if (maxRowCount > 0)
            {
                this.currentMaxRowCount = this.maxRowCount - rowCount;
            }

            try
            {
                this.currentConnection = MithraAbstractDatabaseObject.this.getConnectionForQueryGenericSource(this.query, this.source);
                query.prepareForQuery(this.currentQueryNumber);
                statement = this.getStatement(this.dt, this.query, this.analyzedOperation, this.currentMaxRowCount);
                mustResetRowCount = limitRowCount(this.currentMaxRowCount, this.dt, this.currentConnection);
                if (this.sqlLogger.isDebugEnabled())
                {
                    PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                    this.query.setStatementParameters(pps);
                    MithraAbstractDatabaseObject.this.logWithSource(this.sqlLogger, this.source, "connection:"+System.identityHashCode(this.currentConnection)+" find with: " + pps.getPrintableStatement());
                }
                this.currentStatement = this.currentConnection.prepareStatement(statement);
                this.query.setStatementParameters(this.currentStatement);
                this.currentResultSet = this.currentStatement.executeQuery();
                this.currentQueryNumber++;
                if (this.currentQueryNumber == this.queryCount)
                {
                    this.currentQueryNumber = -1;
                    this.currentSourceNum++;
                }
            }
            catch (SQLException e)
            {
                try
                {
                    MithraAbstractDatabaseObject.this.analyzeAndWrapSqlExceptionGenericSource("find failed for statement "+statement+ "\nwith message: " + e.getMessage(), e, this.source, this.currentConnection);
                }
                finally
                {
                    this.cleanUpDbConnections(true);
                }
            }
        }

        private void cleanUpDbConnections(boolean cleanUpSource)
        {
            if (mustResetRowCount && currentConnection != null)
            {
                this.dt.setInfiniteRowCount(this.currentConnection);
            }
            MithraAbstractDatabaseObject.this.closeDatabaseObjects(this.currentConnection, this.currentStatement, this.currentResultSet);
            this.currentConnection = null;
            this.currentStatement = null;
            this.currentResultSet = null;
            if (cleanUpSource)
            {
                query.cleanTempForSource(this.currentQueryNumber, this.dt);
            }
        }

        public void remove()
        {
        }

        public boolean hasNext()
        {
            try
            {
                if (this.state == READY)
                {
                    do
                    {
                        this.currentHasNext = ((this.maxRowCount <= 0) || (this.rowCount < this.maxRowCount)) &&
                                ((this.currentResultSet != null) && (this.currentResultSet.next()));

                        while (!this.currentHasNext && ((this.maxRowCount <= 0) || (this.rowCount < this.maxRowCount)) && (this.currentSourceNum < this.sourceCount))
                        {
                            this.prepareResultSet();
                            this.currentHasNext = ((this.maxRowCount <= 0) || (this.rowCount < this.maxRowCount)) &&
                                    ((this.currentResultSet != null) && (this.currentResultSet.next()));

                        }

                        this.nextDataObject = this.currentHasNext ? this.getObject(this.currentResultSet, this.source) : null;
                    } while (this.currentHasNext && this.nextDataObject == null);

                    this.state = HASNEXT;
                }
                return this.currentHasNext;
            }
            catch (SQLException e)
            {
                try
                {
                    MithraAbstractDatabaseObject.this.analyzeAndWrapSqlExceptionGenericSource("find failed " + e.getMessage(), e, this.source, this.currentConnection);
                }
                finally
                {
                    this.cleanUpDbConnections(true);
                }
                return false; // we won't get here, but the compiler doesn't know that.
            }
        }

        public Object next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("Can't find a 'next' element");
            }
            this.rowCount++;
            this.state = READY;
            return this.nextDataObject;
        }

        public void close()
        {
            cleanUpDbConnectionsWithExceptionHandling(currentQueryNumber == -1);
            if (this.sqlLogger.isDebugEnabled())
            {
                long totalTime = System.currentTimeMillis() - this.startTime;
                this.sqlLogger.debug("retrieved " + this.rowCount + " objects, " +
                        ((this.rowCount > 0) ? ((totalTime / this.rowCount) + " ms per") : (totalTime +
                                " ms")));
            }

            if (statsListener != null)
            {
                statsListener.processRetrieval(this.source, new PrintableStatementBuilder(this.statement, this.query),
                                               this.rowCount, this.startTime, MithraAbstractDatabaseObject.this.getClass());
            }
            getPerformanceData().recordTimeForFind(this.rowCount, this.startTime);
        }

        private void cleanUpDbConnectionsWithExceptionHandling(boolean cleanUpSource)
        {
            try
            {
                ResultSet rs = this.currentResultSet;
                this.currentResultSet = null;
                if (rs != null)
                {
                    rs.close();
                }
                PreparedStatement stm = this.currentStatement;
                this.currentStatement = null;
                if (stm != null)
                {
                    stm.close();
                }
            }
            catch (SQLException e)
            {
                MithraAbstractDatabaseObject.this.analyzeAndWrapSqlExceptionGenericSource("find failed " + e.getMessage(), e, this.source, this.currentConnection);
                cleanUpSource = true;
            }
            finally
            {
                cleanUpDbConnections(cleanUpSource);
            }
        }

        protected Object getObject(ResultSet res, Object source) throws SQLException
        {
            MithraDataObject newData = inflateDataGenericSource(res, source, this.dt);
            return (this.matchesPostLoadOperation(newData))
                    ? this.cache.getObjectFromDataWithoutCaching(newData)
                    : null;
        }

        protected String getStatement(DatabaseType dt, SqlQuery query, AnalyzedOperation analyzedOperation, int rowCount)
        {
            return MithraAbstractDatabaseObject.this.findGetStatement(dt, query, analyzedOperation, rowCount);
        }
    }

    public List computeFunction(Operation op, OrderBy orderby, String columnOrFunctions, ResultSetParser resultSetParser)
    {
        getLogger().error("Deprecated computeFunction usage... Stop using this. It will be removed");
        return protectedComputeFunction(op, orderby, columnOrFunctions, resultSetParser);
    }

    protected List protectedComputeFunction(Operation op, OrderBy orderby, String columnOrFunctions, ResultSetParser resultSetParser)
    {
        SqlQuery query = new SqlQuery(op, orderby, false);
        query.setForceServerSideOrderBy(true);
        int sourceCount = query.getNumberOfSources();
        List result = new MithraFastList();

        for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
        {
            Object source = getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, dt, this.getDatabaseTimeZoneGenericSource(source));
            try
            {
                Connection con = null;
                ResultSet rs = null;
                PreparedStatement stm = null;
                try
                {
                    con = this.getConnectionForQueryGenericSource(query, source);
                    for (int q = 0; q < queries; q++)
                    {
                        query.prepareForQuery(q);
                        String statement = dt.getSelect(columnOrFunctions,
                                query, null, MithraManagerProvider.getMithraManager().isInTransaction(), 0);

                        if (this.getSqlLogger().isDebugEnabled())
                        {
                            PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                            query.setStatementParameters(pps);
                            this.logWithSource(this.getSqlLogger(), source, "executing statement " + pps.getPrintableStatement());
                        }
                        stm = con.prepareStatement(statement);
                        query.setStatementParameters(stm);

                        rs = stm.executeQuery();
                        while (rs.next())
                        {
                            result.add(resultSetParser.parseResult(rs));
                        }
                        rs.close();
                        rs = null;
                        stm.close();
                        stm = null;
                    }
                }
                catch (SQLException e)
                {
                    analyzeAndWrapSqlExceptionGenericSource("computeFunction failed " + e.getMessage(), e, source, con);
                }
                finally
                {
                    closeDatabaseObjects(con, stm, rs);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, dt);
            }
        }
        return result;
    }

    protected Connection getConnectionForQueryGenericSource(SqlQuery query, Object source)
    {
        this.prepareForQuery(query);
        return this.getConnectionGenericSource(source);
    }

    protected Connection getConnectionForReadGenericSource(Object source, boolean mustLock)
    {
        this.prepareTransactionalOperation(mustLock ? MithraTransaction.OperationMode.TRANSACTIONAL_READ : MithraTransaction.OperationMode.READ);
        return this.getConnectionGenericSource(source);
    }

    protected Connection getConnectionForReadGenericSource(Object source)
    {
        this.prepareTransactionalOperation(MithraTransaction.OperationMode.READ);
        return this.getConnectionGenericSource(source);
    }

    protected Connection getConnectionForTempWriteGenericSource(Object source, TupleTempContext tupleTempContext)
    {
        return this.getConnectionForTempWriteGenericSource(source, tupleTempContext.isForQuery());
    }

    protected Connection getConnectionForTempWriteGenericSource(Object source, boolean isForQuery)
    {
        this.prepareTransactionalOperation(isForQuery ? MithraTransaction.OperationMode.TEMP_WRITE_FOR_READ : MithraTransaction.OperationMode.TEMP_WRITE_FOR_WRITE);
        return this.getConnectionGenericSource(source);
    }

    protected Connection getConnectionForWriteGenericSource(Object source)
    {
        this.prepareTransactionalOperation(MithraTransaction.OperationMode.WRITE);
        return this.getConnectionGenericSource(source);
    }

    public String getFullyQualifiedTableNameGenericSource(Object source)
    {
        String schema = this.getSchemaGenericSource(source);
        String tableName = getTableNameGenericSource(source);
        return this.getDatabaseTypeGenericSource(source).getFullyQualifiedTableName(schema, tableName);
    }

    public String getTableNameForQuery(SqlQuery query, MapperStackImpl mapperStack, int sourceNumber)
    {
        Object source = getSourceAttributeValueGeneric(query, mapperStack, sourceNumber);
        return this.getFullyQualifiedTableNameGenericSource(source);
    }

    public Cursor findCursor(AnalyzedOperation analyzedOperation, Filter postLoadFilter, OrderBy orderby, int rowcount, boolean bypassCache, int maxParallelDegree, boolean forceImplicitJoin)
    {
        return new DatabaseCursor(analyzedOperation, postLoadFilter, orderby, rowcount, false);
    }

    protected CachedQuery find(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount)
    {
        return findSequential(analyzedOperation, orderby, forRelationship, rowcount, false);
    }

    public CachedQuery find(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount, int numberOfThreads, boolean bypassCache, boolean forceImplicitJoin)
    {
        if (rowcount > 0 || numberOfThreads == 1)
        {
            return this.findSequential(analyzedOperation, orderby, forRelationship, rowcount, forceImplicitJoin);
        }
        return this.findParallel(analyzedOperation, orderby, forRelationship, numberOfThreads, forceImplicitJoin);
    }

    protected CachedQuery findSequential(AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int rowcount, boolean forceImplicitJoin)
    {
        long startTime = System.currentTimeMillis();
        SqlQuery query = new SqlQuery(analyzedOperation, orderby, forceImplicitJoin);
        if (rowcount > 0)
        {
            query.setForceServerSideOrderBy(true);
        }
        CachedQuery cachedQuery = new CachedQuery(analyzedOperation.getOriginalOperation(), orderby);
        CachedQuery cachedQuery2 = createSecondCachedQuery(analyzedOperation, orderby, cachedQuery);
        int sourceCount = query.getNumberOfSources();
        final MithraFastList result = new MithraFastList();
        Cache cache = this.getMithraObjectPortal().getCache();
        boolean reachedMaxRowCount = false;
        boolean mustResetRowCount = false;
        List portalList = cachedQuery.getPortalList();
        ObjectWithMapperStack[] asOfOpWithStacks = this.getAsOfOpWithStacks(query, analyzedOperation);

        for (int sourceNum = 0; sourceNum < sourceCount && !reachedMaxRowCount; sourceNum++)
        {
            Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
            String databaseIdentifier = this.getDatabaseIdentifierGenericSource(source);
            for (int p = 0; p < portalList.size(); p++)
            {
                MithraObjectPortal portal = (MithraObjectPortal) portalList.get(p);
                portal.registerForNotification(databaseIdentifier);
            }

            DatabaseType dt = this.getDatabaseTypeGenericSource(source);
            final TimeZone timeZone = getDatabaseTimeZoneGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, dt, timeZone);
            try
            {
                Connection con = null;
                ResultSet rs = null;
                PreparedStatement stm = null;
                String lastStatement = null;
                try
                {
                    con = this.getConnectionForQueryGenericSource(query, source);
                    for (int q = 0; q < queries; q++)
                    {
                        long queryStartTime = System.currentTimeMillis();
                        mustResetRowCount = limitRowCount(rowcount, dt, con);
                        lastStatement = prepareStatementString(analyzedOperation, rowcount, query, source, dt, q, con);
                        stm = prepareDatabaseStatement(query, con, lastStatement);
                        rs = stm.executeQuery();
                        int numberOfResultsBefore = result.size();
                        reachedMaxRowCount = processResultSet(rs, result, source, asOfOpWithStacks, cache, dt, rowcount, timeZone);

                        if (this.statsListener != null)
                        {
                            this.statsListener.processRetrieval(source, new PrintableStatementBuilder(lastStatement, query), result.size() - numberOfResultsBefore, queryStartTime, this.getClass());
                        }
                        if (reachedMaxRowCount)
                        {
                            reachedMaxRowCount = rs.next();
                        }
                        rs.close();
                        rs = null;
                        stm.close();
                        stm = null;
                    }
                }
                catch (SQLException e)
                {
                    this.analyzeAndWrapSqlExceptionGenericSource("find failed for statement " + lastStatement + "\nwith message: " + e.getMessage(), e, source, con);
                }
                finally
                {
                    if (mustResetRowCount)
                    {
                        dt.setInfiniteRowCount(con);
                    }
                    this.closeDatabaseObjects(con, stm, rs);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, dt);
            }
        }
        return processResults(orderby, forRelationship, startTime, query, cachedQuery, cachedQuery2, result, reachedMaxRowCount);
    }

    private CachedQuery processResults(OrderBy orderby,
                                       boolean forRelationship,
                                       long startTime,
                                       SqlQuery query,
                                       CachedQuery cachedQuery,
                                       CachedQuery cachedQuery2,
                                       List result,
                                       boolean reachedMaxRowCount)
    {
        processInMemoryDistinct(query, result);
        this.getPerformanceData().recordTimeForFind(result.size(), startTime);
        if (this.getSqlLogger().isDebugEnabled())
        {
            long totalTime = System.currentTimeMillis() - startTime;
            this.getSqlLogger().debug("retrieved " + result.size() + " objects, " +
                    (result.size() > 0 ? ((double) totalTime) / result.size() + " ms per" : totalTime + " ms"));
        }
        if (orderby != null && !orderby.mustUseServerSideOrderBy() && result.size() > 1)
        {
            Collections.sort(result, orderby);
        }
        return cacheQuery(forRelationship, cachedQuery, cachedQuery2, result, reachedMaxRowCount);
    }

    protected CachedQuery findParallel(final AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship, int parallelCount, boolean forceImplicitJoin)
    {
        long startTime = System.currentTimeMillis();
        SqlQuery query = new SqlQuery(analyzedOperation, orderby, forceImplicitJoin);
        CachedQuery cachedQuery = new CachedQuery(analyzedOperation.getOriginalOperation(), orderby);
        CachedQuery cachedQuery2 = createSecondCachedQuery(analyzedOperation, orderby, cachedQuery);
        List portalList = cachedQuery.getPortalList();

        List result = this.parallelFetchForAllSources(analyzedOperation, query, portalList, parallelCount);
        return processResults(orderby, forRelationship, startTime, query, cachedQuery, cachedQuery2, result, false);
    }

    private List parallelFetchForAllSources(final AnalyzedOperation analyzedOperation,
                                            final SqlQuery query,
                                            List portalList,
                                            int parallelCount)
    {
        AutoShutdownThreadExecutor threadExecutor = new AutoShutdownThreadExecutor(parallelCount, "Mithra Parallel Find");
        threadExecutor.setTimeoutInMilliseconds(100);

        try
        {
            int sourceCount = query.getNumberOfSources();
            final Cache cache = this.getMithraObjectPortal().getCache();
            final LinkedBlockingQueue output = new LinkedBlockingQueue();

            List result = new MithraFastList();

            for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
            {
                this.parallelFetchForDataSource(analyzedOperation, query, portalList, threadExecutor, cache, output, result, sourceNum);
            }
            return result;
        }
        finally
        {
            threadExecutor.shutdown();
        }
    }

    private void parallelFetchForDataSource(final AnalyzedOperation analyzedOperation,
                                            final SqlQuery query,
                                            List portalList,
                                            AutoShutdownThreadExecutor threadExecutor,
                                            final Cache cache,
                                            final LinkedBlockingQueue output,
                                            List result,
                                            final int sourceNum)
    {
        final ObjectWithMapperStack[] asOfOpWithStacks = this.getAsOfOpWithStacks(query, analyzedOperation);

        final Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
        String databaseIdentifier = this.getDatabaseIdentifierGenericSource(source);
        for (int p = 0; p < portalList.size(); p++)
        {
            MithraObjectPortal portal = (MithraObjectPortal) portalList.get(p);
            portal.registerForNotification(databaseIdentifier);
        }

        final DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        final TimeZone timeZone = getDatabaseTimeZoneGenericSource(source);
        int queries = query.prepareQueryForSource(sourceNum, dt, timeZone, true);
        try
        {
            final CountingLock countingLock = new CountingLock();
            for (int q = 0; q < queries; q++)
            {
                final int queryNum = q;
                Runnable runnable = new Runnable()
                {
                    public void run()
                    {
                        Connection con = null;
                        ResultSet rs = null;
                        PreparedStatement stm = null;
                        String lastStatement = null;
                        MithraFastList perThreadResult = new MithraFastList();
                        try
                        {
                            countingLock.lockForTurn(queryNum);
                            con = getConnectionForQueryGenericSource(query, source);
                            lastStatement = prepareStatementString(analyzedOperation, 0, query, source, dt, queryNum, con);
                            stm = prepareDatabaseStatement(query, con, lastStatement);
                            countingLock.releaseTurn(queryNum);

                            rs = stm.executeQuery();
                            processResultSet(rs, perThreadResult, source, asOfOpWithStacks, cache, dt, 0, timeZone);
                            rs.close();
                            rs = null;
                            stm.close();
                            stm = null;
                            output.add(perThreadResult);
                        }
                        catch (SQLException e)
                        {
                            try
                            {
                                analyzeAndWrapSqlExceptionGenericSource("find failed for statement " + lastStatement + "\nwith message: " + e.getMessage(), e, source, con);
                            }
                            catch (MithraDatabaseException e1)
                            {
                                output.add(e1);
                            }
                        }
                        catch (Throwable t)
                        {
                            output.add(t);
                        }
                        finally
                        {
                            countingLock.releaseTurn(queryNum);
                            closeDatabaseObjects(con, stm, rs);
                        }
                    }
                };
                if (queries > 1)
                {
                    threadExecutor.submit(runnable);
                }
                else
                {
                    runnable.run();
                }
            }
            this.combineResults(output, result, queries);
        }
        finally
        {
            query.cleanTempForSource(sourceNum, dt);
        }
    }

    private void combineResults(LinkedBlockingQueue output, List result, int queries)
    {
        for (int q = 0; q < queries; q++)
        {
            try
            {
                Object o = output.take();
                if (o instanceof RuntimeException)
                {
                    throw (RuntimeException) o;
                }
                if (o instanceof Throwable)
                {
                    throw new RuntimeException("unexpected exception", (Throwable) o);
                }
                else
                {
                    result.addAll((List) o);
                }
            }
            catch (InterruptedException e)
            {
                throw new MithraDatabaseException("unexpected interrupt", e);
            }
        }
    }

    private CachedQuery createSecondCachedQuery(AnalyzedOperation analyzedOperation, OrderBy orderby, CachedQuery cachedQuery)
    {
        CachedQuery cachedQuery2 = null;
        if (analyzedOperation.isAnalyzedOperationDifferent())
        {
            cachedQuery2 = new CachedQuery(analyzedOperation.getAnalyzedOperation(), orderby);
            cachedQuery.setWasDefaulted();
        }
        return cachedQuery2;
    }

    private CachedQuery cacheQuery(boolean forRelationship, CachedQuery cachedQuery, CachedQuery cachedQuery2, List result, boolean reachedMaxRowCount)
    {
        cachedQuery.setResult(result);
        cachedQuery.setReachedMaxRetrieveCount(reachedMaxRowCount);
        cachedQuery.cacheQuery(forRelationship);
        if (cachedQuery2 != null)
        {
            cachedQuery2.setResult(result);
            cachedQuery2.setReachedMaxRetrieveCount(reachedMaxRowCount);
            cachedQuery2.cacheQuery(forRelationship);
        }
        return cachedQuery;
    }

    private void processInMemoryDistinct(SqlQuery query, final List result)
    {
        if (query.requiresInMemoryDistinct() && result.size() > 1)
        {
            FullUniqueIndex index = new FullUniqueIndex("", IdentityExtractor.getArrayInstance());
            for (int i = 0; i < result.size(); i++)
            {
                index.put(result.get(i));
            }
            result.clear();
            index.forAll(new DoUntilProcedure()
            {
                public boolean execute(Object object)
                {
                    result.add(object);
                    return false;
                }
            });
        }
    }

    private PreparedStatement prepareDatabaseStatement(SqlQuery query, Connection con, String statement)
            throws SQLException
    {
        PreparedStatement stm;
        stm = con.prepareStatement(statement);
        query.setStatementParameters(stm);
        return stm;
    }

    private String prepareStatementString(AnalyzedOperation analyzedOperation, int rowcount, SqlQuery query, Object source, DatabaseType dt, int q, Connection con)
            throws SQLException
    {
        query.prepareForQuery(q);
        String statement = findGetStatement(dt, query, analyzedOperation, rowcount);

        if (this.getSqlLogger().isDebugEnabled())
        {
            PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
            query.setStatementParameters(pps);
            this.logWithSource(this.getSqlLogger(), source, "connection:"+System.identityHashCode(con)+" find with: " + pps.getPrintableStatement());
        }
        return statement;
    }

    protected boolean processResultSet(ResultSet res, MithraFastList result, Object source,
                                       ObjectWithMapperStack[] asOfOpWithStacks, Cache cache, DatabaseType dt, int rowcount, TimeZone timeZone)
            throws SQLException
    {
        if (rowcount > 0)
        {
            while (res.next())
            {
                MithraDataObject newData = inflateDataGenericSource(res, source, dt);
                result.add(cache.getObjectFromData(newData));
                rowcount--;
                if (rowcount == 0)
                {
                    return true;
                }
            }
        }
        else
        {
            Object[] dataArray = getDataArray();
            int len = 0;
            while (res.next())
            {
                dataArray[len] = inflateDataGenericSource(res, source, dt);
                len++;
                if (len == DATA_ARRAY_SIZE)
                {
                    getManyObjects(cache, dataArray, len, result);
                    len = 0;
                }
            }
            if (len > 0)
            {
                getManyObjects(cache, dataArray, len, result);
            }
            returnDataArray(dataArray);
        }
        return false;
    }

    protected Object[] getDataArray()
    {
        Object[] result = arrayPool.poll();
        if (result == null) result = new Object[DATA_ARRAY_SIZE];
        return result;
    }

    protected void returnDataArray(Object[] dataArray)
    {
        if (arrayPool.size() < 4)
        {
            for (int i = 0; i < DATA_ARRAY_SIZE && dataArray[i] != null; i++) dataArray[i] = null;
            arrayPool.add(dataArray);
        }
    }

    private void getManyObjects(Cache nonDatedCache, Object[] dataArray, int len, MithraFastList result)
    {
        if (len == 1)
        {
            result.add(nonDatedCache.getObjectFromData((MithraDataObject) dataArray[0]));
        }
        else
        {
            nonDatedCache.getManyObjectsFromData(dataArray, len, false);
            result.zEnsureCapacity(result.size() + len);
            for (int i = 0; i < len; i++)
            {
                result.add(dataArray[i]);
            }
        }
    }

    protected String findGetStatement(DatabaseType dt, SqlQuery query, AnalyzedOperation analyzedOperation, int rowCount)
    {
        return dt.getSelect(this.getColumnListWithPk(SqlQuery.DEFAULT_DATABASE_ALIAS),
                query, null, MithraManagerProvider.getMithraManager().isInTransaction(), rowCount);
    }

    public ObjectWithMapperStack[] getAsOfOpWithStacks(SqlQuery query, AnalyzedOperation analyzedOperation)
    {
        return null;
    }

    public Timestamp[] getAsOfDates()
    {
        return null;
    }

    // Test Method
    public void insertData(List attributes, List dataObjects)
    {
        this.insertData(attributes, dataObjects, null);
    }

    public void insertData(List attributes, List dataObjects, Object source)
    {
        Connection con = null;
        PreparedStatement stm = null;

        MithraDataObject mdo;
        String tableName = this.getFullyQualifiedTableNameGenericSource(source);
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        int attrCount = attributes.size();

        for (int i = 0; i < attrCount; i++)
        {
            sql.append((((SingleColumnAttribute) attributes.get(i)).getColumnName()));
            if ((i + 1) < attrCount)
            {
                sql.append(',');
            }
        }
        sql.append(") values (");
        sql.append(this.createQuestionMarks(attrCount));
        sql.append(')');

        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            con.setAutoCommit(false);

            if (this.hasIdentity())
            {
                sql.insert(0, this.getDatabaseTypeGenericSource(source).getAllowInsertIntoIdentityStatementFor(tableName, " ON "));
            }
            stm = con.prepareStatement(sql.toString());

            for (int i = 0; i < dataObjects.size(); i++)
            {
                mdo = (MithraDataObject) dataObjects.get(i);
                if (this.getTestSqlLogger().isDebugEnabled())
                {
                    PrintablePreparedStatement pps = new PrintablePreparedStatement(sql.toString());
                    for (int j = 0; j < attrCount; j++)
                    {
                        ((SingleColumnAttribute) attributes.get(j)).setSqlParameters(pps, mdo, j + 1, TimestampAttribute.NO_CONVERSION_TIMEZONE, databaseType);
                    }
                    this.logWithSource(this.getTestSqlLogger(), source, "executing statement " + pps.getPrintableStatement());
                }

                for (int j = 0; j < attrCount; j++)
                {
                    if ((Attribute) attributes.get(j) instanceof TimestampAttribute)
                    {
                        Timestamp obj = ((TimestampAttribute) attributes.get(j)).timestampValueOf(mdo);
                        if (obj == NullDataTimestamp.getInstance())
                        {
                            ((TimestampAttribute) attributes.get(j)).setSqlParameter(j + 1, stm, null, TimestampAttribute.NO_CONVERSION_TIMEZONE, databaseType);
                        }
                        else
                        {
                            ((TimestampAttribute) attributes.get(j)).setSqlParameter(j + 1, stm, obj, TimestampAttribute.NO_CONVERSION_TIMEZONE, databaseType);
                        }
                    }
                    else
                    {
                        ((SingleColumnAttribute) attributes.get(j)).setSqlParameters(stm, mdo, j + 1, TimestampAttribute.NO_CONVERSION_TIMEZONE, databaseType);
                    }
                }
                stm.addBatch();
            }

            stm.executeBatch();
            if (this.hasIdentity())
            {
                stm = con.prepareCall(this.getDatabaseTypeGenericSource(source).getAllowInsertIntoIdentityStatementFor(tableName, " OFF "));
                stm.execute();
            }
            con.commit();
            con.setAutoCommit(true);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("insert data failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            try
            {
                if (con != null) con.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                this.analyzeAndWrapSqlExceptionGenericSource("insert data failed " + e.getMessage(), e, source, con);
            }
            this.closeStatementAndConnection(con, stm);
        }
    }

    protected final String trimString(String str)
    {
        if (str != null)
        {
            return str.trim();
        }
        return null;
    }

    protected void logWithSource(Logger logger, Object source, String text)
    {
        if (source != null)
        {
            logger.debug("source '" + source + "': " + text);
        }
        else
        {
            logger.debug(text);
        }
    }

    public void deleteAllRowsFromTestTable()
    {
        this.deleteAllRowsFromTestTable(null);
    }

    public void deleteAllRowsFromTestTable(Object source)
    {
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        String statement = databaseType.getDeleteStatementForTestTables() + this.getFullyQualifiedTableNameGenericSource(source);
        this.executeSqlStatementGenericSource(statement, source);
    }

    public Object dropTestTable()
    {
        return this.dropTestTable(null);
    }

    public Object dropTestTable(Object source)
    {
        String statement = "drop table " + this.getFullyQualifiedTableNameGenericSource(source);
        Connection con = null;
        Statement stm = null;

        MithraDatabaseException mdbe = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);

            if (this.getTestSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                this.logWithSource(this.getTestSqlLogger(), source, "executing statement " + pps.getPrintableStatement());
            }
            stm = con.createStatement();
            stm.execute(statement);
        }
        catch (SQLException e)
        {
            mdbe = new MithraDatabaseException("drop table failed " + e.getMessage(), e);
            mdbe.setRetriable(this.getDatabaseTypeGenericSource(source).loopNestedExceptionForFlagAndDetermineState(DatabaseType.RETRIABLE_FLAG, e));
            if (!mdbe.isRetriable())
            {
                mdbe.setTimedOut(this.getDatabaseTypeGenericSource(source).loopNestedExceptionForFlagAndDetermineState(DatabaseType.TIMED_OUT_FLAG, e));
            }
            return mdbe;
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
        return mdbe;
    }

    protected void createPrimaryKeyIndexForTestTable(Object source)
    {
        StringBuilder indexPK = new StringBuilder("CREATE UNIQUE INDEX ");
        String schemaName = this.getSchemaGenericSource(source);
        if (schemaName != null)
        {
            if (getDatabaseTypeGenericSource(source).indexRequiresSchemaName())
            {
                indexPK.append(schemaName).append('.');
            }
        }
        indexPK.append("I_").append(this.getTableNameGenericSource(source)).append("_PK ON ").append(this.getFullyQualifiedTableNameGenericSource(source)).append(" (");
        indexPK.append(this.getPrimaryKeyIndexColumns());
        indexPK.append(')');
        this.executeSqlStatementGenericSource(indexPK.toString(), source);
    }

    protected void executeSqlStatementGenericSource(String statement, Object source)
    {
        Connection con = null;
        Statement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            if (this.getTestSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                this.getTestSqlLogger().debug("executing statement " + pps.getPrintableStatement());
            }
            stm = con.createStatement();
            stm.execute(statement);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("statement failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    public List deserializeList(Operation op, ObjectInput in, boolean weak) throws IOException, ClassNotFoundException
    {
        Cache cache = this.getMithraObjectPortal().getCache();
        int size = in.readInt();
        List result = new MithraFastList(size);
        for (int i = 0; i < size; i++)
        {
            MithraDataObject data = this.deserializeFullData(in);
            if (weak)
            {
                result.add(cache.getObjectFromDataWithoutCaching(data));
            }
            else
            {
                result.add(cache.getObjectFromData(data));
            }
        }
        return result;
    }

    public void deserializeForReload(ObjectInput in) throws IOException, ClassNotFoundException
    {
        Cache cache = this.getMithraObjectPortal().getCache();
        PrimaryKeyIndex fullUniqueIndex = cache.getPrimayKeyIndexCopy();
        int size = in.readInt();
        MithraFastList newDataList = new MithraFastList();
        MithraFastList updatedDataList = new MithraFastList();
        for (int i = 0; i < size; i++)
        {
            Timestamp[] asOfDates = this.getAsOfDates();
            MithraDataObject data = this.deserializeFullData(in);
            if (asOfDates != null)
            {
                this.deserializeAsOfAttributes(in, asOfDates);
            }
            this.analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
        }
        List deletedData = fullUniqueIndex.getAll();
        cache.updateCache(newDataList, updatedDataList, deletedData);
    }

    public MithraDataObject refreshDatedObject(MithraDatedObject obj, boolean lockInDatabase)
            throws MithraDatabaseException
    {
        throw new RuntimeException("not implemented");
    }

    private List getDeletedData(PrimaryKeyIndex fullUniqueIndex)
    {
        List deletedData = fullUniqueIndex.getAll();
//        if (deletedData.size() > 0 && this.getFinder().getAsOfAttributes() != null)
        if (deletedData.size() > 0 && deletedData.get(0) instanceof MithraTransactionalObject)
        {
            for(int i=0;i<deletedData.size();i++)
            {
                deletedData.set(i, ((MithraTransactionalObject)deletedData.get(i)).zGetNonTxData());
            }
        }
        return deletedData;
    }

    private void getInsertUpdateListBasedOnDataFromDB(SqlQuery query, List newDataList, List updatedDataList, PrimaryKeyIndex fullUniqueIndex, SmallSet portalList)
    {
        int sourceCount = query.getNumberOfSources();
        for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
        {
            Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
            String databaseIdentifier = this.getDatabaseIdentifierGenericSource(source);
            for (int p = 0; p < portalList.size(); p++)
            {
                MithraObjectPortal portal = (MithraObjectPortal) portalList.get(p);
                portal.registerForNotification(databaseIdentifier);
            }
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, dt, this.getDatabaseTimeZoneGenericSource(source));
            try
            {
                Connection con = null;
                ResultSet rs = null;
                PreparedStatement stm = null;
                try
                {
                    con = this.getConnectionForQueryGenericSource(query, source);
                    for (int q = 0; q < queries; q++)
                    {
                        String statement = dt.getSelect(this.getColumnListWithPk(
                                SqlQuery.DEFAULT_DATABASE_ALIAS), query, null,
                                MithraManagerProvider.getMithraManager().isInTransaction(), 0);
                        if (this.getSqlLogger().isDebugEnabled())
                        {
                            PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                            query.setStatementParameters(pps);
                            this.getSqlLogger().debug("reload with: " + pps.getPrintableStatement());
                        }
                        stm = con.prepareStatement(statement);
                        query.setStatementParameters(stm);
                        rs = stm.executeQuery();
                        while (rs.next())
                        {
                            MithraDataObject data = inflateDataGenericSource(rs, source, dt);
                            analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
                        }
                        rs.close();
                        rs = null;
                        stm.close();
                        stm = null;
                    }
                }
                catch (SQLException e)
                {
                    this.analyzeAndWrapSqlExceptionGenericSource("reload failed " + e.getMessage(), e, source, con);
                }
                finally
                {
                    this.closeDatabaseObjects(con, stm, rs);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, dt);
            }
        }
    }

    protected void reloadCache(List analyzedOperations)
    {
        synchronized (this)
        {
            if (this.reloading)
            {
                return;
            }
            this.reloading = true;
        }
        try
        {
            long startTime = System.currentTimeMillis();
            MithraFastList newDataList = new MithraFastList();
            MithraFastList updatedDataList = new MithraFastList();
            Cache cache = this.getMithraObjectPortal().getCache();
            PrimaryKeyIndex fullUniqueIndex = cache.getPrimayKeyIndexCopy();
            for (int k = 0; k < analyzedOperations.size(); k++)
            {
                AnalyzedOperation analyzedOperation = (AnalyzedOperation) analyzedOperations.get(k);
                SqlQuery query = new SqlQuery(analyzedOperation, null, false);
                SmallSet portalList = new SmallSet(3);
                analyzedOperation.getAnalyzedOperation().addDependentPortalsToSet(portalList);

                this.getInsertUpdateListBasedOnDataFromDB(query, newDataList, updatedDataList, fullUniqueIndex, portalList);
            }

            List deletedData = this.getDeletedData(fullUniqueIndex);
            cache.updateCache(newDataList, updatedDataList, deletedData);
            if (this.getSqlLogger().isDebugEnabled())
            {
                long totalTime = System.currentTimeMillis() - startTime;
                this.getSqlLogger().debug("reload finished. new data: " + newDataList.size() +
                        " updated data: " + updatedDataList.size() + " deleted data: " +
                        deletedData.size() + " in " + totalTime + " ms");
            }
        }
        finally
        {
            synchronized (this)
            {
                this.reloading = false;
            }
        }
    }

    private Extractor[] getPrimaryKeyFor(RelatedFinder finder)
    {
        if (finder.getAsOfAttributes() == null)
        {
            return finder.getPrimaryKeyAttributes();
        }
        else
        {
            Extractor[] primKeyAttr = finder.getPrimaryKeyAttributes();
            AsOfAttribute[] asOfKeyAttr = finder.getAsOfAttributes();

            Extractor[] fullKey = new Extractor[primKeyAttr.length + asOfKeyAttr.length];
            System.arraycopy(primKeyAttr, 0, fullKey, 0, primKeyAttr.length);

            for (int i = 0; i < asOfKeyAttr.length; i++)
            {
                fullKey[i + primKeyAttr.length] = asOfKeyAttr[i].getFromAttribute();
            }
            return fullKey;
        }
    }

    public void loadFullCache()
    {
        List<Operation> ops = this.getOperationsForFullCacheLoad();
        MithraFastList<AnalyzedOperation> analyzedOps = new MithraFastList<AnalyzedOperation>(ops.size());
        for (int i = 0; i < ops.size(); i++)
        {
            analyzedOps.add(new AnalyzedOperation(ops.get(i)));
        }
        this.reloadCache(analyzedOps);
    }

    public void reloadFullCache()
    {
        this.loadFullCache();
    }

    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        synchronized (this)
        {
            if (this.reloading)
            {
                return  RenewedCacheStats.EMPTY_STATS;
            }
            this.reloading = true;
        }

        RenewedCacheStats result;
        try
        {
            long startTime = System.currentTimeMillis();
            MithraFastList<MithraDataObject> newDataList = new MithraFastList();
            MithraFastList<MithraDataObject> updatedDataList = new MithraFastList();

            MithraList<MithraObject> cachedData = this.getFinder().findMany(op);
            ExtractorBasedHashStrategy pkStrategy = ExtractorBasedHashStrategy.create(this.getPrimaryKeyFor(this.getFinder()));
            FullUniqueIndex fullUniqueIndex = new FullUniqueIndex(pkStrategy);
            if (this.getFinder().getAsOfAttributes() != null)
            {
                for (int i = 0; i < cachedData.size(); i++)
                {
                    fullUniqueIndex.put(cachedData.get(i).zGetCurrentData());
                }
            }
            else
            {
                fullUniqueIndex.addAll(cachedData);
            }

            // creates lists
            SqlQuery query = new SqlQuery(op, null, false);
            SmallSet portalList = new SmallSet(3);
            op.addDependentPortalsToSet(portalList);

            this.getInsertUpdateListBasedOnDataFromDB(query, newDataList, updatedDataList, fullUniqueIndex, portalList);

            List deletedData = this.getDeletedData(fullUniqueIndex);
            // run update
            this.getMithraObjectPortal().getCache().updateCache(newDataList, updatedDataList, deletedData);

            long totalTime = System.currentTimeMillis() - startTime;
            result = new RenewedCacheStats(newDataList, updatedDataList, deletedData, totalTime);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.getSqlLogger().debug("reload finished. new data: " + newDataList.size() +
                        " updated data: " + updatedDataList.size() + " deleted data: " +
                        deletedData.size() + " in " + totalTime + " ms");
            }

        }
        finally
        {
            synchronized (this)
            {
                this.reloading = false;
            }
        }
        return result;
    }

    public Map extractDatabaseIdentifiers(Operation op)
    {
        MithraDatabaseIdentifierExtractor extractor = new MithraDatabaseIdentifierExtractor();
        return extractor.extractDatabaseIdentifierMap(op);
    }

    public Map extractDatabaseIdentifiers(Set sourceAttributeValueSet)
    {
        MithraDatabaseIdentifierExtractor extractor = new MithraDatabaseIdentifierExtractor();
        return extractor.extractDatabaseIdentifierMap(this.getFinder(), sourceAttributeValueSet);
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        //nothing to do
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        //nothing to do
    }
    public void prepareForMassPurge(List mithraObjects)
    {
        //nothing to do

    }

    public void setTxParticipationMode(TxParticipationMode mode, MithraTransaction tx)
    {
        //nothing to do
    }

    private void appendSelectedTableFromClause(StringBuilder buffer, DatabaseType dt, Object source, boolean lock)
    {
        MithraObjectPortal portal = this.getMithraObjectPortal();
        MithraObjectPortal[] superClasses = portal.getSuperClassPortals();
        if (superClasses != null)
        {
            buffer.append(superClasses[0].getDatabaseObject().getFullyQualifiedTableNameGenericSource(source));
            buffer.append(' ').append(SqlQuery.DEFAULT_DATABASE_ALIAS);
            if (dt.hasPerTableLock())
            {
                buffer.append(' ').append(dt.getPerTableLock(lock));
            }
            for (int i = 1; i < superClasses.length; i++)
            {
                joinWithSuper(buffer, " JOIN ", SqlQuery.DEFAULT_DATABASE_ALIAS, superClasses[i], dt, lock, source);
            }
            joinWithSuper(buffer, " JOIN ", SqlQuery.DEFAULT_DATABASE_ALIAS, portal, dt, lock, source);
        }
        else
        {
            buffer.append(this.getFullyQualifiedTableNameGenericSource(source));
            buffer.append(' ').append(SqlQuery.DEFAULT_DATABASE_ALIAS);
            if (dt.hasPerTableLock())
            {
                buffer.append(' ').append(dt.getPerTableLock(lock));
            }
        }
    }

    private void joinWithSuper(StringBuilder buffer, String join, String tableAlias,
                               MithraObjectPortal portal, DatabaseType dt, boolean lock, Object source)
    {
        buffer.append(join);
        buffer.append(portal.getDatabaseObject().getFullyQualifiedTableNameGenericSource(source));
        buffer.append(' ').append(tableAlias).append(portal.getUniqueAlias());
        if (dt.hasPerTableLock())
        {
            buffer.append(' ').append(dt.getPerTableLock(lock));
        }
        buffer.append(" ON ");
        portal.appendJoinToSuper(buffer, tableAlias);
    }

    public MithraDataObject refresh(MithraDataObject oldData, boolean lockInDatabase) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        Object source = this.getSourceAttributeValueFromObjectGeneric(oldData);
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        boolean perTableLock = dt.hasPerTableLock();
        StringBuilder stmBuilder = new StringBuilder("select ");
        stmBuilder.append(getColumnListWithoutPkWithAliasOrOne());
        stmBuilder.append(" from ");
        this.appendSelectedTableFromClause(stmBuilder, dt, source, lockInDatabase);
        stmBuilder.append(" where ").append(this.getSqlWhereClauseForRefresh(oldData));

        if (!perTableLock)
        {
            stmBuilder.append(' ').append(dt.getPerStatementLock(lockInDatabase));
        }
        String statement = stmBuilder.toString();
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement stm = null;
        MithraDataObject refreshData = null;
        TimeZone databaseTimeZone;
        try
        {
            con = this.getConnectionForReadGenericSource(source, lockInDatabase);
            databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                this.setPrimaryKeyAttributesWithoutOptimistic(pps, 1, oldData, databaseTimeZone, dt);
                this.logWithSource(this.getSqlLogger(), source, "refreshing with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(statement);
            this.setPrimaryKeyAttributesWithoutOptimistic(stm, 1, oldData, databaseTimeZone, dt);
            rs = stm.executeQuery();
            if (rs.next())
            {
                refreshData = oldData.copy(false);

                this.inflateNonPkDataGenericSource(refreshData, rs, source, dt);
            }
            if (refreshData != null && rs.next())
            {
                throw new MithraDatabaseException("the primary key for " + getDomainClassName() + " is not unique!" +
                        " Non unique key: " + oldData.zGetPrintablePrimaryKey());
            }
            rs.close();
            rs = null;
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("refresh failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeDatabaseObjects(con, stm, rs);
        }
        this.getPerformanceData().recordTimeForRefresh(startTime);
        return refreshData;
    }

    protected String getColumnListWithoutPkWithAliasOrOne()
    {
        String columnListWithoutPkWithAlias = this.getColumnListWithoutPkWithAlias();
        if (columnListWithoutPkWithAlias.length() == 0)
        {
            columnListWithoutPkWithAlias = "1";
        }
        return columnListWithoutPkWithAlias;
    }

    protected String getSqlWhereClauseForRefresh(MithraDataObject firstDataToUpdate)
    {
        String sql;
        if (this.hasNullablePrimaryKeys())
        {
            sql = this.getPrimaryKeyWhereSqlWithNullableAttributeWithDefaultAlias(firstDataToUpdate);
        }
        else
        {
            sql = this.getPrimaryKeyWhereSqlWithDefaultAlias();
        }
        return sql;
    }

    public List findAggregatedData(Operation op, Map<String, MithraAggregateAttribute> nameToAggregateAttributeMap,
                                   Map<String, MithraGroupByAttribute> nameToGroupByAttributeMap, HavingOperation havingOperation, boolean bypassCache, Class bean)
    {
        long startTime = System.currentTimeMillis();
        AggregateDataConfig aggDataConfig = new AggregateDataConfig(nameToGroupByAttributeMap, nameToAggregateAttributeMap);
        List<com.gs.fw.common.mithra.MithraAggregateAttribute> aggregateAttributes = aggDataConfig.getAggregateAttributes();
        List<MithraGroupByAttribute> groupByAttributes = aggDataConfig.getGroupByAttributes();

        AggregateSqlQuery query = new AggregateSqlQuery(op, aggregateAttributes, groupByAttributes, havingOperation, null);
        query.setMaxUnionCount(1);
        int sourceCount = query.getNumberOfSources();
        List result = new MithraFastList();
        SmallSet portalList = new SmallSet(3);
        op.addDependentPortalsToSet(portalList);

        for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
        {
            Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);

            String databaseIdentifier = this.getDatabaseIdentifierGenericSource(source);
            for (int p = 0; p < portalList.size(); p++)
            {
                MithraObjectPortal portal = (MithraObjectPortal) portalList.get(p);
                portal.registerForNotification(databaseIdentifier);
            }

            DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
            TimeZone timezone = getDatabaseTimeZoneGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, databaseType, timezone);
            if (queries > 1)
            {
                throw new RuntimeException("cannot do aggregation with large in clauses");
            }
            String statement = null;
            try
            {
                statement = databaseType.getSelectForAggregatedData(query, aggregateAttributes, groupByAttributes);

                if (groupByAttributes != null && groupByAttributes.size() > 0)
                {
                    statement += this.createGroupByExpression(groupByAttributes, query);
                }

                if (havingOperation != null)
                {
                    statement += query.getHavingClause();
                }

                Connection con = null;
                ResultSet rs = null;
                PreparedStatement stm = null;
                try
                {
                    con = this.getConnectionForQueryGenericSource(query, source);

                    if (this.getSqlLogger().isDebugEnabled())
                    {
                        PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                        query.setStatementParameters(pps);
                        if (source != null)
                        {
                            this.getSqlLogger().debug("source '" + source + "': find with:  " + pps.getPrintableStatement());
                        }
                        else
                        {
                            this.getSqlLogger().debug("find with: " + pps.getPrintableStatement());
                        }
                    }
                    stm = con.prepareStatement(statement);
                    query.setStatementParameters(stm);
                    rs = stm.executeQuery();
                    Object[] scratchArray = new Object[1];
                    while (rs.next())
                    {
                        Object data;
                        if (bean.equals(AggregateData.class))
                        {
                            data = new AggregateData(aggDataConfig);
                        }
                        else
                        {
                            data = getInstance(bean);
                        }
                        this.inflateAggregateData(data, rs, aggregateAttributes, groupByAttributes, timezone, databaseType, scratchArray);
                        result.add(data);
                    }

                    rs.close();
                    rs = null;
                    stm.close();
                    stm = null;
                }
                catch (SQLException e)
                {
                    this.analyzeAndWrapSqlExceptionGenericSource("find failed for statement "+statement+"\nwith message: " + e.getMessage(), e, source, con);
                }
                finally
                {
                    this.closeDatabaseObjects(con, stm, rs);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, databaseType);
            }
        }

        if (this.getSqlLogger().isDebugEnabled())
        {
            long totalTime = System.currentTimeMillis() - startTime;
            this.getSqlLogger().debug("retrieved " + result.size() + " objects, " +
                    (result.size() > 0 ? totalTime / result.size() + " ms per" : totalTime + " ms"));
        }
        return result;
    }

    private Object getInstance(Class bean)
    {
        try
        {
            return bean.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new MithraBusinessException("Exception occurred instantiating " + bean.getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("Exception occurred instantiating " + bean.getName(), e);
        }
    }

    private String createGroupByExpression(List groupByAttributes, SqlQuery query)
    {
        StringBuilder groupByExpression = new StringBuilder(" group by ");
        for (int i = 0; i < groupByAttributes.size(); i++)
        {
            if (i != 0)
            {
                groupByExpression.append(" , ");
            }
            GroupByAttribute attr = (GroupByAttribute) groupByAttributes.get(i);
            Attribute singleColumnAttribute = attr.getAttribute();
            groupByExpression.append(singleColumnAttribute.getFullyQualifiedLeftHandExpression(query));
        }
        return groupByExpression.toString();
    }

    private void inflateAggregateData(Object object, ResultSet rs, List<MithraAggregateAttribute> aggregateAttributes, List<MithraGroupByAttribute> groupByAttributes, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray)
            throws SQLException
    {
        int pos = 1;
        int groupBySize = groupByAttributes.size();
        for (int i = 0; i < groupBySize; i++)
        {
            groupByAttributes.get(i).populateValueFromResultSet(pos++, i, rs, object, databaseTimezone, dt, scratchArray);
        }
        int aggAttributeSize = aggregateAttributes.size();
        for (int i = 0; i < aggAttributeSize; i++)
        {
            pos += aggregateAttributes.get(i).populateValueFromResultSet(pos, i + groupBySize, rs, object, databaseTimezone, dt, scratchArray);
        }
    }

    public void analyzeAndWrapSqlExceptionGenericSource(String msg, List mithraObjectList, SQLException e, Object source, Connection con)
            throws MithraDatabaseException
    {
        msg = "In object " + this.getClass().getName() + " " + msg;
        if (source != null)
        {
            msg = "source '" + source.toString() + "' " + msg;
        }

        this.reportNextException(e);
        MithraDatabaseException dbe = new MithraDatabaseException(msg + "(SQL code: " + e.getErrorCode() + " SQL State: " + e.getSQLState() + ')', e);
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        if (mithraObjectList != null && databaseType.violatesUniqueIndex(e))
        {
            msg = this.getUniqueIndexViolationMessage(mithraObjectList, msg);
            dbe = new MithraUniqueIndexViolationException(msg + "(SQL code: " + e.getErrorCode() + " SQL State: " + e.getSQLState() + ')', e);
        }
        dbe.setRetriable(databaseType.loopNestedExceptionForFlagAndDetermineState(DatabaseType.RETRIABLE_FLAG, e));
        if (!dbe.isRetriable())
        {
            dbe.setTimedOut(databaseType.loopNestedExceptionForFlagAndDetermineState(DatabaseType.TIMED_OUT_FLAG, e));
        }
        if (con != null && databaseType.isConnectionDead(e))
        {
            ClosableConnection primary = AbstractConnectionManager.getClosableConnection(con);
            if (primary != null)
            {
                primary.setDead();
            }
            else
            {
                this.getSqlLogger().warn("Connection is dead, but underlying pool is not closable. " + con.getClass().getName());
            }
            dbe.setRetriable(!(databaseType.isKilledConnection(e) && isNoRetries()));
            this.getConnectionManagerWrapper().cleanupDeadConnection(dbe, con);
        }
        throw dbe;
    }

    private boolean isNoRetries()
    {
        synchronized (NO_RETRY_THREAD_NAMES)
        {
            boolean noRetries = NO_RETRY_THREAD_NAMES.contains(Thread.currentThread().getName());
            if (noRetries)
            {
                resetNoRetryThreadNames();
            }
            return noRetries;
        }
    }

    private String getUniqueIndexViolationMessage(List mithraObjectList, String msg)
    {
        int mithraDataObjectListSize = mithraObjectList.size();
        if (mithraDataObjectListSize > 1)
        {
            for (int i = 0; i < mithraDataObjectListSize; i++)
            {
                MithraObject o = (MithraObject) mithraObjectList.get(i);
                MithraDataObject data = o.zGetCurrentData();
                if (data == null && o instanceof MithraTransactionalObject)
                {
                    data = ((MithraTransactionalObject) o).zGetTxDataForRead();
                }
                if (data != null)
                {
                    msg += " Primary Key: " + data.zGetPrintablePrimaryKey();
                }
            }
        }
        else
        {
            msg += " Primary Key: " + ((MithraDataObject) mithraObjectList.get(0)).zGetPrintablePrimaryKey();
        }
        return msg;
    }

    public void analyzeAndWrapSqlExceptionGenericSource(String msg, SQLException e, Object source, Connection con) throws MithraDatabaseException
    {
        this.analyzeAndWrapSqlExceptionGenericSource(msg, null, e, source, con);
    }

    public void analyzeAndWrapSqlException(String msg, SQLException e, Connection con) throws MithraDatabaseException
    {
        this.analyzeAndWrapSqlExceptionGenericSource(msg, e, null, con);
    }

    public void analyzeAndWrapSqlException(String msg, SQLException e, Connection con, Object source) throws MithraDatabaseException
    {
        this.analyzeAndWrapSqlExceptionGenericSource(msg, e, source, con);
    }

    protected String getFullyQualifiedChildQueueTableNameGenericSource(Object source)
    {
        String schema = this.getSchemaGenericSource(source);
        String tableName = this.getTableNameGenericSource(source);
        return this.getDatabaseTypeGenericSource(source).getFullyQualifiedTableName(schema, "ap_" + tableName);
    }

    public void deleteAllReplicationNotificationData()
    {
        StringBuilder deleteSql = new StringBuilder("delete from ").append(this.getFullyQualifiedChildQueueTableNameGenericSource(null));
        executeSqlStatementGenericSource(deleteSql.toString(), null);
    }

    public void dropChildQueueTestTable()
    {
        StringBuilder statement = new StringBuilder("drop table ").append(this.getFullyQualifiedChildQueueTableNameGenericSource(null));
        executeSqlStatementGenericSource(statement.toString(), null);
    }

    protected void checkNullPrimitive(ResultSet rs, MithraDataObject data, String name)
            throws SQLException
    {
        if (rs.wasNull())
            throw new MithraBusinessException("attribute '" + name + "' is null in database but is not marked as nullable in mithra xml for primary key / "
                    + data.zGetPrintablePrimaryKey());
    }

    public boolean hasIdentity()
    {
        return false;
    }

    public void setIdentity(Connection conn, Object source, MithraDataObject mithraDataObject) throws SQLException
    {
    }

    public String getColumnsForBulkInsertCreation(DatabaseType dt)
    {
        return null; // we use in bulk insert. null means get it from the existing table. temp objects override this.
    }

    public String getTableNamePrefixGenericSource(Object source)
    {
        return this.getTableNameGenericSource(source);
    }

    public void deserializeAsOfAttributes(ObjectInput in, Timestamp[] asof) throws IOException, ClassNotFoundException
    {
        // subclass to override
    }

    protected SingleColumnAttribute[] getPersistentAttributes()
    {
        if (persistentAttributes == null)
        {
            Attribute[] attributes = this.getFinder().getPersistentAttributes();
            SingleColumnAttribute[] result = new SingleColumnAttribute[attributes.length];
            System.arraycopy(attributes, 0, result, 0, attributes.length);
            this.persistentAttributes = result;
            return result;
        }
        return persistentAttributes;

    }

    public boolean verifyTable(Object source)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        String schema = this.getSchemaGenericSource(source);
        Connection con = null;
        SingleColumnAttribute[] persistentAttributes = this.getPersistentAttributes();
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TableColumnInfo tableInfo = dt.getTableColumnInfo(con, schema, this.getTableNameGenericSource(source));
            if (tableInfo == null || tableInfo.getNumberOfColumns() != persistentAttributes.length)
            {
                return false;
            }
            for (SingleColumnAttribute attr : persistentAttributes)
            {
                if (!tableInfo.hasColumn(attr)) return false;
            }
            return true;
        }
        catch (SQLException e)
        {
            analyzeAndWrapSqlExceptionGenericSource("verify table failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            closeConnection(con);
        }

        return false;
    }

    public void createTestTable(Object source)
    {
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        StringBuilder sb = new StringBuilder("create table ").append(this.getFullyQualifiedTableNameGenericSource(source)).append(" ( ");
        appendColumnDefinitions(sb, dt, false);
        sb.append(" )");
        dt.appendTestTableCreationPostamble(sb);
        executeSqlStatementGenericSource(sb.toString(), source);

        if (this.getFinder().getAsOfAttributes() != null)
        {
            this.createUniqueIndexForTestTable(source);
        }
        else
        {
            this.createPrimaryKeyIndexForTestTable(source);
        }
    }

    public void appendColumnDefinitions(StringBuilder sb, DatabaseType dt, boolean mustBeIndexable)
    {
        SingleColumnAttribute[] attributes = this.getPersistentAttributes();
        appendColumnDefinitions(sb, dt, attributes, mustBeIndexable);
    }

    protected void appendColumnDefinitions(StringBuilder sb, DatabaseType dt, SingleColumnAttribute[] attributes, boolean mustBeIndexable)
    {
        for (int i = 0; i < attributes.length; i++)
        {
            if (i != 0)
            {
                sb.append(',');
            }
            attributes[i].appendColumnDefinition(sb, dt, getSqlLogger(), mustBeIndexable);
        }
    }

    private void createUniqueIndexForTestTable(Object source)
    {
        StringBuilder indexStmtFrom = new StringBuilder("CREATE UNIQUE INDEX ");
        StringBuilder indexStmtTo = new StringBuilder("CREATE UNIQUE INDEX ");

        String schemaName = this.getSchemaGenericSource(source);
        if (schemaName != null && getDatabaseTypeGenericSource(source).indexRequiresSchemaName())
        {
            indexStmtFrom.append(schemaName).append('.');
            indexStmtTo.append(schemaName).append('.');
        }

        indexStmtFrom.append("I_").append(this.getTableNameGenericSource(source)).append("_F ON ").append(this.getFullyQualifiedTableNameGenericSource(source)).append(" (");
        indexStmtTo.append("I_").append(this.getTableNameGenericSource(source)).append("_T ON ").append(this.getFullyQualifiedTableNameGenericSource(source)).append(" (");

        indexStmtFrom.append(getPrimaryKeyIndexColumns());
        indexStmtTo.append(getPrimaryKeyIndexColumns());

        AsOfAttribute[] asOfAttributes = this.getFinder().getAsOfAttributes();

        for (int i = 0; i < asOfAttributes.length; i++)
        {
            indexStmtFrom.append(',');
            indexStmtTo.append(',');
            indexStmtFrom.append(asOfAttributes[i].getFromAttribute().getColumnName());
            indexStmtTo.append(asOfAttributes[i].getToAttribute().getColumnName());
        }

        indexStmtFrom.append(')');
        indexStmtTo.append(')');

        executeSqlStatementGenericSource(indexStmtFrom.toString(), source);
        executeSqlStatementGenericSource(indexStmtTo.toString(), source);
    }

    protected List<List> segregateBySourceAttribute(List mithraObjects, Extractor sourceAttribute)
    {
        if (sourceAttribute == null || mithraObjects.size() == 1)
            return ListFactory.create(mithraObjects);
        MultiHashMap map = null;
        Object firstData = ((MithraTransactionalObject) mithraObjects.get(0)).zGetTxDataForRead();
        for (int i = 0; i < mithraObjects.size(); i++)
        {
            Object current = mithraObjects.get(i);
            MithraDataObject curData = ((MithraTransactionalObject) current).zGetTxDataForRead();
            if (map != null)
            {
                map.put(sourceAttribute.valueOf(curData), current);
            }
            else if (!sourceAttribute.valueEquals(firstData, curData))
            {
                map = new MultiHashMap();
                Object firstSource = sourceAttribute.valueOf(firstData);
                for (int j = 0; j < i; j++)
                {
                    map.put(firstSource, mithraObjects.get(j));
                }
                map.put(sourceAttribute.valueOf(curData), current);
            }
        }

        if (map != null)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(mithraObjects);
        }
    }

    public void destroyTempContext(String fullyQualifiedTableName, Object source, boolean isForQuery)
    {
        try
        {
            this.dropTempTable(source, fullyQualifiedTableName, isForQuery);
        }
        finally
        {
            try
            {
                this.getConnectionManagerWrapper().unbindConnection(source);
            }
            catch (SQLException e)
            {
                getLogger().error("Could not close connection", e);
            }
        }
    }

    public void insertTuples(TupleTempContext context, List list, int bulkInsertThreshold)
    {
        Extractor sourceAttribute = context.getTupleSourceExtractor();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateBySourceAttribute(list, sourceAttribute);
            for (int i = 0; i < segregated.size(); i++)
            {
                this.insertTuplesForSameSource(context, (List) segregated.get(i), bulkInsertThreshold);
            }
        }
        else
        {
            this.insertTuplesForSameSource(context, list, bulkInsertThreshold);
        }
    }

    private void insertTuplesForSameSource(TupleTempContext context, List list, int bulkInsertThreshold)
    {
        Extractor sourceAttribute = context.getTupleSourceExtractor();
        Object source = null;
        if (sourceAttribute != null)
        {
            source = sourceAttribute.valueOf(list.get(0));
        }
        insertTuplesForSameSource(context, list, bulkInsertThreshold, source);
    }

    public void insertTuplesForSameSource(TupleTempContext context, List list, int bulkInsertThreshold, Object source)
    {
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        if (bulkInsertThreshold > 0 && list.size() > bulkInsertThreshold && databaseType.hasBulkInsert())
        {
            bulkInsertTuplesForSameSource(context, list, databaseType, source);
        }
        else if (databaseType.hasMultiInsert())
        {
            multiInsertTuplesForSameSource(context, list, databaseType, source);
        }
        else
        {
            batchInsertTuplesForSameSource(context, list, databaseType, source);
        }
        updateStatisticsOnTempTable(context, databaseType, source);
    }

    private void updateStatisticsOnTempTable(TupleTempContext context, DatabaseType databaseType, final Object source)
    {
        String tableName = getOrCreateTupleTempTable(context, source);
        final String sql = databaseType.getUpdateTableStatisticsSql(tableName);
        if (sql != null)
        {
            Connection con = null;
            PreparedStatement stm = null;
            try
            {
                con = getConnectionGenericSource(source);
                if (getSqlLogger().isDebugEnabled())
                {
                    logWithSource(getSqlLogger(), source, "updating statistics with: " + sql);
                }
                stm = con.prepareStatement(sql);
                stm.executeUpdate();
                stm.close();
                stm = null;
            }
            catch (SQLException e)
            {
                analyzeAndWrapSqlExceptionGenericSource("update statistics failed " + e.getMessage(), null, e, source, con);
            }
            finally
            {
                closeStatementAndConnection(con, stm);
            }
        }
    }

    public void multiInsertTuplesForSameSource(TupleTempContext context, List mithraObjects, DatabaseType databaseType, Object source)
            throws MithraDatabaseException
    {
        String tableName = getOrCreateTupleTempTable(context, source);
        String sql = "insert into " + tableName;
        SingleColumnAttribute[] attributes = context.getPersistentTupleAttributes();

        sql += " (" + this.getTupleColumnNames(attributes) + ')';
        String questionMarks = this.getQuestionMarks(attributes.length);
        int currentBatchSize = 0;
        int listPos = 0;
        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForTempWriteGenericSource(source, context);

            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "multi inserting with: " + sql + " for " + mithraObjects.size() + " objects ");
            }

            int batchSize = databaseType.getMultiInsertBatchSize(attributes.length);
            int batches = mithraObjects.size() / batchSize;
            int firstBatchSize = batchSize;
            int remainder = mithraObjects.size() % batchSize;
            if (remainder > 0)
            {
                batches++;
                firstBatchSize = remainder;
            }
            currentBatchSize = firstBatchSize;
            String batchSql = sql + databaseType.createMultiInsertParametersStatement(questionMarks, firstBatchSize);
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(batchSql);
            }
            stm = con.prepareStatement(batchSql);
            listPos = this.multiInsertTupleOnce(stm, pps, firstBatchSize, mithraObjects, 0, databaseTimeZone, source, attributes);
            if (batches > 1 && firstBatchSize != batchSize)
            {
                stm.close();
                stm = null;
                currentBatchSize = batchSize;
                batchSql = sql + databaseType.createMultiInsertParametersStatement(questionMarks, batchSize);
                stm = con.prepareStatement(batchSql);
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps = new PrintablePreparedStatement(batchSql);
                }
            }
            for (int b = 1; b < batches; b++)
            {
                listPos = this.multiInsertTupleOnce(stm, pps, batchSize, mithraObjects, listPos, databaseTimeZone, source, attributes);
            }
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch insert failed " + e.getMessage(), mithraObjects.subList(listPos, listPos + currentBatchSize), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    private String getOrCreateTupleTempTable(TupleTempContext context, Object source)
    {
        PersisterId persisterId = this.getMithraObjectPortal().getPersisterId();
        String tableName;
        synchronized (context)
        {
            tableName = context.getFullyQualifiedTableName(source, persisterId);
            if (tableName == null)
            {
                if (MithraManagerProvider.getMithraManager().isInTransaction())
                {
                    tableName = createNonSharedTempTable(source, context.getNominalTableName(source, persisterId),
                            context.getPersistentTupleAttributes(), context.getPersistentTupleAttributes(), false, context.isForQuery());
                }
                else
                {
                    DatabaseType dt = this.getDatabaseTypeGenericSource(source);
                    if (dt.supportsSharedTempTable() && context.prefersMultiThreadedDataAccess())
                    {
                        tableName = createSharedTempTable(source, context.getNominalTableName(source, persisterId),
                                context.getPersistentTupleAttributes(), context.getPersistentTupleAttributes(), context.isForQuery());
                    }
                    else
                    {
                        bindConnection(context, source);
                        tableName = createNonSharedTempTable(source, context.getNominalTableName(source, persisterId),
                                context.getPersistentTupleAttributes(), context.getPersistentTupleAttributes(), true, context.isForQuery());
                    }
                }
                context.setFullyQualifiedTableName(source, persisterId, tableName, this.getMithraObjectPortal());
            }
        }
        return tableName;
    }

    private void bindConnection(TupleTempContext context, Object source)
    {
        Connection c = this.getConnectionForTempWriteGenericSource(source, context);
        this.getConnectionManagerWrapper().bindConnection(context, source, c);
        // by design, the connection won't be closed here.
    }

    public void batchInsertTuplesForSameSource(TupleTempContext context, List mithraObjects, DatabaseType databaseType, Object source)
            throws MithraDatabaseException
    {
        SingleColumnAttribute[] attributes = context.getPersistentTupleAttributes();
        String sql = "insert into " + this.getOrCreateTupleTempTable(context, source);
        sql += " (" + this.getTupleColumnNames(attributes) + ") values (" + this.getQuestionMarks(attributes.length) + ')';
        Connection con = null;
        PreparedStatement stm = null;
        int batchSize = 0;
        int currentPosition = 0;
        int listSize = mithraObjects.size();
        try
        {
            con = this.getConnectionForTempWriteGenericSource(source, context);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "batch inserting with: " + sql + " for " + mithraObjects.size() + " objects ");
            }
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(sql);
            }
            stm = con.prepareStatement(sql);
            batchSize = databaseType.getMaxPreparedStatementBatchCount(this.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes().length);
            if (batchSize <= 0) batchSize = listSize;
            int objectsInBatch = 0;
            for (int i = 0; i < listSize; i++)
            {
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps.clearParameters();
                    this.setTupleInsertAttributes(pps, mithraObjects.get(i), databaseTimeZone, 1, databaseType, attributes);
                    this.logWithSource(this.getBatchSqlLogger(), source, "batch inserting with: " + pps.getPrintableStatement());
                }
                this.setTupleInsertAttributes(stm, mithraObjects.get(i), databaseTimeZone, 1, databaseType, attributes);
                stm.addBatch();
                objectsInBatch++;

                if (objectsInBatch == batchSize)
                {
                    objectsInBatch = 0;
                    this.executeBatch(stm, true);
                    currentPosition += batchSize;
                }
            }
            if (objectsInBatch > 0)
            {
                this.executeBatch(stm, true);

            }
            stm.close();
            stm = null;
        }
        catch (SQLException e)
        {
            int endPosition = currentPosition + batchSize > listSize ? listSize : currentPosition + batchSize;
            this.analyzeAndWrapSqlExceptionGenericSource("batch insert failed " + e.getMessage(), mithraObjects.subList(currentPosition, endPosition), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    public int multiInsertTupleOnce(PreparedStatement stm, PrintablePreparedStatement pps, int batchSize,
                                    List mithraObjects, int listPos, TimeZone databaseTimeZone, Object source, SingleColumnAttribute[] persistentAttributes)
            throws SQLException
    {
        if (this.getBatchSqlLogger().isDebugEnabled())
        {
            pps.clearParameters();
        }
        int paramPos = 1;
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        for (int i = 0; i < batchSize; i++, listPos++)
        {
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                this.setTupleInsertAttributes(pps, mithraObjects.get(listPos), databaseTimeZone, paramPos, dt, persistentAttributes);
            }
            this.setTupleInsertAttributes(stm, mithraObjects.get(listPos), databaseTimeZone, paramPos, dt, persistentAttributes);
            paramPos += persistentAttributes.length;
        }
        if (this.getBatchSqlLogger().isDebugEnabled())
        {
            this.logWithSource(this.getBatchSqlLogger(), source, "batch inserting with: " + pps.getPrintableStatement());
        }
        setExpectedExecuteReturn(batchSize);
        int inserted = stm.executeUpdate();
        if (inserted != batchSize)
        {
            throw new MithraBusinessException("inserted only " + inserted + " when expecting " + batchSize);
        }
        return listPos;
    }

    private void setTupleInsertAttributes(PreparedStatement stm, Object data, TimeZone databaseTimeZone, int paramPos,
                                          DatabaseType dt, SingleColumnAttribute[] persistentAttributes) throws SQLException
    {
        for (int i = 0; i < persistentAttributes.length; i++)
        {
            persistentAttributes[i].setSqlParameters(stm, data, paramPos + i, databaseTimeZone, dt);
        }
    }

    public void bulkInsertTuplesForSameSource(TupleTempContext context, List listToInsert, DatabaseType databaseType, Object source) throws MithraDatabaseException
    {
        TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

        String schema = getSchemaForBulkInsert(databaseType, source);
        String tableName = getOrCreateTupleTempTable(context, source);

        BulkLoader bulkLoader = null;

        Connection con = null;
        Statement stm = null;
        String tempTableName = assignTempTableName(tableName);
        String tempDbSchemaName = databaseType.getTempDbSchemaName();
        if (tempDbSchemaName == null)
        {
            tempDbSchemaName = schema;
        }
        String fullyQualifiedTempTableName = databaseType.getFullyQualifiedTableName(tempDbSchemaName, tempTableName);
        DropBulkTempTableSynchronization dropSync = null;
        try
        {
            bulkLoader = this.createBulkLoaderGenericSource(source);
            Logger sqlLogger = this.getSqlLogger();
            StringBuilder sb = new StringBuilder();
            appendColumnDefinitions(sb, databaseType, context.getPersistentTupleAttributes(), true);
            con = this.getConnectionForTempWriteGenericSource(source, context);
            bulkLoader.initialize(databaseTimeZone, tempDbSchemaName, tableName,
                    context.getTupleAttributesAsAttributeArray(),
                    sqlLogger, tempTableName, sb.toString(), con);
            boolean createsTempTable = bulkLoader.createsTempTable();
            if (createsTempTable)
            {
                dropSync = new DropBulkTempTableSynchronization(source, tempTableName);
                MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
                if (tx != null)
                {
                    tx.registerSynchronization(dropSync);
                    dropSync = null;
                }
            }
            bulkLoader.bindObjectsAndExecute(listToInsert, con);
            bulkLoader.destroy();
            bulkLoader = null;
            if (createsTempTable)
            {
                String sql = "insert into " + tableName + " select * from " + fullyQualifiedTempTableName;
                if (sqlLogger.isDebugEnabled())
                {
                    sqlLogger.debug("Batch inserting with temp table: " + sql);
                }
                stm = con.createStatement();
                setExpectedExecuteReturn(listToInsert.size());
                int inserted = stm.executeUpdate(sql);
                stm.close();
                stm = null;
                if (inserted < listToInsert.size())
                {
                    throw new MithraDatabaseException("bulk insert did not insert the correct number of rows. Expected " + listToInsert.size() +
                            " but got " + inserted);
                }
            }
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("bulk insert failed " + e.getMessage(), listToInsert, e, source, con);
        }
        catch (BulkLoaderException e)
        {
            throw new MithraDatabaseException("bulk insert failed " + e.getMessage(), e);
        }
        finally
        {
            if (bulkLoader != null) bulkLoader.destroy();
            this.closeStatementAndConnection(con, stm);
            if (dropSync != null) dropSync.afterCompletion(Status.STATUS_COMMITTED);
        }
    }

    private String assignTempTableName(String tableName)
    {
        String subString = tableName;
        int cutStartIndex = tableName.lastIndexOf('.') + 1; // in case of schema
        if (tableName.charAt(cutStartIndex) == '#') cutStartIndex++;
        int cutIndex = Math.min(10 + cutStartIndex, tableName.length());
        if (cutIndex < tableName.length() || cutStartIndex > 0)
        {
            subString = tableName.substring(cutStartIndex, cutIndex);
        }
        return 'B' + TempTableNamer.getNextTempTableName() + subString;
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

    protected void checkUpdateCount(int[] results)
    {
        for (int i = 0; i < results.length; i++)
        {
            if (results[i] != Statement.SUCCESS_NO_INFO && results[i] != 1)
            {
                this.getSqlLogger().warn("batch command did not insert/update/delete the correct number of rows " + results[i] + " at position " + (i + 1));
            }
        }
    }

    protected int[] executeBatchAndHandleBatchException(PreparedStatement stm)
            throws SQLException
    {
        int[] results;
        try
        {
            results = stm.executeBatch();
        }
        catch (BatchUpdateException e)
        {
            this.reportWarnings(stm);
            results = e.getUpdateCounts();
            int firstFailure = 0;
            for (int i = 0; i < results.length; i++)
            {
                if (results[i] == Statement.EXECUTE_FAILED)
                {
                    if (firstFailure == 0)
                    {
                        firstFailure = i + 1;
                    }
                }
                else
                {
                    if (firstFailure != 0)
                    {
                        this.getSqlLogger().error("failed in batch execute. See warnings above. Failure in batched statement number(s) " + firstFailure + " to " + i);
                        firstFailure = 0;
                    }
                }
            }
            if (firstFailure != 0)
            {
                this.getSqlLogger().error("failed in batch execute. See warnings above. Failure in batched statement number(s) " + firstFailure + " to " + results.length);
            }
            throw e;
        }
        return results;
    }

    private static class CountingLock
    {
        private int currentNumber;
        private boolean aborted = false;

        public synchronized void lockForTurn(int turn)
        {
            while (!aborted)
            {
                if (turn < currentNumber)
                {
                    throw new RuntimeException("Should not get here");
                }
                if (turn == currentNumber)
                {
                    return;
                }
                try
                {
                    this.wait(100);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }

        public synchronized void releaseTurn(int turn)
        {
            if (turn == currentNumber)
            {
                currentNumber++;
                this.notifyAll();
            }
        }

        public synchronized void abort()
        {
            this.aborted = true;
        }
    }

    private class DropSynchronization extends ExceptionHandlingTask implements Synchronization, PostTransactionAction
    {
        private Object genericSource;
        private String tableName;
        private boolean synchronous;
        private boolean isForQuery;

        private DropSynchronization(Object genericSource, String tableName, boolean isForQuery)
        {
            this.genericSource = genericSource;
            this.tableName = tableName;
            this.isForQuery = isForQuery;
        }

        public void setSynchronous(boolean synchronous)
        {
            this.synchronous = synchronous;
        }

        public void afterCompletion(int i)
        {
            // we have to drop the table in case of both rollback and commit
            if (synchronous)
            {
                try
                {
                    this.execute();
                }
                catch (Exception e)
                {
                    logger.error("ignoring synchronous post transaction action", e);
                }
            }
            else
            {
                ExceptionCatchingThread.executeTaskIgnoringExceptions(this);
            }
        }

        @Override
        public void execute(Connection con)
        {
            Statement stm = null;
            try
            {
                MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
                MithraTransaction.OperationMode operationMode = null;
                if (tx != null)
                {
                    operationMode = tx.getOperationMode();
                    tx.setWriteOperationMode(this.isForQuery ? MithraTransaction.OperationMode.TEMP_WRITE_FOR_READ : MithraTransaction.OperationMode.TEMP_WRITE_FOR_WRITE);
                }
                stm = dropTable(con);
                if (tx != null)
                {
                    tx.setWriteOperationMode(operationMode);
                }
            }
            catch (SQLException e)
            {
                analyzeAndWrapSqlExceptionGenericSource("drop temp table failed " + e.getMessage(), e, genericSource, con);
            }
            finally
            {
                closeStatement(stm);
            }
        }

        @Override
        public void execute()
        {
            Connection con = null;
            Statement stm = null;
            try
            {
                con = getConnectionForTempWriteGenericSource(genericSource, isForQuery);
                stm = dropTable(con);
            }
            catch (SQLException e)
            {
                analyzeAndWrapSqlExceptionGenericSource("drop temp table failed " + e.getMessage(), e, genericSource, con);
            }
            finally
            {
                closeStatementAndConnection(con, stm);
            }
        }

        private Statement dropTable(Connection con) throws SQLException
        {
            Statement stm = con.createStatement();
            String sql = "drop table " + tableName;
            if (getSqlLogger().isDebugEnabled())
            {
                getSqlLogger().debug("dropping temp table with: " + sql);
            }
            stm.executeUpdate(sql);
            stm.close();
            stm = null;
            return stm;
        }

        public void beforeCompletion()
        {
            //nothing to do
        }
    }

    protected class DropBulkTempTableSynchronization implements Synchronization
    {
        private Object source;
        private String tempTableName;

        public DropBulkTempTableSynchronization(Object source, String tempTableName)
        {
            this.source = source;
            this.tempTableName = tempTableName;
        }

        public void afterCompletion(int i)
        {
            try
            {
                BulkLoader bulkLoader = createBulkLoaderGenericSource(source);
                bulkLoader.dropTempTable(this.tempTableName);
                bulkLoader.destroy();
            }
            catch (BulkLoaderException e)
            {
                getLogger().error("Could not drop temporary table tempdb.." + tempTableName +
                        " please drop this table manually", e);
            }
        }

        public void beforeCompletion()
        {
            // nothing to do
        }
    }

    public List<Operation> getOperationsForFullCacheLoad()
    {
        return this.loadOperationProvider.getOperationsForFullCacheLoad(this.getFinder());
    }

    protected MithraDataObject getMithraDataObjectForUpdate(MithraTransactionalObject mithraObject, List updateWrappers)
    {
        return ((AttributeUpdateWrapper) updateWrappers.get(0)).getDataToUpdate();
    }

    protected String getSqlWhereClauseForUpdate(MithraDataObject firstDataToUpdate)
    {
        String sql;
        if (this.hasNullablePrimaryKeys())
        {
            sql = " where " + this.getPrimaryKeyWhereSqlWithNullableAttribute(firstDataToUpdate);
        }
        else
        {
            sql = " where " + this.getPrimaryKeyWhereSql();
        }
        return sql;
    }

    public String getPrimaryKeyWhereSql()
    {
        throw new RuntimeException("not implemented");
    }

    protected String getSqlWhereClauseForDelete(MithraDataObject dataToDelete)
    {
        String sql;
        if (this.hasNullablePrimaryKeys())
        {
            sql = " where " + this.getPrimaryKeyWhereSqlWithNullableAttribute(dataToDelete);
        }
        else
        {
            sql = " where " + this.getPrimaryKeyWhereSql();
        }
        return sql;
    }

    protected MithraDataObject getDataForUpdate(MithraTransactionalObject obj)
    {
        return obj.zGetTxDataForRead();
    }

    protected List zFindForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        long startTime = System.currentTimeMillis();
        AnalyzedOperation analyzedOperation = new AnalyzedOperation(op);
        SqlQuery query = new SqlQuery(analyzedOperation, null, forceImplicitJoin);
        int sourceCount = query.getNumberOfSources();
        MithraFastList result = new MithraFastList();
        Cache cache = this.getMithraObjectPortal().getCache();

        for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
        {
            Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, dt, this.getDatabaseTimeZoneGenericSource(source));
            try
            {
                Connection con = null;
                ResultSet rs = null;
                PreparedStatement stm = null;
                String statement = null;
                try
                {
                    con = this.getConnectionForWriteGenericSource(source);

                    for (int q = 0; q < queries; q++)
                    {
                        query.prepareForQuery(q);
                        statement = dt.getSelect(this.getPkColumnList(SqlQuery.DEFAULT_DATABASE_ALIAS),
                                query, null, MithraManagerProvider.getMithraManager().isInTransaction(), 0);
                        if (this.getSqlLogger().isDebugEnabled())
                        {
                            PrintablePreparedStatement pps = new PrintablePreparedStatement(statement);
                            query.setStatementParameters(pps);
                            this.logWithSource(this.getSqlLogger(), source, "find for delete with: " + pps.getPrintableStatement());
                        }
                        stm = con.prepareStatement(statement);
                        query.setStatementParameters(stm);
                        rs = stm.executeQuery();
                        while (rs.next())
                        {
                            MithraDataObject newData = this.inflatePkDataGenericSource(rs, source, dt);
                            Object fromCache = cache.getObjectByPrimaryKey(newData, true);
                            if (fromCache != null) result.add(fromCache);
                        }
                        rs.close();
                        rs = null;
                        stm.close();
                        stm = null;
                    }
                }
                catch (SQLException e)
                {
                    this.analyzeAndWrapSqlExceptionGenericSource("mass delete failed with statement "+statement+"\nwith message " + e.getMessage(), e, source, con);
                }
                finally
                {
                    this.closeDatabaseObjects(con, stm, rs);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, dt);
            }
        }
        if (this.getSqlLogger().isDebugEnabled())
        {
            long totalTime = System.currentTimeMillis() - startTime;
            this.getSqlLogger().debug("retrieved " + result.size() + " objects, " +
                    (result.size() > 0 ? totalTime / result.size() + " ms per" : totalTime + " ms"));
        }
        return result;
    }

    public String getPkColumnList(String defaultDatabaseAlias)
    {
        throw new RuntimeException("only transactional objects should implemented");
    }

//------------------------------------------

    public String getInsertFields()
    {
        throw new RuntimeException("only transactional objects should implemented");
    }

    public String getInsertQuestionMarks()
    {
        throw new RuntimeException("only transactional objects should implemented");
    }

    public void setInsertAttributes(PreparedStatement stm, MithraDataObject dataObj, TimeZone databaseTimeZone, int pos, DatabaseType dt) throws SQLException
    {
        throw new RuntimeException("only transactional objects should implemented");
    }

    protected void zInsert(MithraDataObject dataToInsert) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        Object source = this.getSourceAttributeValueFromObjectGeneric(dataToInsert);
        String sql = "insert into " + this.getFullyQualifiedTableNameGenericSource(source);
        sql += '(' + this.getInsertFields() + ") values (" + this.getInsertQuestionMarks() + ')';
        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                this.setInsertAttributes(pps, dataToInsert, databaseTimeZone, 1, dt);
                this.logWithSource(this.getSqlLogger(), source, "insert with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(sql);
            this.setInsertAttributes(stm, dataToInsert, databaseTimeZone, 1, dt);
            this.setExpectedExecuteReturn(1);
            int inserted = stm.executeUpdate();
            if (inserted != 1)
            {
                reportBadInsert(stm.getWarnings(), inserted, 1);
            }

            stm.close();
            stm = null;

            if (dataToInsert.zHasIdentity())
            {
                this.setIdentity(con, source, dataToInsert);
            }

            //Send MithraNotification message
            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.INSERT, dataToInsert, source);
            this.getMithraObjectPortal().registerForNotification(dbid);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("insert failed " + e.getMessage(), ListFactory.create(dataToInsert), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
        this.getPerformanceData().recordTimeForInsert(1, startTime);
    }

    private void reportBadInsert(SQLWarning sqlWarning, int insertedCount, int expectedCount)
    {
        this.reportNextWarning(sqlWarning);
        throw new MithraBusinessException("inserted " + insertedCount + " rows when expecting " + expectedCount);
    }

    // update Start -----------------------------------------------------------------------------------------------------------
    protected void zUpdate(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper) throws MithraDatabaseException
    {
        this.zUpdate(mithraObject, ListFactory.create(wrapper));
    }

    protected void zUpdate(MithraTransactionalObject mithraObject, List updateWrappers) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        MithraDataObject firstData = this.getMithraDataObjectForUpdate(mithraObject, updateWrappers);
        (mithraObject).zGetCurrentData();
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);
        StringBuilder builder = new StringBuilder(30 + updateWrappers.size() * 12);
        builder.append("update ");
        builder.append(this.getFullyQualifiedTableNameGenericSource(source)).append(" set ");
        for (int i = 0; i < updateWrappers.size(); i++)
        {
            if (i > 0) builder.append(", ");
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
            builder.append(wrapper.getSetAttributeSql());
        }

        builder.append(this.getSqlWhereClauseForUpdate(firstData));
        String sql = builder.toString();
        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);
            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                int pos = setSqlParametersFromUpdates(updateWrappers, pps, databaseTimeZone, dt);
                this.setPrimaryKeyAttributes(pps, pos, firstData, databaseTimeZone, dt);
                this.logWithSource(this.getSqlLogger(), source, "update with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(sql);
            int pos = setSqlParametersFromUpdates(updateWrappers, stm, databaseTimeZone, dt);
            this.setPrimaryKeyAttributes(stm, pos, firstData, databaseTimeZone, dt);
            setExpectedExecuteReturn(1);
            int updatedRows = stm.executeUpdate();
            this.checkUpdatedRows(updatedRows, firstData);
            stm.close();
            stm = null;
            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEventForUpdate(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.UPDATE, firstData, updateWrappers, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("update failed " + e.getMessage(), ListFactory.create(firstData), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
        this.getPerformanceData().recordTimeForUpdate(1, startTime);
    }

    private void setExpectedExecuteReturn(int expected)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            tx.setExpectedExecuteReturn(expected);
        }
    }

    private int setSqlParametersFromUpdates(List updateWrappers, PreparedStatement stm, TimeZone databaseTimeZone, DatabaseType dt)
            throws SQLException
    {
        int pos = 1;
        for (int i = 0; i < updateWrappers.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
            pos += wrapper.setSqlParameters(stm, pos, databaseTimeZone, dt);
        }
        return pos;
    }

    protected MithraNotificationEventManager getNotificationEventManager()
    {
        return MithraManager.getInstance().getNotificationEventManager();
    }

    protected void checkUpdatedRows(int updatedRows, MithraDataObject data)
            throws MithraOptimisticLockException
    {
        if (updatedRows != 1)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (this.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking())
            {
                this.getMithraObjectPortal().getCache().markDirtyForReload(data, tx);
                MithraOptimisticLockException mithraOptimisticLockException = new MithraOptimisticLockException("optimistic lock failed on instance of " + getDomainClassName() + " with data " +
                        data.zGetPrintablePrimaryKey());
                if (tx.retryOnOptimisticLockFailure())
                {
                    mithraOptimisticLockException.setRetriable(true);
                }
                throw mithraOptimisticLockException;
            }
            throw new MithraDatabaseException("in trying to update instance of " + getDomainClassName() + " with primary key " +
                    data.zGetPrintablePrimaryKey() + ' ' + updatedRows + " were updated!");
        }
    }

    protected String getDomainClassName()
    {
        String finderClassName = this.getFullyQualifiedFinderClassName();
        return finderClassName.substring(0, finderClassName.length() - "Finder".length());
    }

// update end -----------------------------------------------------------------------------------------------------------

    // delete Start -----------------------------------------------------------------------------------------------------------

    protected void zDelete(MithraDataObject dataToDelete) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        Object source = this.getSourceAttributeValueFromObjectGeneric(dataToDelete);
        String sql = "delete from " + this.getFullyQualifiedTableNameGenericSource(source);

        sql += this.getSqlWhereClauseForDelete(dataToDelete);

        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);
            DatabaseType dt = this.getDatabaseTypeGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                this.setPrimaryKeyAttributes(pps, 1, dataToDelete, databaseTimeZone, dt);
                this.logWithSource(this.getSqlLogger(), source, "deleting with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(sql);
            this.setPrimaryKeyAttributes(stm, 1, dataToDelete, databaseTimeZone, dt);
            int deleted = stm.executeUpdate();
            stm.close();
            stm = null;
            checkDeletedRows(deleted, dataToDelete);

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.DELETE, dataToDelete, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("delete failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
        this.getPerformanceData().recordTimeForDelete(1, startTime);
    }

    protected void checkDeletedRows(int deletedRows, MithraDataObject data)
            throws MithraOptimisticLockException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (deletedRows != 1 && this.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking())
        {
            this.getMithraObjectPortal().getCache().markDirtyForReload(data, tx);
            MithraOptimisticLockException mithraOptimisticLockException = new MithraOptimisticLockException("optimistic lock failed on data " +
                    data.zGetPrintablePrimaryKey());
            if (tx.retryOnOptimisticLockFailure())
            {
                mithraOptimisticLockException.setRetriable(true);
            }
            throw mithraOptimisticLockException;
        }
    }

// delete End -----------------------------------------------------------------------------------------------------------


    // batchUpdateForSameSourceAttribute Start -----------------------------------------------------------------------------------------------------------
    protected void zBatchUpdateForSameSourceAttribute(List<UpdateOperation> updateOperations, BatchUpdateOperation batchUpdateOperation)
    {
        UpdateOperation firstOperation = updateOperations.get(0);
        MithraDataObject firstData = this.getDataForUpdate(firstOperation);
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);

        if (databaseType.getUpdateViaInsertAndJoinThreshold() > 0 &&
                databaseType.getUpdateViaInsertAndJoinThreshold() < updateOperations.size() &&
                this.getFinder().getVersionAttribute() == null &&
                !batchUpdateOperation.isIncrement() &&
                batchUpdateOperation.isEligibleForUpdateViaJoin())
        {
            zBatchUpdateViaInsertAndJoin(updateOperations, source, databaseType);
            return;
        }
        if (this.hasOptimisticLocking())
        {
            if (this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking() && !databaseType.canCombineOptimisticWithBatchUpdates())
            {
                //we'll do single updates
                for(int i=0;i<updateOperations.size();i++)
                {
                    UpdateOperation updateOperation = updateOperations.get(i);
                    zUpdate(updateOperation.getMithraObject(), updateOperation.getUpdates());
                }

                return;
            }
        }

        List firstUpdateWrappers = firstOperation.getUpdates();
        StringBuilder builder = new StringBuilder(30 + firstUpdateWrappers.size() * 12);
        builder.append("update ");
        builder.append(this.getFullyQualifiedTableNameGenericSource(source)).append(" set ");
        for (int i = 0; i < firstUpdateWrappers.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) firstUpdateWrappers.get(i);
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append(wrapper.getSetAttributeSql());
        }

        builder.append(this.getSqlWhereClauseForBatchUpdateForSameSourceAttribute(firstData));
        String sql = builder.toString();
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "batch update of " + updateOperations.size() + " objects with: " + sql);
            }
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(sql);
            }
            stm = con.prepareStatement(sql);
            int batchSize = databaseType.getMaxPreparedStatementBatchCount(firstOperation.getUpdates().size() +
                    this.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes().length);
            if (batchSize < 0)
            {
                batchSize = updateOperations.size();
            }

            int objectsInBatch = 0;
            int batchStart = 0;
            for (int u = 0; u < updateOperations.size(); u++)
            {
                UpdateOperation operation = updateOperations.get(u);
                MithraDataObject data = this.getDataForUpdate(operation);
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps.clearParameters();
                    int pos = operation.setSqlParameters(pps, databaseTimeZone, databaseType);
                    this.setPrimaryKeyAttributes(pps, pos, data, databaseTimeZone, databaseType);
                    this.logWithSource(this.getBatchSqlLogger(), source, "batch updating with: " + pps.getPrintableStatement());
                }
                int pos = operation.setSqlParameters(stm, databaseTimeZone, databaseType);
                this.setPrimaryKeyAttributes(stm, pos, data, databaseTimeZone, databaseType);
                operation.setUpdated();
                stm.addBatch();
                objectsInBatch++;

                if (objectsInBatch == batchSize)
                {
                    this.executeBatchForUpdateOperations(stm, updateOperations, batchStart);
                    objectsInBatch = 0;
                    batchStart = u + 1;
                }
            }
            if (objectsInBatch > 0)
            {
                this.executeBatchForUpdateOperations(stm, updateOperations, batchStart);
            }
            stm.close();
            stm = null;

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEventForBatchUpdate(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.UPDATE, updateOperations, firstUpdateWrappers, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch update failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }

    }

    private Function<UpdateOperation, MithraDataObject>DATA_FOR_UPDATE = new Function<UpdateOperation, MithraDataObject>()
    {
        @Override
        public MithraDataObject valueOf(UpdateOperation updateOperation)
        {
             return getDataForUpdate(updateOperation);
        }
    };

    protected void batchDeleteForSameSourceAttributeViaTempJoin(
            List<? extends MithraTransactionalObject> mithraTransactionalObjects,
            Object source, Attribute[] primaryKeyAttributes) throws MithraDatabaseException
    {
        List<MithraDataObject> dataObjects = FastList.newList(mithraTransactionalObjects.size());
        for (MithraTransactionalObject each : mithraTransactionalObjects)
        {
            dataObjects.add(each.zGetTxDataForRead());
        }

        MithraDataObject firstData = dataObjects.get(0);

        MithraFastList<Attribute> prototypeAttributes = new MithraFastList(primaryKeyAttributes.length + 1);
        MithraFastList<Attribute> nullAttributes = new MithraFastList(2);
        addPrimaryKeyAttributes(firstData, primaryKeyAttributes, prototypeAttributes, nullAttributes);
        int pkAttributeCount = prototypeAttributes.size();
        Attribute[] prototypeArray = new Attribute[prototypeAttributes.size()];
        prototypeAttributes.toArray(prototypeArray);
        TupleTempContext tempContext = null;
        try
        {
            tempContext = new TupleTempContext(prototypeArray, false);

            tempContext.setPrefersMultiThreadedDataAccess(false);
            this.getMithraObjectPortal().getMithraTuplePersister().insertTuplesForSameSource(tempContext,
                    new LazyListAdaptor(dataObjects, LazyTuple.createFactory(prototypeArray)), 0, source);

            StringBuilder builder = new StringBuilder("delete from " + this.getFullyQualifiedTableNameGenericSource(source));
            appendTempTableUpdateDeleteJoin(source, prototypeArray, nullAttributes, pkAttributeCount, tempContext, builder);
            String sql = builder.toString();
            executeBatchPurgeViaTemp(source, dataObjects.size(), sql);

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            this.getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.DELETE, mithraTransactionalObjects, source);
        }
        finally
        {
            if (tempContext != null) tempContext.destroy();
        }
    }

    private void zBatchUpdateViaInsertAndJoin(List<UpdateOperation> updateOperations, Object source, DatabaseType databaseType)
    {
        UpdateOperation firstOperation = updateOperations.get(0);
        MithraDataObject firstData = this.getDataForUpdate(firstOperation);

        Attribute[] primaryKeyAttributes = this.getFinder().getPrimaryKeyAttributes();
        List firstUpdates = firstOperation.getUpdates();
        MithraFastList<Attribute> prototypeAttributes = new MithraFastList(primaryKeyAttributes.length + firstUpdates.size());
        MithraFastList<Attribute> nullAttributes = new MithraFastList(2);
        boolean optimistic = this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking();
        addPrimaryKeyAttributes(firstData, primaryKeyAttributes, prototypeAttributes, nullAttributes);

        addOptimisticAttribute(new LazyListAdaptor(updateOperations, DATA_FOR_UPDATE), prototypeAttributes, optimistic);
        int pkAttributeCount = prototypeAttributes.size();

        for (int i = 0; i < firstUpdates.size(); i++)
        {
            AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) firstUpdates.get(i);
            prototypeAttributes.add(wrapper.getAttribute());
        }
        Attribute[] prototypeArray = new Attribute[prototypeAttributes.size()];
        prototypeAttributes.toArray(prototypeArray);
        TupleTempContext tempContext = null;
        try
        {
            tempContext = new TupleTempContext(prototypeArray, false);
            this.getMithraObjectPortal().getMithraTuplePersister().insertTuplesForSameSource(
                    tempContext, new LazyListAdaptor(updateOperations, UpdateOperationTupleAdaptor.createFactory(prototypeArray)), 0, source);

            StringBuilder builder = new StringBuilder(30 + firstUpdates.size() * 12);

            databaseType.setBatchUpdateViaJoinQuery(source, firstUpdates, prototypeArray, nullAttributes, pkAttributeCount, tempContext, this.getMithraObjectPortal(), this.getFullyQualifiedTableNameGenericSource(source), builder);

            String sql = builder.toString();

            executeBatchUpdateViaTemp(source, new LazyListAdaptor(updateOperations, DATA_FOR_UPDATE),
                    databaseType, nullAttributes, optimistic, pkAttributeCount, prototypeArray, tempContext, sql, firstUpdates, false);
            for (int i = 0; i < updateOperations.size(); i++)
            {
                UpdateOperation op = updateOperations.get(i);
                op.setUpdated();
            }
            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEventForBatchUpdate(dbid, this.getFullyQualifiedFinderClassName(),
                    MithraNotificationEvent.UPDATE, updateOperations, firstUpdates, source);
        }
        finally
        {
            if (tempContext != null)
            {
                tempContext.destroy();
            }
        }
    }

    private void executeBatchUpdateViaTemp(
            Object source, List dataList, DatabaseType databaseType,
            MithraFastList<Attribute> nullAttributes, boolean optimistic, int pkAttributeCount, Attribute[] prototypeArray,
            TupleTempContext tempContext, String sql, List updates, boolean isMultiUpdate)
    {
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                if (isMultiUpdate)
                {
                    setSqlParametersFromUpdates(updates, pps, databaseTimeZone, databaseType);
                }
                this.logWithSource(this.getSqlLogger(), source, "batch update of " + dataList.size() + " objects with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(sql);
            if (isMultiUpdate)
            {
                setSqlParametersFromUpdates(updates, stm, databaseTimeZone, databaseType);
            }
            int updated = stm.executeUpdate();
            stm.close();
            stm = null;
            boolean throwOptimisticException = false;
            if (optimistic)
            {
                if (updated != dataList.size())
                {
                    throwOptimisticException = true;
                    determineDirtyData(source, tempContext, prototypeArray, nullAttributes, pkAttributeCount, con, databaseType,
                            dataList, databaseTimeZone, updates, isMultiUpdate);
                }
            }
            else
            {
                if (updated != dataList.size())
                {
                    this.getSqlLogger().warn("batch command did not update the correct number of rows. Expecting " + dataList.size() + " but got " + updated);
                }
            }
            if (throwOptimisticException)
            {
                throwOptimisticLockException();
            }

        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch update failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    private void executeBatchPurgeViaTemp(Object source, int size, String sql)
    {
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            con = this.getConnectionForWriteGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                this.logWithSource(this.getSqlLogger(), source, "batch update of " + size + " objects with: " + pps.getPrintableStatement());
            }
            stm = con.prepareStatement(sql);
            int purged = stm.executeUpdate();
            stm.close();
            stm = null;
            if (purged != size)
            {
                this.getSqlLogger().warn("batch command did not purge the correct number of rows. Expecting " + size + " but got " + purged);
            }

        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch purge failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    private void addOptimisticAttribute(List dataList, MithraFastList<Attribute> prototypeAttributes, boolean optimistic)
    {
        if (optimistic)
        {
            Attribute versionAttribute = (Attribute) this.getFinder().getVersionAttribute();
            if (versionAttribute != null)
            {
                prototypeAttributes.add(versionAttribute.zGetShadowAttribute());
            }
            AsOfAttribute[] asOfAttributes = this.getFinder().getAsOfAttributes();
            if (asOfAttributes != null)
            {
                Attribute optimisticProcessing = this.getOptimisticKey(asOfAttributes, dataList);
                if (optimisticProcessing != null)
                {
                    prototypeAttributes.add(optimisticProcessing);
                }
            }
        }
    }

    private void addPrimaryKeyAttributes(MithraDataObject firstData, Attribute[] primaryKeyAttributes, MithraFastList<Attribute> prototypeAttributes, MithraFastList<Attribute> nullAttributes)
    {
        for (int i = 0 ; i< primaryKeyAttributes.length;i++)
        {
            Attribute attr = primaryKeyAttributes[i];

            if (!attr.zGetShadowAttribute().isAttributeNull(firstData))
            {
                prototypeAttributes.add(attr.zGetShadowAttribute());
            }
            else
            {
                nullAttributes.add(attr);
            }
        }
        AsOfAttribute[] asOfAttributes = this.getFinder().getAsOfAttributes();
        if (asOfAttributes != null)
        {
            for (int i = 0; i < asOfAttributes.length; i++)
            {
                TimestampAttribute toAttribute = asOfAttributes[i].getToAttribute();
                if (!toAttribute.isAttributeNull(firstData))
                {
                    prototypeAttributes.add(toAttribute);
                }
                else
                {
                    nullAttributes.add(toAttribute);
                }
            }
        }
    }

    private Attribute getOptimisticKey(AsOfAttribute[] asOfAttributes, List<MithraDataObject> data)
    {
        AsOfAttribute businessDate = null;
        AsOfAttribute processingDate = null;
        if (asOfAttributes.length == 2)
        {
            businessDate = asOfAttributes[0];
            processingDate = asOfAttributes[1];
        }
        else if (asOfAttributes[0].isProcessingDate())
        {
            processingDate = asOfAttributes[0];
        }
        if (processingDate != null)
        {
            boolean mustAddProcessingDate = true;
            if (businessDate != null)
            {
                if (MithraManagerProvider.getMithraManager().getCurrentTransaction().retryOnOptimisticLockFailure())
                {
                    long infinityTime = businessDate.getInfinityDate().getTime();
                    int count = 0;
                    for (; count < data.size(); count++)
                    {
                        if (businessDate.getToAttribute().timestampValueOfAsLong(data.get(count)) != infinityTime)
                        {
                            break;
                        }
                    }
                    mustAddProcessingDate = count < data.size();
                }
            }
            if (mustAddProcessingDate)
            {
                return processingDate.getFromAttribute();
            }
        }
        return null;
    }

    // TODO: This should be moved to DatabaseType as a part of deleteViaJoin work.
    private void appendTempTableUpdateDeleteJoin(Object source, Attribute[] prototypeAttributes, MithraFastList<Attribute> nullAttributes, int pkAttributeCount, TupleTempContext tempContext, StringBuilder builder)
    {
        builder.append(" from ").append(this.getFullyQualifiedTableNameGenericSource(source));
        builder.append(" t0, ");
        this.appendTempTableRightSideJoin(source, prototypeAttributes, nullAttributes, pkAttributeCount, tempContext, builder);
    }

    // TODO: This should be moved to DatabaseType as a part of deleteViaJoin work.
    private void appendTempTableRightSideJoin(
            Object source,
            Attribute[] prototypeAttributes,
            MithraFastList<Attribute> nullAttributes,
            int pkAttributeCount,
            TupleTempContext tempContext,
            StringBuilder builder)
    {
        builder.append(tempContext.getFullyQualifiedTableName(source, this.getMithraObjectPortal().getPersisterId()));
        builder.append(" t1 where ");
        this.constructJoin(prototypeAttributes, nullAttributes, pkAttributeCount, builder);
    }

    protected void constructJoin(Attribute[] prototypeAttributes, MithraFastList<Attribute> nullAttributes, int pkAttributeCount, StringBuilder builder)
    {
        for (int i = 0; i < pkAttributeCount; i++)
        {
            if (!prototypeAttributes[i].isSourceAttribute())
            {
                if (i > 0)
                {
                    builder.append(" and ");
                }
                builder.append("t0.").append(prototypeAttributes[i].getColumnName()).append(" = t1.c").append(i);
            }
        }
        for (int i = 0; i < nullAttributes.size(); i++)
        {
            builder.append(" and t0.").append(nullAttributes.get(i).getColumnName()).append(" IS NULL");
        }
    }

    private void determineDirtyData(Object source, TupleTempContext tempContext, Attribute[] prototypeArray,
                                    MithraFastList<Attribute> nullAttributes, int pkAttributeCount,
                                    Connection con, DatabaseType databaseType, List dataList, TimeZone databaseTimeZone, List updates,
                                    boolean isMultiUpdate) throws SQLException
    {
        PreparedStatement stm = null;
        ResultSet rs = null;
        Attribute[] persistedPkAttributes = createPersistedPkAttributes(prototypeArray, pkAttributeCount);
        FullUniqueIndex index = new FullUniqueIndex("", persistedPkAttributes);
        index.addAll(dataList);
        HashSet<String> updatedColumns = new HashSet<String>(updates.size());
        for (int i = 0; i < updates.size(); i++)
        {
            updatedColumns.add(((AttributeUpdateWrapper) updates.get(i)).getAttribute().getColumnName());
        }
        StringBuilder builder = new StringBuilder();
        builder.append("delete from ").append(tempContext.getFullyQualifiedTableName(source, this.getMithraObjectPortal().getPersisterId()));
        builder.append(" from ").append(this.getFullyQualifiedTableNameGenericSource(source));
        builder.append(" t0, ");
        builder.append(tempContext.getFullyQualifiedTableName(source, this.getMithraObjectPortal().getPersisterId()));
        builder.append((" t1 where "));
        boolean appended = false;
        List<AttributeUpdateWrapper> parameterSetters = appendOptimisticLockFailureWhereClause(prototypeArray, nullAttributes,
                pkAttributeCount, updates, isMultiUpdate, updatedColumns, builder, appended);
        String sql = builder.toString();
        if (this.getSqlLogger().isDebugEnabled())
        {
            PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
            for (int i = 0; i < parameterSetters.size(); i++)
            {
                parameterSetters.get(i).setSqlParameters(pps, i + 1, databaseTimeZone, databaseType);
            }
            this.logWithSource(this.getSqlLogger(), source, "determining optimistic failure with: " + pps.getPrintableStatement());
        }
        try
        {
            stm = con.prepareStatement(sql);
            for (int i = 0; i < parameterSetters.size(); i++)
            {
                parameterSetters.get(i).setSqlParameters(stm, i + 1, databaseTimeZone, databaseType);
            }
            int updated = stm.executeUpdate();
            stm.close();
            stm = null;
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "total optimistic failures: " + (dataList.size() - updated));
            }
            builder.setLength(0);
            builder.append("select c0");
            for (int i = 1; i < pkAttributeCount; i++)
            {
                if (!prototypeArray[i].isSourceAttribute())
                {
                    builder.append(", c").append(i);
                }
            }
            builder.append(" from ").append(tempContext.getFullyQualifiedTableName(source, this.getMithraObjectPortal().getPersisterId()));
            sql = builder.toString();
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "determining optimistic failure with: " + sql);
            }
            stm = con.prepareStatement(sql);
            rs = stm.executeQuery();
            List<Tuple> tupleList = tempContext.parseResultSet(rs, persistedPkAttributes.length, databaseType, databaseTimeZone);
            Extractor[] tupleExtractors = new Extractor[persistedPkAttributes.length];
            SingleColumnAttribute[] allTupleAttributes = tempContext.getPersistentTupleAttributes();
            for (int i = 0; i < tupleExtractors.length; i++)
            {
                tupleExtractors[i] = (Extractor) allTupleAttributes[i];
            }
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            for (int i = 0; i < tupleList.size(); i++)
            {
                MithraDataObject data = (MithraDataObject) index.get(tupleList.get(i), tupleExtractors);
                this.getSqlLogger().error("Optimistic lock failed on " + data.zGetPrintablePrimaryKey());
                this.getMithraObjectPortal().getCache().markDirtyForReload(data, tx);
            }
            rs.close();
            rs = null;
            stm.close();
            stm = null;
        }
        finally
        {
            closeDatabaseObjects(null, stm, rs);
        }
    }

    private List<AttributeUpdateWrapper> appendOptimisticLockFailureWhereClause(Attribute[] prototypeArray, MithraFastList<Attribute> nullAttributes, int pkAttributeCount, List updates, boolean isMultiUpdate, HashSet<String> updatedColumns, StringBuilder builder, boolean appended)
    {
        HashSet<String> relevantUpdatedColumns = new HashSet<String>();
        for (int i = 0; i < pkAttributeCount; i++)
        {
            if (!prototypeArray[i].isSourceAttribute())
            {
                String columnName = prototypeArray[i].getColumnName();
                if (!updatedColumns.contains(columnName))
                {
                    if (appended) builder.append(" and ");
                    builder.append("t0.").append(columnName).append(" = t1.c").append(i);
                    appended = true;
                }
                else
                {
                    relevantUpdatedColumns.add(columnName);
                }
            }
        }
        for (int i = 0; i < nullAttributes.size(); i++)
        {
            String columnName = nullAttributes.get(i).getColumnName();
            if (!updatedColumns.contains(columnName))
            {
                if (appended) builder.append(" and ");
                builder.append("t0.").append(columnName).append(" IS NULL");
                appended = true;
            }
            else
            {
                relevantUpdatedColumns.add(columnName);
            }
        }
        List<AttributeUpdateWrapper> parameterSetters = new MithraFastList<AttributeUpdateWrapper>();
        if (isMultiUpdate)
        {
            for (int i = 0; i < updates.size(); i++)
            {
                AttributeUpdateWrapper wrapper = (AttributeUpdateWrapper) updates.get(i);
                String columnName = wrapper.getAttribute().getColumnName();
                if (relevantUpdatedColumns.contains(columnName))
                {
                    parameterSetters.add(wrapper);
                }
                if (appended) builder.append(" and ");
                builder.append("t0.").append(columnName).append(" = ?");
            }
        }
        else
        {
            for (int i = pkAttributeCount; i < prototypeArray.length; i++)
            {
                String columnName = prototypeArray[i].getColumnName();
                if (relevantUpdatedColumns.contains(columnName))
                {
                    if (appended) builder.append(" and ");
                    builder.append("t0.").append(columnName).append(" = t1.c").append(i);
                }
            }
        }
        return parameterSetters;
    }

    private Attribute[] createPersistedPkAttributes(Attribute[] prototypeArray, int pkAttributeCount)
    {
        Attribute sourceAttribute = this.getFinder().getSourceAttribute();
        Attribute[] persistedPkAttributes;
        if (sourceAttribute == null)
        {
            persistedPkAttributes = new Attribute[pkAttributeCount];
            System.arraycopy(prototypeArray, 0, persistedPkAttributes, 0, pkAttributeCount);
        }
        else
        {
            persistedPkAttributes = new Attribute[pkAttributeCount - 1];
            int pos = 0;
            for (int i = 0; i < pkAttributeCount; i++)
            {
                if (!prototypeArray[i].isSourceAttribute())
                {
                    persistedPkAttributes[pos++] = prototypeArray[i];
                }
            }
        }
        return persistedPkAttributes;
    }

    protected String getSqlWhereClauseForBatchUpdateForSameSourceAttribute(MithraDataObject firstDataToUpdate)
    {
        return this.getSqlWhereClauseForUpdate(firstDataToUpdate);
    }

    protected MithraDataObject getDataForUpdate(UpdateOperation operation)
    {
        return getDataForUpdate(operation.getMithraObject());
    }

// batchUpdateForSameSourceAttribute End -----------------------------------------------------------------------------------------------------------

    protected void zBulkInsertListForSameSourceAttribute(List listToInsert, DatabaseType databaseType, Object source) throws MithraDatabaseException
    {
        TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

        String schema = getSchemaForBulkInsert(databaseType, source);

        BulkLoader bulkLoader = null;

        Connection con = null;
        Statement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            String fullyQualifiedTableName = this.getFullyQualifiedTableNameGenericSource(source);
            String tableName = this.getTableNameGenericSource(source);
            bulkLoader = this.createBulkLoaderGenericSource(source);
            boolean createsTempTable = bulkLoader.createsTempTable();
            String tempTableName = null;
            String fullyQualifiedTempTableName = null;
            if (createsTempTable)
            {
                String tableNamePrefix = this.getTableNamePrefixGenericSource(source);
                tempTableName = assignTempTableName(tableNamePrefix);
                String tempDbSchemaName = databaseType.getTempDbSchemaName();
                if (tempDbSchemaName == null)
                {
                    tempDbSchemaName = schema;
                }
                fullyQualifiedTempTableName = databaseType.getFullyQualifiedTableName(tempDbSchemaName, tempTableName);
            }
            Logger sqlLogger = this.getSqlLogger();
            bulkLoader.initialize(databaseTimeZone, schema, tableName,
                    this.getMithraObjectPortal().getFinder().getPersistentAttributes(),
                    sqlLogger, tempTableName, this.getColumnsForBulkInsertCreation(databaseType), con);
            if (createsTempTable)
            {
                MithraManagerProvider.getMithraManager().getCurrentTransaction().registerSynchronization(createDropBulkTempTableSynchronization(source, tempTableName));
            }
            bulkLoader.bindObjectsAndExecute(listToInsert, con);
            bulkLoader.destroy();
            bulkLoader = null;
            if (createsTempTable)
            {
                String sql = "insert into " + fullyQualifiedTableName + " select * from " + fullyQualifiedTempTableName;
                if (sqlLogger.isDebugEnabled())
                {
                    sqlLogger.debug("Batch inserting with temp table, " + listToInsert.size() + " rows: " + sql);
                }
                stm = con.createStatement();
                this.setExpectedExecuteReturn(listToInsert.size());
                int inserted = stm.executeUpdate(sql);
                stm.close();
                stm = null;
                if (inserted < listToInsert.size())
                {
                    throw new MithraDatabaseException("bulk insert did not insert the correct number of rows. Expected " + listToInsert.size() +
                            " but got " + inserted);
                }
            }
            zRegisterForNotification(listToInsert, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("bulk insert failed " + e.getMessage(), listToInsert, e, source, con);
        }
        catch (BulkLoaderException e)
        {
            throw new MithraDatabaseException("bulk insert failed " + e.getMessage(), e);
        }
        finally
        {
            if (bulkLoader != null) bulkLoader.destroy();
            this.closeStatementAndConnection(con, stm);
        }
    }

    private String getSchemaForBulkInsert(DatabaseType databaseType, Object source)
    {
        String schema = this.getSchemaGenericSource(source);
        if (schema == null)
        {
            Connection con = null;
            try
            {
                con = this.getConnectionForReadGenericSource(source);
                schema = databaseType.getCurrentSchema(con);
            }
            catch (SQLException e)
            {
                throw new MithraDatabaseException("could not determine schema for bulk insert ", e);
            }
            finally
            {
                this.closeConnection(con);
            }
        }
        return schema;
    }

    protected void zBatchUpdate(BatchUpdateOperation batchUpdateOperation)
    {
        long startTime = System.currentTimeMillis();
        List<UpdateOperation> updateOperations = batchUpdateOperation.getUpdateOperations();
        Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateUpdatesBySourceAttribute(updateOperations, sourceAttribute);
            int segregatedSize = segregated.size();
            for (int i = 0; i < segregatedSize; i++)
            {
                this.zBatchUpdateForSameSourceAttribute((List<UpdateOperation>) segregated.get(i), batchUpdateOperation);
            }
        }
        else
        {
            this.zBatchUpdateForSameSourceAttribute(updateOperations, batchUpdateOperation);
        }
        this.getPerformanceData().recordTimeForUpdate(updateOperations.size(), startTime);
    }

    protected List<List> segregateUpdatesBySourceAttribute(List updateOperations, Attribute sourceAttribute)
    {
        MultiHashMap map = new MultiHashMap();
        for (int i = 0; i < updateOperations.size(); i++)
        {
            UpdateOperation op = (UpdateOperation) updateOperations.get(i);
            map.put(sourceAttribute.valueOf(op.getMithraObject().zGetTxDataForRead()), op);
        }

        if (map.size() > 1)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(updateOperations);
        }
    }

    protected void zBatchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateBySourceAttribute(mithraObjects, sourceAttribute);
            int segregatedSize = segregated.size();
            for (int i = 0; i < segregatedSize; i++)
            {
                this.zBatchInsertForSameSourceAttribute((List) segregated.get(i), bulkInsertThreshold);
            }
        }
        else
        {
            this.zBatchInsertForSameSourceAttribute(mithraObjects, bulkInsertThreshold);
        }
        this.getPerformanceData().recordTimeForInsert(mithraObjects.size(), startTime);
    }

    protected void zBatchInsertForSameSourceAttribute(List mithraObjects, int bulkInsertThreshold)
            throws MithraDatabaseException
    {
        Object source = this.getSourceAttributeValueFromObjectGeneric(((MithraTransactionalObject) mithraObjects.get(0)).zGetTxDataForRead());
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        if (bulkInsertThreshold > 0 && mithraObjects.size() > bulkInsertThreshold && databaseType.hasBulkInsert())
        {
            zBulkInsertListForSameSourceAttribute(mithraObjects, databaseType, source);
        }
        else if (databaseType.hasMultiInsert())
        {
            zMultiInsertForSameSourceAttribute(mithraObjects, databaseType, source);
        }
        else
        {
            zBatchInsertForSameSourceAttribute(mithraObjects, databaseType, source);
        }
    }

    protected void zMultiInsertForSameSourceAttribute(List mithraObjects, DatabaseType databaseType, Object source)
            throws MithraDatabaseException
    {
        String sql = "insert into " + this.getFullyQualifiedTableNameGenericSource(source);
        sql += " (" + this.getInsertFields() + ')';
        String questionMarks = this.getInsertQuestionMarks();

        int currentBatchSize = 0;
        int listPos = 0;
        Connection con = null;
        PreparedStatement stm = null;
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "multi inserting with: " + sql + " for " + mithraObjects.size() + " objects ");
            }

            int batchSize = databaseType.getMultiInsertBatchSize(this.getTotalColumnsInInsert());
            int batches = mithraObjects.size() / batchSize;
            int firstBatchSize = batchSize;
            int remainder = mithraObjects.size() % batchSize;
            if (remainder > 0)
            {
                batches++;
                firstBatchSize = remainder;
            }
            currentBatchSize = firstBatchSize;
            String batchSql = sql + databaseType.createMultiInsertParametersStatement(questionMarks, firstBatchSize);
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(batchSql);
            }
            stm = con.prepareStatement(batchSql);
            listPos = this.zMultiInsertOnce(stm, pps, firstBatchSize, mithraObjects, 0, databaseTimeZone, source);
            if (batches > 1 && firstBatchSize != batchSize)
            {
                stm.close();
                stm = null;
                currentBatchSize = batchSize;
                batchSql = sql + databaseType.createMultiInsertParametersStatement(questionMarks, batchSize);
                stm = con.prepareStatement(batchSql);
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps = new PrintablePreparedStatement(batchSql);
                }
            }
            for (int b = 1; b < batches; b++)
            {
                listPos = this.zMultiInsertOnce(stm, pps, batchSize, mithraObjects, listPos, databaseTimeZone, source);
            }
            stm.close();
            stm = null;

            zRegisterForNotification(mithraObjects, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("batch insert failed " + e.getMessage(), mithraObjects.subList(listPos, listPos + currentBatchSize), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    protected void zBatchInsertForSameSourceAttribute(List mithraObjects, DatabaseType databaseType, Object source)
            throws MithraDatabaseException
    {
        String sql = "insert into " + this.getFullyQualifiedTableNameGenericSource(source);
        sql += " (" + this.getInsertFields() + ") values (" + this.getInsertQuestionMarks() + ')';
        Connection con = null;
        PreparedStatement stm = null;
        int batchSize = 0;
        int currentPosition = 0;
        int listSize = mithraObjects.size();
        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);

            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source, "batch inserting with: " + sql + " for " + mithraObjects.size() + " objects ");
            }
            PrintablePreparedStatement pps = null;
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                pps = new PrintablePreparedStatement(sql);
            }
            stm = con.prepareStatement(sql);
            batchSize = databaseType.getMaxPreparedStatementBatchCount(this.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes().length);
            if (batchSize <= 0) batchSize = listSize;
            int objectsInBatch = 0;
            for (int i = 0; i < listSize; i++)
            {
                MithraDataObject data = ((MithraTransactionalObject) mithraObjects.get(i)).zGetTxDataForRead();
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps.clearParameters();
                    this.setInsertAttributes(pps, data, databaseTimeZone, 1, databaseType);
                    this.logWithSource(this.getBatchSqlLogger(), source, "batch inserting with: " + pps.getPrintableStatement());
                }
                this.setInsertAttributes(stm, data, databaseTimeZone, 1, databaseType);
                stm.addBatch();
                objectsInBatch++;

                if (objectsInBatch == batchSize)
                {
                    objectsInBatch = 0;
                    this.executeBatch(stm, true);
                    currentPosition += batchSize;
                }
            }
            if (objectsInBatch > 0)
            {
                this.executeBatch(stm, true);

            }
            stm.close();
            stm = null;

            zRegisterForNotification(mithraObjects, source);
        }
        catch (SQLException e)
        {
            int endPosition = currentPosition + batchSize > listSize ? listSize : currentPosition + batchSize;
            this.analyzeAndWrapSqlExceptionGenericSource("batch insert failed " + e.getMessage(), mithraObjects.subList(currentPosition, endPosition), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    protected void zRegisterForNotification(List mithraObjects, Object source)
    {
        String dbid = this.getDatabaseIdentifierGenericSource(source);
        getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.INSERT, mithraObjects, source);
        this.getMithraObjectPortal().registerForNotification(dbid);
    }

    protected int zMultiInsertOnce(PreparedStatement stm, PrintablePreparedStatement pps, int batchSize,
                                   List mithraObjects, int listPos, TimeZone databaseTimeZone, Object source)
            throws SQLException
    {
        if (this.getBatchSqlLogger().isDebugEnabled())
        {
            pps.clearParameters();
        }
        int paramPos = 1;
        DatabaseType dt = this.getDatabaseTypeGenericSource(source);
        for (int i = 0; i < batchSize; i++, listPos++)
        {
            MithraDataObject data = ((MithraTransactionalObject) mithraObjects.get(listPos)).zGetTxDataForRead();
            if (this.getBatchSqlLogger().isDebugEnabled())
            {
                this.setInsertAttributes(pps, data, databaseTimeZone, paramPos, dt);
            }
            this.setInsertAttributes(stm, data, databaseTimeZone, paramPos, dt);
            paramPos += this.getTotalColumnsInInsert();
        }
        if (this.getBatchSqlLogger().isDebugEnabled())
        {
            this.logWithSource(this.getBatchSqlLogger(), source, "batch inserting with: " + pps.getPrintableStatement());
        }
        this.setExpectedExecuteReturn(batchSize);
        int inserted = stm.executeUpdate();
        if (inserted != batchSize)
        {
            throw new MithraBusinessException("inserted only " + inserted + " when expecting " + batchSize);
        }
        return listPos;
    }

    protected void zBatchDelete(List mithraObjects, boolean checkCount) throws MithraDatabaseException
    {
        long startTime = System.currentTimeMillis();
        Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        if (sourceAttribute != null)
        {
            List segregated = this.segregateBySourceAttribute(mithraObjects, sourceAttribute);
            int segregatedListSize = segregated.size();
            for (int i = 0; i < segregatedListSize; i++)
            {
                batchDeleteForSameSourceAttribute((List) segregated.get(i), checkCount);
            }
        }
        else
        {
            batchDeleteForSameSourceAttribute(mithraObjects, checkCount);
        }
        this.getPerformanceData().recordTimeForDelete(mithraObjects.size(), startTime);
    }

    protected void batchDeleteForSameSourceAttribute(List mithraObjects, boolean checkCount) throws MithraDatabaseException
    {
        MithraDataObject firstData = ((MithraTransactionalObject) mithraObjects.get(0)).zGetTxDataForRead();
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);
        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        Attribute[] primaryKeyAttributes = this.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes();

        if (databaseType.getDeleteViaInsertAndJoinThreshold() >= 0 && mithraObjects.size() > databaseType.getDeleteViaInsertAndJoinThreshold())
        {
            this.batchDeleteForSameSourceAttributeViaTempJoin(mithraObjects, source, primaryKeyAttributes);
            return;
        }

        String sql = "delete from " + this.getFullyQualifiedTableNameGenericSource(source);

        sql += this.getSqlWhereClauseForDelete(firstData);

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
            int batchStart = 0;
            for (int i = 0; i < mithraObjects.size(); i++)
            {
                MithraDataObject data = ((MithraTransactionalObject) mithraObjects.get(i)).zGetTxDataForRead();
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps.clearParameters();
                    this.setPrimaryKeyAttributes(pps, 1, data, databaseTimeZone, databaseType);
                    this.logWithSource(this.getBatchSqlLogger(), source, "batch deleting with: " + pps.getPrintableStatement());
                }
                this.setPrimaryKeyAttributes(stm, 1, data, databaseTimeZone, databaseType);
                stm.addBatch();
                objectsInBatch++;

                if (objectsInBatch == batchSize)
                {
                    objectsInBatch = 0;
                    this.executeBatchWithObjects(stm, mithraObjects, batchStart, checkCount);
                    batchStart = i + 1;
                }
            }
            if (objectsInBatch > 0)
            {
                this.executeBatchWithObjects(stm, mithraObjects, batchStart, checkCount);
            }
            stm.close();
            stm = null;

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEvent(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.DELETE, mithraObjects, source);
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

    protected String getOptimisticLockingWhereSqlIfNecessary()
    {
        String sql = "";
        if (this.hasOptimisticLocking())
        {
            if (this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking())
            {
                sql += ' ' + this.getOptimisticLockingWhereSql();
            }
        }
        return sql;
    }

    public String getOptimisticLockingWhereSql()
    {
        throw new RuntimeException("only transactional objects should implemented");
    }

    protected void executeBatchForUpdateOperations(PreparedStatement stm, List updateOperations, int start) throws SQLException
    {
        int[] results = executeBatchAndHandleBatchException(stm);
        boolean optimistic = this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking();
        boolean throwOptimisticException = false;
        if (optimistic)
        {
            throwOptimisticException = checkOptimisticResults(results, updateOperations, start, throwOptimisticException);
        }
        else
        {
            this.checkUpdateCount(results);
        }
        if (throwOptimisticException)
        {
            throwOptimisticLockException();
        }
        stm.clearBatch();
    }

    protected void executeBatchWithObjects(PreparedStatement stm, List mithraObjects, int start, boolean checkCount) throws SQLException
    {
        int[] results = executeBatchAndHandleBatchException(stm);
        boolean optimistic = this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking();
        boolean throwOptimisticException = false;
        if (optimistic)
        {
            throwOptimisticException = checkOptimisticResultsForObjects(results, mithraObjects, start, throwOptimisticException);
        }
        else if (checkCount)
        {
            this.checkUpdateCount(results);
        }
        if (throwOptimisticException)
        {
            throwOptimisticLockException();
        }
        stm.clearBatch();
    }

    private void throwOptimisticLockException()
    {
        MithraOptimisticLockException mithraOptimisticLockException = new MithraOptimisticLockException("Optimistic lock failed, see above log for specific objects.");
        if (MithraManagerProvider.getMithraManager().getCurrentTransaction().retryOnOptimisticLockFailure())
        {
            mithraOptimisticLockException.setRetriable(true);
        }
        throw mithraOptimisticLockException;
    }

    private boolean checkOptimisticResults(int[] results, List updateOperations, int start, boolean throwOptimisticException)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        for (int i = 0; i < results.length; i++)
        {
            if (results[i] != 1)
            {
                UpdateOperation updateOperation = (UpdateOperation) updateOperations.get(i + start);
                MithraTransactionalObject mithraObject = updateOperation.getMithraObject();
                MithraDataObject data = mithraObject.zGetCurrentData();
                if (data == null)
                {
                    data = mithraObject.zGetTxDataForRead();
                }
                this.getSqlLogger().error("Optimistic lock failed on " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
                this.getMithraObjectPortal().getCache().markDirtyForReload(data, tx);
                throwOptimisticException = true;
            }
        }
        return throwOptimisticException;
    }

    private boolean checkOptimisticResultsForObjects(int[] results, List mithraObjects, int start, boolean throwOptimisticException)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        for (int i = 0; i < results.length; i++)
        {
            if (results[i] != 1)
            {
                MithraTransactionalObject mithraObject = (MithraTransactionalObject) mithraObjects.get(start + i);
                MithraDataObject data = mithraObject.zGetTxDataForRead();
                this.getSqlLogger().error("Optimistic lock failed on " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
                this.getMithraObjectPortal().getCache().markDirtyForReload(data, tx);
                throwOptimisticException = true;
            }
        }
        return throwOptimisticException;
    }


    protected void zDeleteUsingOperation(Operation op)
    {
        this.zDeleteUsingOperation(op, 0);
    }

    protected int zDeleteUsingOperation(Operation op, int rowCount) throws MithraDatabaseException
    {
        int deletedRows = 0;
        SqlQuery query = new SqlQuery(op, null, false);
        query.setUseDatabaseAliasInSqlQuery(false);
        query.setMaxUnionCount(1);
        query.setDisableTempTableJoin(true);
        int sourceCount = query.getNumberOfSources();
        for (int sourceNum = 0; sourceNum < sourceCount; sourceNum++)
        {
            Object source = this.getSourceAttributeValueForSelectedObjectGeneric(query, sourceNum);
            DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
            int queries = query.prepareQueryForSource(sourceNum, databaseType, this.getDatabaseTimeZoneGenericSource(source));
            try
            {
                Connection con = null;
                PreparedStatement stm = null;
                try
                {
                    con = this.getConnectionForWriteGenericSource(source);
                    for (int queryNumber = 0; (rowCount == 0 || rowCount - deletedRows > 0) && queryNumber < queries; queryNumber++)
                    {
                        int toDelete = 0;
                        if (rowCount > 0)
                        {
                            toDelete = rowCount - deletedRows;
                        }
                        query.prepareForQuery(queryNumber);
                        String sql = databaseType.getDelete(query, toDelete);

                        if (this.getSqlLogger().isDebugEnabled())
                        {
                            PrintablePreparedStatement pps = new PrintablePreparedStatement(sql);
                            query.setStatementParameters(pps);
                            this.logWithSource(this.getSqlLogger(), source, "deleting with: " + pps.getPrintableStatement());
                        }
                        stm = con.prepareStatement(sql);
                        query.setStatementParameters(stm);
                        deletedRows += stm.executeUpdate();
                        stm.close();
                        stm = null;
                        String dbid = this.getDatabaseIdentifierGenericSource(source);
                        getNotificationEventManager().addMithraNotificationEventForMassDelete(dbid, this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.MASS_DELETE, op);
                    }
                }
                catch (SQLException e)
                {
                    this.analyzeAndWrapSqlExceptionGenericSource("delete failed " + e.getMessage(), e, source, con);
                }
                finally
                {
                    this.closeStatementAndConnection(con, stm);
                }
            }
            finally
            {
                query.cleanTempForSource(sourceNum, databaseType);
            }
        }
        return deletedRows;
    }

    protected void zMultiUpdate(MultiUpdateOperation multiUpdateOperation)
    {
        long startTime = System.currentTimeMillis();
        this.multiUpdateForSameSourceAttribute(multiUpdateOperation);
        this.getPerformanceData().recordTimeForUpdate(multiUpdateOperation.getMithraObjects().size(), startTime);
    }

    private void multiUpdateForSameSourceAttribute(MultiUpdateOperation multiUpdateOperation)
    {
        MithraTransactionalObject firstObject = multiUpdateOperation.getMithraObject();
        MithraDataObject firstData = this.getDataForUpdate(firstObject);
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);

        DatabaseType databaseType = this.getDatabaseTypeGenericSource(source);
        if (databaseType.getUpdateViaInsertAndJoinThreshold() > 0 &&
                databaseType.getUpdateViaInsertAndJoinThreshold() < multiUpdateOperation.getAllObjects().size() &&
                this.getFinder().getVersionAttribute() == null &&
                multiUpdateOperation.isEligibleForUpdateViaInsert()
                )
        {
            multiUpdateViaInsertAndJoin(multiUpdateOperation, databaseType);
            return;
        }
        String tableName = this.getFullyQualifiedTableNameGenericSource(source);
        multiUpdateOperation.prepareForSqlGeneration(tableName, databaseType);
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            con = this.getConnectionForWriteGenericSource(source);
            TimeZone databaseTimeZone = this.getDatabaseTimeZoneGenericSource(source);
            if (this.getSqlLogger().isDebugEnabled())
            {
                this.logWithSource(this.getSqlLogger(), source,
                        "multi update of " + multiUpdateOperation.getMithraObjects().size() + " objects with: " +
                                multiUpdateOperation.getPrintableSql());
            }
            PrintablePreparedStatement pps;

            String sql;
            while ((sql = multiUpdateOperation.getNextMultiUpdateSql(databaseType.getMaxSearchableArguments())) != null)
            {
                if (this.getBatchSqlLogger().isDebugEnabled())
                {
                    pps = new PrintablePreparedStatement(sql);
                    multiUpdateOperation.setSqlParameters(pps, databaseTimeZone, databaseType);
                    this.logWithSource(this.getBatchSqlLogger(),
                            source, "multi updating with: " + pps.getPrintableStatement());
                }
                stm = con.prepareStatement(sql);
                multiUpdateOperation.setSqlParameters(stm, databaseTimeZone, databaseType);
                this.setExpectedExecuteReturn(multiUpdateOperation.getExpectedUpdates());
                int actualUpdates = stm.executeUpdate();
                multiUpdateOperation.checkUpdateResult(actualUpdates, this.getSqlLogger());
                stm.close();
                stm = null;
            }

            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEventForMultiUpdate(dbid,
                    this.getFullyQualifiedFinderClassName(), MithraNotificationEvent.UPDATE,
                    multiUpdateOperation, source);
        }
        catch (SQLException e)
        {
            this.analyzeAndWrapSqlExceptionGenericSource("multi update failed " + e.getMessage(), e, source, con);
        }
        finally
        {
            this.closeStatementAndConnection(con, stm);
        }
    }

    private void multiUpdateViaInsertAndJoin(MultiUpdateOperation multiUpdateOperation, DatabaseType databaseType)
    {
        MithraTransactionalObject firstObject = multiUpdateOperation.getMithraObject();
        MithraDataObject firstData = this.getDataForUpdate(firstObject);
        Object source = this.getSourceAttributeValueFromObjectGeneric(firstData);
        List allObjects = multiUpdateOperation.getAllObjects();
        MithraFastList dataList = new MithraFastList(allObjects.size());
        dataList.add(firstData);
        for (int i = 1; i < allObjects.size(); i++)
        {
            dataList.add(this.getDataForUpdate((MithraTransactionalObject) allObjects.get(i)));
        }

        Attribute[] primaryKeyAttributes = this.getFinder().getPrimaryKeyAttributes();
        MithraFastList<Attribute> prototypeAttributes = new MithraFastList(primaryKeyAttributes.length + 1);
        MithraFastList<Attribute> nullAttributes = new MithraFastList(2);
        boolean optimistic = this.getMithraObjectPortal().getTxParticipationMode().isOptimisticLocking();
        addPrimaryKeyAttributes(firstData, primaryKeyAttributes, prototypeAttributes, nullAttributes);
        addOptimisticAttribute(dataList, prototypeAttributes, optimistic);
        int pkAttributeCount = prototypeAttributes.size();
        Attribute[] prototypeArray = new Attribute[prototypeAttributes.size()];
        prototypeAttributes.toArray(prototypeArray);
        TupleTempContext tempContext = null;
        List updates = multiUpdateOperation.getUpdates();
        try
        {
            tempContext = new TupleTempContext(prototypeArray, false);
            //we only get here in transactions. no need for special retry handling
            tempContext.insert(dataList, this.getMithraObjectPortal(), databaseType.getUpdateViaInsertAndJoinThreshold(), false);

            StringBuilder builder = new StringBuilder(30 + updates.size() * 12);

            databaseType.setMultiUpdateViaJoinQuery(source, updates, prototypeArray, nullAttributes, pkAttributeCount, tempContext, this.getMithraObjectPortal(), this.getFullyQualifiedTableNameGenericSource(source), builder);

            String sql = builder.toString();
            executeBatchUpdateViaTemp(source, dataList, databaseType, nullAttributes, optimistic, pkAttributeCount, prototypeArray, tempContext, sql, updates, true);
            multiUpdateOperation.setUpdated();
            String dbid = this.getDatabaseIdentifierGenericSource(source);
            getNotificationEventManager().addMithraNotificationEventForMultiUpdate(dbid, this.getFullyQualifiedFinderClassName(),
                    MithraNotificationEvent.UPDATE, multiUpdateOperation, source);
        }
        finally
        {
            if (tempContext != null)
            {
                tempContext.destroy();
            }
        }
    }

    public static void setStatsListenerFactory(MithraStatsListenerFactory statsListenerFactory)
    {
        MithraAbstractDatabaseObject.statsListenerFactory = statsListenerFactory;
    }

    protected void throwNullAttribute(String name)
    {
        if(this.checkNullOnInsert)
        {
            throw new MithraBusinessException("the field '" + name + "' must not be null in class "+getDomainClassName());
        }
    }
}
