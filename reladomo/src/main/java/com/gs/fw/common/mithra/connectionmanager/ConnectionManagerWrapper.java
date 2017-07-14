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

package com.gs.fw.common.mithra.connectionmanager;


import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.bulkloader.BulkLoader;
import com.gs.fw.common.mithra.bulkloader.BulkLoaderException;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.tempobject.CommonTempContext;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.SmallSet;
import com.gs.fw.common.mithra.util.WrappedConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

public class ConnectionManagerWrapper implements SourcelessConnectionManager,
        ObjectSourceConnectionManager, IntSourceConnectionManager
{
    private Object delegate;

    private final UnifiedMap<ConnectionKey, WrappedConnectionIgnoreClose> tempContextMap = UnifiedMap.newMap();

    public ConnectionManagerWrapper(Object delegate)
    {
        this.delegate = delegate;
    }

    private SourcelessConnectionManager getAsSourceLess()
    {
        return (SourcelessConnectionManager) this.delegate;
    }

    private ObjectSourceConnectionManager getAsObjectSource()
    {
        return (ObjectSourceConnectionManager) this.delegate;
    }

    private IntSourceConnectionManager getAsIntSource()
    {
        return (IntSourceConnectionManager) this.delegate;
    }

    @Override
    public BulkLoader createBulkLoader() throws BulkLoaderException
    {
        return getAsSourceLess().createBulkLoader();
    }

    @Override
    public Connection getConnection()
    {
        if (this.tempContextMap.size() > 0)
        {
            synchronized (this.tempContextMap)
            {
                WrappedConnectionIgnoreClose wrapped = this.tempContextMap.get(new ConnectionKey(Thread.currentThread(), null));
                if (wrapped != null)
                {
                    return wrapped;
                }
            }
        }
        return getAsSourceLess().getConnection();
    }

    @Override
    public String getDatabaseIdentifier()
    {
        return getAsSourceLess().getDatabaseIdentifier();
    }

    @Override
    public TimeZone getDatabaseTimeZone()
    {
        return getAsSourceLess().getDatabaseTimeZone();
    }

    @Override
    public DatabaseType getDatabaseType()
    {
        return getAsSourceLess().getDatabaseType();
    }

    @Override
    public BulkLoader createBulkLoader(Object sourceAttribute) throws BulkLoaderException
    {
        return getAsObjectSource().createBulkLoader(sourceAttribute);
    }

    @Override
    public Connection getConnection(Object sourceAttribute)
    {
        if (this.tempContextMap.size() > 0)
        {
            synchronized (this.tempContextMap)
            {
                WrappedConnectionIgnoreClose wrapped = this.tempContextMap.get(new ConnectionKey(Thread.currentThread(), sourceAttribute));
                if (wrapped != null)
                {
                    return wrapped;
                }
            }
        }
        return getAsObjectSource().getConnection(sourceAttribute);
    }

    @Override
    public String getDatabaseIdentifier(Object sourceAttribute)
    {
        return getAsObjectSource().getDatabaseIdentifier(sourceAttribute);
    }

    @Override
    public TimeZone getDatabaseTimeZone(Object sourceAttribute)
    {
        return getAsObjectSource().getDatabaseTimeZone(sourceAttribute);
    }

    @Override
    public DatabaseType getDatabaseType(Object sourceAttribute)
    {
        return getAsObjectSource().getDatabaseType(sourceAttribute);
    }

    @Override
    public BulkLoader createBulkLoader(int sourceAttribute) throws BulkLoaderException
    {
        return getAsIntSource().createBulkLoader(sourceAttribute);
    }

    @Override
    public Connection getConnection(int sourceAttribute)
    {
        if (this.tempContextMap.size() > 0)
        {
            synchronized (this.tempContextMap)
            {
                WrappedConnectionIgnoreClose wrapped = this.tempContextMap.get(new ConnectionKey(Thread.currentThread(), sourceAttribute));
                if (wrapped != null)
                {
                    return wrapped;
                }
            }
        }
        return getAsIntSource().getConnection(sourceAttribute);
    }

    @Override
    public String getDatabaseIdentifier(int sourceAttribute)
    {
        return getAsIntSource().getDatabaseIdentifier(sourceAttribute);
    }

    @Override
    public TimeZone getDatabaseTimeZone(int sourceAttribute)
    {
        return getAsIntSource().getDatabaseTimeZone(sourceAttribute);
    }

    @Override
    public DatabaseType getDatabaseType(int sourceAttribute)
    {
        return getAsIntSource().getDatabaseType(sourceAttribute);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ConnectionManagerWrapper)
        {
            return this.delegate.equals(((ConnectionManagerWrapper)obj).delegate);
        }
        return this.delegate.equals(obj);
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    public void unbindConnection(Object source) throws SQLException
    {
        if (this.tempContextMap.size() > 0)
        {
            synchronized (this.tempContextMap)
            {
                WrappedConnectionIgnoreClose wrappedConnection = this.tempContextMap.get(new ConnectionKey(Thread.currentThread(), source));
                if (wrappedConnection != null)
                {
                    wrappedConnection.decrementUsage();
                    if (wrappedConnection.usageCount == 0)
                    {
                        this.tempContextMap.remove(new ConnectionKey(Thread.currentThread(), source));
                        wrappedConnection.reallyClose();
                    }
                }
            }
        }
    }

    public void bindConnection(CommonTempContext context, Object source, Connection c)
    {
        if (c instanceof WrappedConnectionIgnoreClose)
        {
            ((WrappedConnectionIgnoreClose)c).incrementUsage(context);
            return;
        }
        WrappedConnectionIgnoreClose wrappedConnection = new WrappedConnectionIgnoreClose(c, context);
        synchronized (this.tempContextMap)
        {
            tempContextMap.put(new ConnectionKey(Thread.currentThread(), source), wrappedConnection);
        }
        context.markSingleThreaded();
    }

    public void cleanupDeadConnection(final MithraDatabaseException dbe, final Connection deadConnection)
    {
        if (this.tempContextMap.size() > 0)
        {
            synchronized (this.tempContextMap)
            {
                final List<ConnectionKey> toRemove = FastList.newList();
                this.tempContextMap.forEachKeyValue(new Procedure2<ConnectionKey, WrappedConnectionIgnoreClose>()
                {
                    @Override
                    public void value(ConnectionKey connectionKey, WrappedConnectionIgnoreClose wrappedConnectionIgnoreClose)
                    {
                        if (connectionKey.thread == Thread.currentThread() && wrappedConnectionIgnoreClose == deadConnection)
                        {
                            toRemove.add(connectionKey);
                            dbe.addContextsForRetry(wrappedConnectionIgnoreClose.contexts);
                            try
                            {
                                wrappedConnectionIgnoreClose.reallyClose();
                            }
                            catch (SQLException e)
                            {
                                //ignore
                            }
                        }
                    }
                });
                for(int i=0;i<toRemove.size();i++)
                {
                    this.tempContextMap.remove(toRemove.get(i));
                }
            }
        }

    }

    private static final class ConnectionKey
    {
        private Thread thread;
        private Object source;

        private ConnectionKey(Thread thread, Object source)
        {
            this.thread = thread;
            this.source = source;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConnectionKey that = (ConnectionKey) o;

            return (source == null ? that.source == null : source.equals(that.source)) && thread.equals(that.thread);
        }

        @Override
        public int hashCode()
        {
            return HashUtil.combineHashes(thread.hashCode(), HashUtil.hash(source));
        }
    }

    private static final class WrappedConnectionIgnoreClose extends WrappedConnection
    {
        private int usageCount = 1;
        private Set<CommonTempContext> contexts = new SmallSet(2);

        private WrappedConnectionIgnoreClose(Connection c, CommonTempContext context)
        {
            super(c);
            contexts.add(context);
        }

        @Override
        public void close() throws SQLException
        {
            // do nothing
        }

        public void reallyClose() throws SQLException
        {
            this.getUnderlyingConnection().close();
        }

        public void incrementUsage(CommonTempContext context)
        {
            contexts.add(context);
            usageCount++;
        }

        public void decrementUsage()
        {
            usageCount--;
        }
    }
}
