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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.util.MithraProcessInfo;
import com.gs.fw.common.mithra.util.WrappedConnection;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;



/**
 * Mithra connection manager supports two modes of operation: plain JDBC and LDAP.
 *
 * By calling setJdbcConnectionString(), the pool can be used with plain JDBC connections.  When using plain
 * JDBC connections, setDriverClassName() must also be called with the correct driver.
 *
 * By calling setLdapName(), the pool can be used with databases that are defined via LDAP.
 */
public abstract class AbstractConnectionManager
{
    private static String APPLICATION_NAME = null;
    public static final String APPLICATION_NAME_KEY = "mithra.jdbc.programid";

    private String resourceName;

    private String serverName;

    private String poolName;

    private int poolSize;

    private int initialSize;

	private int maxWait;

    private String jdbcPassword;

    private String jdbcUser;

    private String jdbcConnectionString;

    private Driver driver;

    private DataSource dataSource;

    private ObjectPoolWithThreadAffinity connectionPool;

    private long timeBetweenEvictionRunsMillis = -1L;

    private long minEvictableIdleTimeMillis = 1000L * 60L * 30L;

    private long softMinEvictableIdleTimeMillis = 1000L * 60L * 5L;

    private Properties driverProperties = new Properties();

    private boolean useStatementPooling;

    private long connectionMaxLifeTimeAfterStartMillis = 0;

    private static Logger logger = LoggerFactory.getLogger(AbstractConnectionManager.class.getName());

    public AbstractConnectionManager()
    {
        this.setProperty("APPLICATIONNAME", getApplicationName());
        this.setProperty("HOSTPROC", getPid());
        this.setProperty("workstationID", getPid());  // for MS SQL Server
    }

    public AbstractConnectionManager(String newPoolName)
    {
        this();
        this.setPoolName(newPoolName);
    }

    /**
     * This method must be called after all the connection pool properties have been set.
     */
    public void initialisePool()
    {
        Properties loginProperties = createLoginProperties();

        if (logger.isDebugEnabled())
        {
            logger.debug("about to create pool with user-id : " + this.getJdbcUser());
        }

        ConnectionFactory connectionFactory = createConnectionFactory(loginProperties);
        PoolableObjectFactory objectFactoryForConnectionPool = getObjectFactoryForConnectionPool(connectionFactory, connectionPool);

        connectionPool = new ObjectPoolWithThreadAffinity(objectFactoryForConnectionPool, this.getPoolSize(),
                this.getMaxWait(), this.getPoolSize(), this.getInitialSize(), true, false, this.timeBetweenEvictionRunsMillis,
                this.minEvictableIdleTimeMillis, this.softMinEvictableIdleTimeMillis);

        dataSource = createPoolingDataSource(connectionPool);

        if (this.getInitialSize() > 0)
        {
            try // test connection
            {
                this.getConnection().close();
            }
            catch (Exception e)
            {
                logger.error("Error initializing pool " + this, e);
            }
        }
    }

    /**
     * Setting this value forces connections to close if they've been active for longer than this value.
     * It's advisable to use this with pool eviction times.
     * Default is 0, meaning infinite time.
     * @param connectionMaxLifeTimeAfterStartMillis value in millis. zero means infinite lifetime.
     */
    public void setConnectionMaxLifeTimeAfterStartMillis(long connectionMaxLifeTimeAfterStartMillis)
    {
        this.connectionMaxLifeTimeAfterStartMillis = connectionMaxLifeTimeAfterStartMillis;
    }

    protected Properties createLoginProperties()
    {
        Properties loginProperties = this.getDriverProperties();
        loginProperties.put("user", this.getJdbcUser());
        loginProperties.put("password", this.getJdbcPassword());
        return loginProperties;
    }

    protected DataSource getDataSource()
    {
        return dataSource;
    }

    protected abstract DataSource createPoolingDataSource(ObjectPoolWithThreadAffinity objectPool);

    protected abstract ConnectionFactory createConnectionFactory(Properties loginProperties);

    protected PoolableObjectFactory getObjectFactoryForConnectionPool(ConnectionFactory connectionFactory, ObjectPoolWithThreadAffinity connectionPool)
    {
        MithraPoolableConnectionFactory poolableConnectionFactory;

        try
        {
            poolableConnectionFactory = new MithraPoolableConnectionFactory(connectionFactory, 5, this.connectionMaxLifeTimeAfterStartMillis);
        }
        catch (Exception e)
        {
            throw new RuntimeException("unable to initialize connection pool", e);
        }

        return poolableConnectionFactory;
    }


    public void setProperty(String name, String value)
    {
        this.getDriverProperties().setProperty(name, value);
    }

    public boolean isUseStatementPooling()
    {
        return useStatementPooling;
    }

    /**
     * sets whether statement pooling should be used. Default is true. Statement pooling can increase performance.
     * @param useStatementPooling
     */
    public void setUseStatementPooling(boolean useStatementPooling)
    {
        if (this.connectionPool != null)
        {
            logger.warn("Calling setUseStatementPooling after initializing the pool has no effect! Please call setUseStatementPooling before.",
                    new Exception("setUseStatementPooling called after initializePool. Not an error, just for tracing"));
        }
        this.useStatementPooling = useStatementPooling;
    }

    /**
     * gets a connection from the pool. initializePool() must've been called before calling this method.
     * If all connections are in use, this method will block, unless maxWait has been set.
     * @return a connection.
     */
    public Connection getConnection()
    {
        Connection connection;

        try
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("before: active : " + connectionPool.getNumActive() + " idle: " + connectionPool.getNumIdle());
            }
            connection = dataSource.getConnection();
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("after: active : " + connectionPool.getNumActive() + " idle: " + connectionPool.getNumIdle() + " giving " + connection);
            }
        }
        catch (Exception e)
        {
            getLogger().error("Unable to get connection " + this, e);
            throw new MithraDatabaseException("could not get connection", e);
        }
        if (connection == null)
        {
            throw new MithraDatabaseException("could not get connection"+this);
        }
        return connection;
    }

    public int getNumberOfActiveConnections()
    {
        return connectionPool.getNumActive();
    }

    public int getNumberOfIdleConnections()
    {
        return connectionPool.getNumIdle();
    }

    /**
     * sets the LDAP name that is used to resolve this connection.
     * @param ldapServerName for example: "NYPARAD01"
     */
    public void setLdapName(String ldapServerName)
    {
        this.serverName = ldapServerName;
    }

    public String getLdapName()
    {
        return serverName;
    }

    /**
     * @deprecated use setLdapName instead
     * @param serverName
     */
    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }

    /**
     * @deprecated use getLdapName instead
     * @return the LDAP name used to resolve this connection
     */
    public String getServerName()
    {
        return serverName;
    }

    public String getDefaultSchemaName()
    {
        return resourceName;
    }

    /**
     * sets the default schema name. The connection is issued a setCatalog(schemaName) command when the connection
     * is initially created.
     * @param schemaName
     */
    public void setDefaultSchemaName(String schemaName)
    {
        this.resourceName = schemaName;
    }

    /**
     * @deprecated use getDefaultSchemaName instead
     * @return the default schema name.
     */
    public String getResourceName()
    {
        return resourceName;
    }

    /**
     * @deprecated This is an alias for setDefaultSchemaName.
     * @param resourceName
     */
    public void setResourceName(String resourceName)
    {
        this.resourceName = resourceName;
    }

    public String getPoolName()
    {
        return poolName;
    }

    public void setPoolName(String poolName)
    {
        this.poolName = poolName;
    }

    public int getPoolSize()
    {
        return poolSize;
    }

    /**
     * sets the maximum pool size. The pool will not grow larger than this number.
     * @param poolSize
     */
    public void setPoolSize(int poolSize)
    {
        this.poolSize = poolSize;
    }

    public int getInitialSize()
    {
        return initialSize;
    }

    /**
     * sets the initial size of the pool. These connections will be established when initalizePool() is called.
     * @param initialSize
     */
    public void setInitialSize(int initialSize)
    {
        this.initialSize = initialSize;
    }

	public int getMaxWait()
	{
		return maxWait;
	}

    /**
     * sets the maximum amount of time to wait if there are no connections available. A value of zero
     * specifies no timeout (fully blocking) behavior and is the default.
     * @param maxWait
     */
    public void setMaxWait(int maxWait)
	{
		this.maxWait = maxWait;
	}

    public String getJdbcPassword()
    {
        return jdbcPassword;
    }

    /**
     * sets the password for the connection.
     * @param jdbcPassword
     */
    public void setJdbcPassword(String jdbcPassword)
    {
        this.jdbcPassword = jdbcPassword;
    }

    public String getJdbcUser()
    {
        return jdbcUser;
    }

    /**
     * sets the user for the connection.
     * @param jdbcUser
     */
    public void setJdbcUser(String jdbcUser)
    {
        this.jdbcUser = jdbcUser;
    }

    public String getJdbcConnectionString()
    {
        return jdbcConnectionString;
    }

    /**
     * sets the JDBC connection string. If this value is set, LDAP will <b>not</b> be used.
     * @param jdbcConnectionString for example: "jdbc:jtds:sybase://foo.example.com:6111/accountdb"
     */
    public void setJdbcConnectionString(String jdbcConnectionString)
    {
        this.jdbcConnectionString = jdbcConnectionString;
    }

    public Driver getDriver()
    {
        return driver;
    }

    public Properties getDriverProperties()
    {
        return driverProperties;
    }

    /**
     * @deprecated Please use the setProperty method to set individual properties instead.
     * @param driverProperties will be passed to the driver when pool is initialized.
     *                         properties can contain something like these: com.gs.fw.aig.jdbc.properties*, com.gs.fw.aig.jdbc*, java.naming*, com.sybase*
     */
    public void setDriverProperties(Properties driverProperties)
    {
        this.driverProperties = driverProperties;
    }

    /**
     * sets the driver class name. This is used in conjunction with the JDBC connection string
     * @param driver the driver class name, for example "com.sybase.jdbc4.jdbc.SybDriver"
     */
    public void setDriverClassName(String driver)
    {
        try
        {
            this.driver = (Driver) Class.forName(driver).newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load driver: " + driver, e);
        }
    }

    public long getTimeBetweenEvictionRunsMillis()
    {
        return timeBetweenEvictionRunsMillis;
    }

    /**
     * The number of milliseconds to sleep between runs of the
     * idle connection evictor thread.
     * When non-positive, no idle connection evictor thread will be
     * run.
     *
     * @param timeBetweenEvictionRunsMillis time in millis. Default is -1 (will not evict).
     */
    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
    {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public long getMinEvictableIdleTimeMillis()
    {
        return minEvictableIdleTimeMillis;
    }

    /**
     * The minimum amount of time a connection may sit idle in the pool
     * before it is eligible for eviction by the idle connection evictor
     * (if any).
     * When non-positive, no connections will be evicted from the pool
     * due to idle time alone.
     *
     * @param minEvictableIdleTimeMillis time in millis.  default is 30 minutes (1000*60*30),
     * but eviction time must be set for this to be meaningful.
     */
    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis)
    {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public long getSoftMinEvictableIdleTimeMillis()
    {
        return softMinEvictableIdleTimeMillis;
    }

    /**
     * The minimum amount of time a connection may sit idle in the pool
     * before it is eligible for eviction by the idle object evictor
     * (if any), with the extra condition that at least
     * "minIdle" amount of object remain in the pool.
     * When non-positive, no connections will be evicted from the pool
     * due to idle time alone.
     *
     * @param softMinEvictableIdleTimeMillis time in millis. default is 5 minutes (1000*60*5),
     * but eviction time must be set for this to be meaningful.
     */
    public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis)
    {
        this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("poolName: ").append(this.getPoolName()).append(", ");
        buf.append("defaultSchemaName: ").append(this.getDefaultSchemaName()).append(", ");
        buf.append("ldapName: ").append(getLdapName()).append(", ");
        buf.append("driver: ").append(this.getDriver()).append(", ");
        buf.append("initial size: ").append(this.getInitialSize()).append(", ");
        buf.append("pool size: ").append(this.getPoolSize()).append(", ");
		buf.append("max wait: ").append(this.getMaxWait());
        return buf.toString();
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public void shutdown()
    {
        try
        {
            if (this.connectionPool != null) this.connectionPool.close();
        }
        catch (Exception e)
        {
           getLogger().error("could not close connection pool", e);
        }
    }

    public static String getPid()
    {
        return MithraProcessInfo.getPid();
    }

    //todo:move this method to utility class if this if functionality is needed somewhere else.
    public static String getApplicationName()
    {
        if (APPLICATION_NAME == null)
        {
            String name = System.getProperty(APPLICATION_NAME_KEY);
            if (name == null)
            {
                name = "JDBC";
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds());
                long mainThreadNumber = -1;
                for(int i=0;i<threadInfos.length;i++)
                {
                    if (threadInfos[i] != null && threadInfos[i].getThreadName().equals("main"))
                    {
                        mainThreadNumber = threadInfos[i].getThreadId();
                        break;
                    }
                }
                if (mainThreadNumber >= 0)
                {
                    ThreadInfo threadInfo = threadMXBean.getThreadInfo(mainThreadNumber, Integer.MAX_VALUE);
                    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
                    if (stackTrace.length > 0)
                    {
                        String cname = stackTrace[stackTrace.length - 1].getClassName();
                        name = cname.substring(Math.max(0, cname.lastIndexOf(".") + 1), cname.length());
                    }
                }
                if (name.equals("ApplicationRunner"))
                {
                    name = System.getProperty("applicationrunner.properties");
                }
            }
            if (name.length() > 30)
            {
                name = name.substring(0, 30);
            }
            APPLICATION_NAME = name;
        }
        return APPLICATION_NAME;
    }

    protected void invokeDataSourcePropertySetters(Object obj, Map props)
    {
        Class c = obj.getClass();
        Method[] methods = c.getMethods();
        Map<String, Method> methodMap = new HashMap<String, Method>();
        for(int i=0;i<methods.length;i++)
        {
            String name = methods[i].getName();
            Class[] parameterTypes = methods[i].getParameterTypes();
            if (name.length() > 3 && name.startsWith("set") && parameterTypes != null && parameterTypes.length == 1 &&
                    (parameterTypes[0].equals(String.class) || parameterTypes[0].equals(Boolean.class) || parameterTypes[0].equals(Integer.class)))
            {
                String attributeName = name.substring(3, name.length());
                methodMap.put(attributeName, methods[i]);
                methodMap.put(attributeName.toLowerCase(), methods[i]);
            }
        }
        String className = c.getName();
        for(Iterator it = props.keySet().iterator(); it.hasNext();)
        {
            String propName = (String) it.next();
            String propValue = (String) props.get(propName);
            if (propName.indexOf(".") < 0)
            {
                setReflectiveProperty(methodMap, propName, obj, propValue);
            }
            else if (propName.startsWith(className) && propName.length() > className.length())
            {
                setReflectiveProperty(methodMap, propName.substring(className.length()+1), obj, propValue);
            }
        }

    }

    protected void setReflectiveProperty(Map<String, Method> methodMap, String propName, Object obj, String propValue)
    {
        Method method = methodMap.get(propName);
        if (method == null)
        {
            method = methodMap.get(propName.toLowerCase());
        }
        if (method != null)
        {
            Class paramType = method.getParameterTypes()[0];
            Object arg = propValue;
            if (paramType == Integer.class)
            {
                try
                {
                    arg = Integer.parseInt(propValue);
                }
                catch (NumberFormatException e)
                {
                    logger.warn("Property "+propName+" requires an integer value, but a non-integer "+propValue+" was supplied");
                    arg = null;
                }
            }
            if (paramType == Integer.class)
            {
                if (propValue.equalsIgnoreCase("TRUE"))
                {
                    arg = Boolean.TRUE;
                }
                else if (propValue.equalsIgnoreCase("FALSE"))
                {
                    arg = Boolean.FALSE;
                }
                else
                {
                    logger.warn("Property "+propName+" requires a boolean value, but a non-boolean "+propValue+" was supplied");
                    arg = null;
                }
            }
            try
            {
                if (arg != null)
                {
                    method.invoke(obj, new Object[] { arg });
                }
            }
            catch (IllegalAccessException e)
            {
                logger.warn("could not set property "+propName+" on object "+obj.getClass().getName());
            }
            catch (InvocationTargetException e)
            {
                logger.warn("could not set property "+propName+" on object "+obj.getClass().getName());
            }
        }
    }

    public static ClosableConnection getClosableConnection(Connection con)
    {
        while(true)
        {
            if (con instanceof ClosableConnection)
            {
                return (ClosableConnection) con;
            }
            if (con instanceof WrappedConnection)
            {
                con = ((WrappedConnection) con).getUnderlyingConnection();
            }
            else
            {
                return null;
            }
        }
    }
}
