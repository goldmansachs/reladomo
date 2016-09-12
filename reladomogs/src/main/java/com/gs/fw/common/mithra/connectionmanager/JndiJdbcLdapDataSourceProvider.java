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


import com.gs.fw.aig.jdbc.JdbcInitialDirContext;

import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.sql.DataSource;
import java.util.Enumeration;
import java.util.Properties;

public class JndiJdbcLdapDataSourceProvider implements LdapDataSourceProvider
{
    private static final String JAVA_NAMING_FACTORY_INITIAL = "java.naming.factory.initial";
    protected static final String USE_JNDI_JDBC_CONNECTION_POOL_KEY = "com.gs.fw.aig.jdbc.useConnectionPool";

    @Override
    public DataSource createLdapDataSource(Properties loginProperties, String ldapName) throws NamingException
    {
        DataSource ds;
        // borisv: this code stores all parameters in static baseEnvironment. So if you have multiple connections, next connection will get parameters from previous (unless overwriten by loginParamters).
        loginProperties.put(USE_JNDI_JDBC_CONNECTION_POOL_KEY, "false");
        if (loginProperties.getProperty(JAVA_NAMING_FACTORY_INITIAL) == null)
        {
            loginProperties.put(JAVA_NAMING_FACTORY_INITIAL, "com.sun.jndi.ldap.LdapCtxFactory");
        }
        InitialDirContext context = new JdbcInitialDirContext(loginProperties);

        Enumeration propKeys = loginProperties.keys();
        while(propKeys.hasMoreElements())
        {
            Object key = propKeys.nextElement();
            context.addToEnvironment((String)key, loginProperties.get(key));
        }

        ds = (DataSource) context.lookup(ldapName);
        return ds;
    }
}
