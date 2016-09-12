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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class H2DbServer
{
    private static final H2DbServer instance = new H2DbServer();
    private static boolean h2dbServerStarted = false;
    private static final Logger logger = LoggerFactory.getLogger(H2DbServer.class.getName());
    public static Logger getLogger()
    {
        return logger;
    }

    private H2DbServer()
    {
    }

    public void startH2DbServer()
    {
        if(!h2dbServerStarted)
        {
            h2dbServerStarted = true;
            try
            {
                initializeH2();
            }
            catch (Exception e)
            {
                getLogger().error("Unable to start H2 Database", e);
            }
        }

    }

    public void stopH2DbServer()
    {
        try
        {
            if(h2dbServerStarted)
            {
                getLogger().info("Stopping H2 database Server");
                // todo: the only way to shut it down is to use a connection
                // conn.createStatement().execute("SHUTDOWN");
                h2dbServerStarted = false;
                getLogger().info("H2 database Server is down");
            }
        }
        catch(Exception e)
        {
            getLogger().error("Unable to stop H2 Database",e);
        }
    }

    private void initializeH2() throws Exception
    {
        getLogger().info("Starting H2 database Server");
        Class.forName("org.h2.Driver").newInstance();
        getLogger().info("H2 database Server Started");
    }

    public static H2DbServer getInstance()
    {
        return instance;
    }
}
