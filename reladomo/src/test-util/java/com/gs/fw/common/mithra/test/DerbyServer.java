
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

import org.apache.derby.drda.NetworkServerControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DerbyServer
{
    private static final Logger logger = LoggerFactory.getLogger(DerbyServer.class.getName());
    public static Logger getLogger()
    {
        return logger;
    }
    
    private static final DerbyServer instance = new DerbyServer();
    private static boolean derbyServerStarted = false;

    private DerbyServer()
    {
    }

    public void startDerbyServer()
    {
        if(!derbyServerStarted)
        {
            derbyServerStarted = true;
            try
            {
                getLogger().info("Starting DerbyServer");
                startNetworkServer();
                getLogger().info("DerbyServer Started");
            }
            catch (Exception e)
            {
                logger.error("During derby DB initialisation!", e);
            }
        }
    }

    private void startNetworkServer() throws Exception
    {
        getLogger().info("Starting Network Server");
        System.setProperty("derby.drda.startNetworkServer", "true");
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        getLogger().info("Network Server Started");
        new NetworkServerControl();
    }

    public static DerbyServer getInstance()
    {
        return instance;
    }
}
