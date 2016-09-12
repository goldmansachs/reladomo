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
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;

import java.io.InputStream;


public class MithraTestApp
{
    private Logger logger = LoggerFactory.getLogger(MithraTestApp.class.getName());


    private static int maxTransactionTimeout = 120;

    public Logger getLogger()
    {
        return logger;
    }

    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }

    public static int getMaxTransactionTimeout()
    {
        return maxTransactionTimeout;
    }

    public static void setMaxTransactionTimeout(int maxTransactionTimeout)
    {
        MithraTestApp.maxTransactionTimeout = maxTransactionTimeout;
    }

    public static void main(String[] args)
    {
        MithraTestApp app = new MithraTestApp();
        try
        {
            app.initialiseMithra();
            app.loadMithraConfigurationXml("MithraRuntimeConfig1.xml");
            // optionally load multiple config files:
            app.loadMithraConfigurationXml("MithraRuntimeConfig2.xml");

            //Do your thing!!

        }
        catch(Exception e)
        {
            System.out.println(e);
        }

    }

    private void loadMithraConfigurationXml(String mithraXml)
            throws Exception
    {
        logger.info("Mithra configuration XML is " + mithraXml);
        InputStream is = MithraTestApp.class.getClassLoader().getResourceAsStream(mithraXml);
        if (is == null) throw new Exception("can't find file: " + mithraXml + " in classpath");
        MithraManagerProvider.getMithraManager().readConfiguration(is);
        try
        {
            is.close();
        }
        catch (java.io.IOException e)
        {
            logger.error("Unable to initialise Mithra!", e);
            throw new Exception("Unable to initialise Mithra!", e);
        }
        logger.info("Mithra configuration XML " + mithraXml+" is now loaded.");
    }

    private void initialiseMithra() throws Exception
    {
        try
        {
            logger.info("Transaction Timeout is " + this.getMaxTransactionTimeout());
            MithraManager mithraManager = MithraManagerProvider.getMithraManager();
            mithraManager.setTransactionTimeout(this.getMaxTransactionTimeout());
        }
        catch (Exception e)
        {
            logger.error("Unable to initialise Mithra!", e);
            throw new Exception("Unable to initialise Mithra!", e);
        }
        logger.info("Mithra has been initialised!");
    }
}
