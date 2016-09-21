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

package sample;

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sample.domain.Person;
import sample.util.H2ConnectionManager;

import java.io.InputStream;

public class HelloReladomoApp
{
    private static Logger logger = LoggerFactory.getLogger(HelloReladomoApp.class.getName());
    private static final int MAX_TRANSACTION_TIMEOUT = 120;

    public static void main(String[] args)
    {
        HelloReladomoApp app = new HelloReladomoApp();
        initialize(app);

        Person.createPerson("Taro", "Tanaka", "JPN");

        Person personFromDb = Person.findPersonNamed("Taro", "Tanaka");

        logger.info("Hello " + personFromDb.getFullName());
    }

    private static void initialize(HelloReladomoApp app)
    {
        try
        {
            // This line is added to make in-memory tables ready.
            // It's not typically done in production app that usually connects to physical db
            H2ConnectionManager.getInstance().prepareTables();

            app.initialiseReladomo();
            app.loadReladomoConfigurationXml("reladomo/config/ReladomoRuntimeConfig.xml");
        }
        catch(Exception e)
        {
            logger.error("Couldn't run HelloReladomoApp.", e);
        }
    }

    private void initialiseReladomo() throws Exception
    {
        try
        {
            logger.info("Transaction Timeout is " + MAX_TRANSACTION_TIMEOUT);
            MithraManager mithraManager = MithraManagerProvider.getMithraManager();
            mithraManager.setTransactionTimeout(MAX_TRANSACTION_TIMEOUT);
            // Notification should be configured here. Refer to notification/Notification.html under reladomo-javadoc.jar.
        }
        catch (Exception e)
        {
            logger.error("Unable to initialise Reladomo!", e);
            throw new Exception("Unable to initialise Reladomo!", e);
        }
        logger.info("Reladomo has been initialised!");
    }

    private void loadReladomoConfigurationXml(String reladomoRuntimeConfig) throws Exception
    {
        logger.info("Reladomo configuration XML is " + reladomoRuntimeConfig);
        InputStream is = HelloReladomoApp.class.getClassLoader().getResourceAsStream(reladomoRuntimeConfig);
        if (is == null) throw new Exception("can't find file: " + reladomoRuntimeConfig + " in classpath");
        MithraManagerProvider.getMithraManager().readConfiguration(is);
        try
        {
            is.close();
        }
        catch (java.io.IOException e)
        {
            logger.error("Unable to initialise Reladomo!", e);
            throw new Exception("Unable to initialise Reladomo!", e);
        }
        logger.info("Reladomo configuration XML " + reladomoRuntimeConfig+" is now loaded.");
    }
}
