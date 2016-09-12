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

package com.gs.fw.common.mithra.gsintegrator;

import com.gs.fw.aig.intgr.IntgrException;
import com.gs.fw.aig.intgr.bus.IPublisher;
import com.gs.fw.aig.intgr.bus.ITxManager;
import com.gs.fw.aig.intgr.config.CustomPropertySet;
import com.gs.fw.aig.intgr.config.CustomStringProperty;
import com.gs.fw.aig.intgr.config.PublisherThreadCfg;
import com.gs.fw.aig.intgr.config.PublisherTopicCfg;
import com.gs.fw.aig.intgr.store.SafeStoreDatum;
import com.gs.fw.aig.intgr.store.SafeStoreResourceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;

import java.sql.Connection;

public class IntegratorMithraPublisher implements IPublisher
{
    static private Logger logger = LoggerFactory.getLogger(IntegratorMithraPublisher.class.getName());

    private PublisherPlugin plugin;
    private IntegratorMithraTxManager txManager;
    private int maxOperationsToBuffer = 20;

    public static final String PUBLISHER_PLUGIN_CLASS = "IntegratorMithraPublisherPluginClass";
    public static final String MAX_OPERATION_TO_BUFFER_KEY = "IntegratorMithraMaxOperationsToBuffer";

    public IntegratorMithraPublisher(PublisherThreadCfg threadCfg) throws IntgrException
    {
        CustomPropertySet props = threadCfg.getProcess().getTransport().getCustomProperties();
        CustomStringProperty customStringProperty = ( ( CustomStringProperty ) props.get( PUBLISHER_PLUGIN_CLASS ) );
        if (customStringProperty == null)
        {
            throw new IntgrException("You must configure a publisher plugin class using "+PUBLISHER_PLUGIN_CLASS+" configuration parameter");
        }
        String pluginClassName = customStringProperty.getValue();
        initializePlugin(pluginClassName);
        customStringProperty = ( ( CustomStringProperty ) props.get( MAX_OPERATION_TO_BUFFER_KEY ) );
        if (customStringProperty != null)
        {
            try
            {
                maxOperationsToBuffer = Integer.parseInt(customStringProperty.getValue());
            }
            catch (NumberFormatException e)
            {
                logger.warn("Could not parse "+MAX_OPERATION_TO_BUFFER_KEY+" '"+customStringProperty.getValue()+"' . Please use an integer.");
            }
        }
    }

    public IntegratorMithraPublisher(String pluginClassName) throws IntgrException
    {
        initializePlugin(pluginClassName);
    }

    private void initializePlugin(String pluginClassName)
            throws IntgrException
    {
        if (pluginClassName == null)
        {
            throw new IntgrException("You must configure a publisher plugin class using "+PUBLISHER_PLUGIN_CLASS+" configuration parameter");
        }
        try
        {
            this.plugin = (PublisherPlugin) Class.forName(pluginClassName).newInstance();
            if (this.txManager != null)
            {
                txManager.setPlugin(this.plugin);
            }
        }
        catch (InstantiationException e)
        {
            throw new IntgrException("could not instantiate publisher plugin: "+pluginClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new IntgrException("could not instantiate publisher plugin: "+pluginClassName, e);
        }
        catch (ClassNotFoundException e)
        {
            throw new IntgrException("could not instantiate publisher plugin: "+pluginClassName, e);
        }
    }

    public boolean isJNI() throws IntgrException
    {
        return false;
    }

    public ITxManager createTxManager(Connection commitAgent) throws IntgrException
    {
        this.txManager = new IntegratorMithraTxManager(commitAgent, this);
        if (this.plugin != null)
        {
            txManager.setPlugin(this.plugin);
        }
        return this.txManager;
    }

    public ITxManager createTxManager(Long nativeConn) throws IntgrException
    {
        throw new IntgrException("native connection not supported");
    }

    public Object getSenderInfoToCache(PublisherTopicCfg topicCfg) throws IntgrException
    {
        // gsi cache is not used in this publisher
        return null;
    }

    public void addSender(PublisherTopicCfg topicCfg, Object cache) throws IntgrException
    {
        this.logger.info("adding sender with cache "+topicCfg);
    }

    public void addSender(PublisherTopicCfg topicCfg) throws IntgrException
    {
        this.logger.info("adding sender "+topicCfg);
    }

    public void send(SafeStoreDatum datum) throws IntgrException
    {
        this.txManager.addMessage(datum);
        SafeStoreResourceName name = new SafeStoreResourceName(datum.resourceName);
        String stream = name.getResourceStream();
        String id = name.getResourceStreamIndex();
        int sequenceNumber = datum.seqNo;
        try
        {
            IntegratorDataSequence sequence = IntegratorDataSequenceFinder.findOne(
                    IntegratorDataSequenceFinder.stream().eq(stream));
            if (sequence == null)
            {
                sequence  = new IntegratorDataSequence();
                sequence.setStream(stream);
                sequence.setId(id);
                sequence.setSeqNo(sequenceNumber);
                sequence.insert();
                sendNonDuplicateMessage(datum);
            }
            else
            {
                if (sequence.isDuplicate(id, sequenceNumber))
                {
                    this.logger.warn("Skipping duplicate message: "+datum.resourceName+" seq: "+datum.seqNo);
                }
                else
                {
                    sequence.setId(id);
                    sequence.setSeqNo(sequenceNumber);
                    sendNonDuplicateMessage(datum);
                }
            }
        }
        catch (MithraBusinessException e)
        {
            this.txManager.retryIfWarranted(e, false);
        }
    }

    private void sendNonDuplicateMessage(SafeStoreDatum datum)
    {
        this.plugin.send(datum);
        // integrator expects the commit call to be short, we therefore flush our operations here
        MithraManagerProvider.getMithraManager().getCurrentTransaction().executeBufferedOperationsIfMoreThan(maxOperationsToBuffer);
    }

    public void removeSender(PublisherTopicCfg topicCfg) throws IntgrException
    {
        this.logger.info("removing sender "+topicCfg);
    }

    public void destroy() throws IntgrException
    {
        // nothing to do
    }
}
