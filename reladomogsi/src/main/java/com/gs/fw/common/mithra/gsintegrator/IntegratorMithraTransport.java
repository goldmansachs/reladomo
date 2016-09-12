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
import com.gs.fw.aig.intgr.bus.IConsumer;
import com.gs.fw.aig.intgr.bus.IPublisher;
import com.gs.fw.aig.intgr.bus.ITransport;
import com.gs.fw.aig.intgr.config.ConsumerThreadCfg;
import com.gs.fw.aig.intgr.config.CustomPropertyDescriptorSet;
import com.gs.fw.aig.intgr.config.CustomStringPropertyDescriptor;
import com.gs.fw.aig.intgr.config.PublisherThreadCfg;

public class IntegratorMithraTransport implements ITransport
{

    private static final String TRANSPORT_NAME = "IntegratorMithraTransport";
    private static final String TRANSPORT_VERSION = "1.0";

    public IConsumer createNewConsumer(ConsumerThreadCfg threadCfg) throws IntgrException
    {
        throw new IntgrException( "consumer not supported" );
    }

    public IPublisher createNewPublisher(PublisherThreadCfg threadCfg) throws IntgrException
    {
        return new IntegratorMithraPublisher(threadCfg);
    }

    public void getCustomPropertyDescriptors(CustomPropertyDescriptorSet descriptors)
    {
        descriptors.add( new CustomStringPropertyDescriptor( IntegratorMithraPublisher.PUBLISHER_PLUGIN_CLASS ) );
        descriptors.add( new CustomStringPropertyDescriptor( IntegratorMithraPublisher.MAX_OPERATION_TO_BUFFER_KEY ) );
    }

    public void getConsumerThreadCustomPropertyDescriptors(CustomPropertyDescriptorSet descriptors)
    {
        // no custom properties
    }

    public void getPublisherThreadCustomPropertyDescriptors(CustomPropertyDescriptorSet descriptors)
    {
        // no custom properties
    }

    public void getConsumerTopicCustomPropertyDescriptors(CustomPropertyDescriptorSet descriptors)
    {
        // no custom properties
    }

    public void getPublisherTopicCustomPropertyDescriptors(CustomPropertyDescriptorSet descriptors)
    {
        // no custom properties
    }

    public boolean hasConsumer()
    {
        return false;
    }

    public boolean hasPublisher()
    {
        return true;
    }

    public String getDefaultName()
    {
        return TRANSPORT_NAME;
    }

    public String getVersion()
    {
        return TRANSPORT_VERSION;
    }

    public String getDescription()
    {
        return TRANSPORT_NAME + " " +TRANSPORT_VERSION;
    }
}
