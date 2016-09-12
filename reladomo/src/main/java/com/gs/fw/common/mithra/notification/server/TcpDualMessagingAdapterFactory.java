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

package com.gs.fw.common.mithra.notification.server;


import com.gs.fw.common.mithra.notification.MithraMessagingAdapterFactory;
import com.gs.fw.common.mithra.notification.MithraNotificationMessageHandler;
import com.gs.fw.common.mithra.notification.MithraNotificationMessagingAdapter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TcpDualMessagingAdapterFactory implements MithraMessagingAdapterFactory, ClientNotificationHandler
{
    private final DualNotificationClient dualClient;
    private AtomicInteger adapters = new AtomicInteger((int) (1000 * Math.random()));  // Random start point to avoid accidental reliance or expectations on this value
    private ConcurrentHashMap<String, TcpDualNotificationMessagingAdapter> adapterMap = new ConcurrentHashMap<String, TcpDualNotificationMessagingAdapter>();

    public TcpDualMessagingAdapterFactory(String host1, int port1, String host2, int port2)
    {
        dualClient = new DualNotificationClient(host1, port1, host2, port2, this);
        dualClient.start();
    }

    @Override
    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        adapters.incrementAndGet();
        TcpDualNotificationMessagingAdapter adapter = new TcpDualNotificationMessagingAdapter(subject);
        adapterMap.put(subject, adapter);
        return adapter;
    }

    @Override
    public void shutdown()
    {
        dualClient.shutdown();
    }

    @Override
    public void handleMessage(String subject, byte[] message)
    {
        TcpDualNotificationMessagingAdapter handler = adapterMap.get(subject);
        if (handler != null)
        {
            handler.handleMessage(message);
        }
    }


    private class TcpDualNotificationMessagingAdapter implements MithraNotificationMessagingAdapter
    {
        private String subject;
        private MithraNotificationMessageHandler mithraHandler;

        private TcpDualNotificationMessagingAdapter(String subject)
        {
            this.subject = subject;
            dualClient.subscribe(subject);
        }

        @Override
        public void broadcastMessage(byte[] message)
        {
            dualClient.broadcastNotification(subject, message);
        }

        @Override
        public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
        {
            this.mithraHandler = messageHandler;
        }

        @Override
        public void shutdown()
        {
            int left = adapters.decrementAndGet();
            adapterMap.remove(this.subject);
            if (left == 0)
            {
                TcpDualMessagingAdapterFactory.this.shutdown();
            }
        }

        public void handleMessage(byte[] message)
        {
            mithraHandler.processNotificationMessage(this.subject, message);
        }
    }
}
