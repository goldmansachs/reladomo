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
import com.gs.fw.common.mithra.notification.MithraNotificationMessagingAdapter;
import com.gs.fw.common.mithra.notification.MithraNotificationMessageHandler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;


public class TcpMessagingAdapterFactory implements MithraMessagingAdapterFactory, ClientNotificationHandler
{

    private NotificationClient client;
    private AtomicInteger adapters = new AtomicInteger();
    private ConcurrentHashMap<String, TcpNotificationMessagingAdapter> adapterMap = new ConcurrentHashMap<String, TcpNotificationMessagingAdapter>();

    public TcpMessagingAdapterFactory(String host, int port)
    {
        client = new NotificationClient(host, port, this);
        client.start();
    }

    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        adapters.incrementAndGet();
        TcpNotificationMessagingAdapter adapter = new TcpNotificationMessagingAdapter(subject);
        adapterMap.put(subject, adapter);
        return adapter;
    }

    public void shutdown()
    {
        client.shutdown();
    }

    public void handleMessage(String subject, byte[] message)
    {
        TcpNotificationMessagingAdapter handler = adapterMap.get(subject);
        if (handler != null)
        {
            handler.handleMessage(message);
        }
    }

    private class TcpNotificationMessagingAdapter implements MithraNotificationMessagingAdapter
    {
        private String subject;
        private MithraNotificationMessageHandler mithraHandler;

        private TcpNotificationMessagingAdapter(String subject)
        {
            this.subject = subject;
            client.subscribe(subject);
        }

        public void broadcastMessage(byte[] message)
        {
            client.broadcastNotification(subject, message);
        }

        public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
        {
            this.mithraHandler = messageHandler;
        }

        public void shutdown()
        {
            int left = adapters.decrementAndGet();
            adapterMap.remove(this.subject);
            if (left == 0)
            {
                TcpMessagingAdapterFactory.this.shutdown();
            }
        }

        public void handleMessage(byte[] message)
        {
            mithraHandler.processNotificationMessage(this.subject, message);
        }
    }
}
