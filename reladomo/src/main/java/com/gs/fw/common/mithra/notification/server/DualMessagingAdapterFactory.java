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

import com.gs.fw.common.mithra.notification.DeDupedDualMessagingAdapter;
import com.gs.fw.common.mithra.notification.MithraMessagingAdapterFactory;
import com.gs.fw.common.mithra.notification.MithraNotificationMessagingAdapter;

/*
    Adapter factory that can be used to compose two factories into a single factory
 */
public class DualMessagingAdapterFactory implements MithraMessagingAdapterFactory
{
    private final MithraMessagingAdapterFactory factory1;
    private final MithraMessagingAdapterFactory factory2;

    public DualMessagingAdapterFactory(MithraMessagingAdapterFactory factory1, MithraMessagingAdapterFactory factory2)
    {
        this.factory1 = factory1;
        this.factory2 = factory2;
    }

    @Override
    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        return new DeDupedDualMessagingAdapter(subject, factory1, factory2);
    }

    @Override
    public void shutdown()
    {
        factory1.shutdown();
        factory2.shutdown();
    }
}
