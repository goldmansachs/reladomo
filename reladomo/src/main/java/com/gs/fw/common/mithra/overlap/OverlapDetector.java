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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.overlap;


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.connectionmanager.PropertiesBasedConnectionManager;
import com.gs.fw.common.mithra.mithraruntime.CacheType;
import com.gs.fw.common.mithra.mithraruntime.ConnectionManagerType;
import com.gs.fw.common.mithra.mithraruntime.MithraObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import org.eclipse.collections.impl.list.mutable.FastList;

public class OverlapDetector
{
    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            throw new IllegalArgumentException("Usage: mithraClassName [source]");
        }
        String mithraClassName = args[0];
        String source = args.length > 1 ? args[1] : null;
        OverlapHandler handler = new OverlapReporter(System.out);

        MithraRuntimeType mithraRuntimeType = new MithraRuntimeType();
        ConnectionManagerType connectionManagerType = new ConnectionManagerType();
        connectionManagerType.setClassName(PropertiesBasedConnectionManager.class.getName());
        MithraObjectConfigurationType mithraObjectConfigurationType = new MithraObjectConfigurationType();
        mithraObjectConfigurationType.setClassName(mithraClassName);
        mithraObjectConfigurationType.setCacheType(CacheType.NONE);
        connectionManagerType.setMithraObjectConfigurations(FastList.newListWith(mithraObjectConfigurationType));
        mithraRuntimeType.setConnectionManagers(FastList.newListWith(connectionManagerType));
        MithraManagerProvider.getMithraManager().initializeRuntime(mithraRuntimeType);

        MithraObjectPortal mithraObjectPortal = MithraManagerProvider.getMithraManager().initializePortal(mithraClassName);
        OverlapProcessor processor = new OverlapProcessor(mithraObjectPortal, source, handler);
        processor.process();
    }

    private OverlapDetector()
    {
    }
}
