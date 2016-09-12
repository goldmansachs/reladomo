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

package com.gs.fw.common.mithra.overlap;


import com.gs.collections.impl.list.mutable.*;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.connectionmanager.*;
import com.gs.fw.common.mithra.mithraruntime.*;
import com.gs.fw.common.mithra.util.*;

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
