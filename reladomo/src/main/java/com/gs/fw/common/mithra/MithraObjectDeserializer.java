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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.remote.RemoteMithraService;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;

import java.util.List;
import java.util.ArrayList;
import java.io.ObjectInput;
import java.io.IOException;


public interface MithraObjectDeserializer
{

    public List deserializeList(Operation op, ObjectInput in, boolean weak) throws IOException, ClassNotFoundException;

    public void deserializeForReload(ObjectInput in) throws IOException, ClassNotFoundException;

    public MithraObject deserializeForRefresh(ObjectInput in) throws IOException, ClassNotFoundException;

    public List<Operation> getOperationsForFullCacheLoad();

    public Cache instantiateFullCache(MithraConfigurationManager.Config config);

    public Cache instantiatePartialCache(MithraConfigurationManager.Config config);

    public List getSimulatedSequenceInitValues();

    public MithraObjectPortal getMithraObjectPortal();

    public MithraDataObject deserializeFullData(ObjectInput in) throws IOException, ClassNotFoundException;

    public abstract void analyzeChangeForReload(PrimaryKeyIndex fullUniqueIndex, MithraDataObject data, List newDataList, List updatedDataList);
}
