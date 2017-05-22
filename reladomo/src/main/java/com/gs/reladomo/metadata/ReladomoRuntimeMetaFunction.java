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

package com.gs.reladomo.metadata;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;

public class ReladomoRuntimeMetaFunction<T extends MithraObject, U extends MithraList<U>, V extends MithraDataObject>
{

    private final ReladomoClassMetaData classMetaData;

    public ReladomoRuntimeMetaFunction(ReladomoClassMetaData classMetaData)
    {
        this.classMetaData = classMetaData;
    }

    public V constructOnHeapData()
    {
        try
        {
            return (V) classMetaData.getOnHeapDataClass().newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not instantiate data class "+classMetaData.getOnHeapDataClass().getName());
        }
    }

    public U constructEmptyList()
    {
        return (U) this.classMetaData.getFinderInstance().constructEmptyList();
    }

    public boolean isConfigured()
    {
        return MithraManagerProvider.getMithraManager().getConfigManager().isClassConfigured(classMetaData.getBusinessOrInterfaceClassName());
    }


}
