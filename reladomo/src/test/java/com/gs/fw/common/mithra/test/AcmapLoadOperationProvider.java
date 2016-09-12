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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.LoadOperationProvider;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.List;


public class AcmapLoadOperationProvider implements LoadOperationProvider
{

    public List<Operation> getOperationsForFullCacheLoad(RelatedFinder finder)
    {
        return MithraTestAbstract.getOperationsForFullCacheLoad((StringAttribute) finder.getAttributeByName("acmapCode"),
                (AsOfAttribute) finder.getAttributeByName("businessDate"), (AsOfAttribute) finder.getAttributeByName("processingDate"));
    }
}
