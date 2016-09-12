
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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.extractor.NormalAndListValueSelector;
import com.gs.fw.finder.Navigation;

import java.util.List;

public interface DeepRelationshipAttribute<T, V> extends Navigation<T>, NormalAndListValueSelector<T, V>
{

    public String getRelationshipName();

    public Operation exists();

    public Operation notExists();

    public DeepRelationshipAttribute getParentDeepRelationshipAttribute();

    public void setParentDeepRelationshipAttribute(DeepRelationshipAttribute parent);

    public DeepRelationshipAttribute copy();

    public boolean isModifiedSinceDetachment(MithraTransactionalObject obj);
}
