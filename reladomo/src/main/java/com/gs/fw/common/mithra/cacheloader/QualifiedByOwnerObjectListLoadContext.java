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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.List;

public class QualifiedByOwnerObjectListLoadContext implements QualifiedLoadContext
{
    private List ownerObejcts;
    private String ownerClassName;

    public QualifiedByOwnerObjectListLoadContext(List ownerObejcts)
    {
        this.ownerObejcts = ownerObejcts;
        if (this.ownerObejcts == null || this.ownerObejcts.isEmpty())
        {
            throw new RuntimeException("nothing to do");
        }
        this.ownerClassName = this.ownerObejcts.get(0).getClass().getName();
        if (this.ownerObejcts.get(0) instanceof MithraDataObject)
        {
            MithraDataObject any = (MithraDataObject) this.ownerObejcts.get(0);
            String dataObjectName = any.zGetSerializationClassName();
            this.ownerClassName = dataObjectName.substring(0, dataObjectName.length() - "Data".length());
        }
    }

    public boolean qualifies(String className, boolean isDependentTask)
    {
        return isDependentTask;
    }

    public AdditionalOperationBuilder getAdditionalOperationBuilder(String className)
    {
        return null;
    }

    @Override
    public boolean qualifiesDependentsFor(String thatOwnerClassName, String dependentClassName)
    {
        return this.ownerClassName.equals(thatOwnerClassName);
    }

    @Override
    public List getKeyHoldersForQualifiedDependents(Operation operation, RelatedFinder finder)
    {
        return operation.applyOperation(this.ownerObejcts);
    }
}
