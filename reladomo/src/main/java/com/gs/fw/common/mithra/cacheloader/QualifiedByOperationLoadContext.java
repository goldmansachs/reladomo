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


import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.util.List;
import java.util.Map;

public class QualifiedByOperationLoadContext implements QualifiedLoadContext
{
    private boolean loadDependents;
    private Map<String, AdditionalOperationBuilder> classesToLoadWithAdditionalOperations;

    public QualifiedByOperationLoadContext(Map<String, AdditionalOperationBuilder> classesToLoadWithAdditionalOperations,
                                           boolean loadDependents)
    {
        this.classesToLoadWithAdditionalOperations = classesToLoadWithAdditionalOperations;
        this.loadDependents = loadDependents;
    }

    public boolean qualifies(String className, boolean isDependentTask)
    {
        return this.classesToLoadWithAdditionalOperations.containsKey(className) || (isDependentTask && this.loadDependents);
    }

    public AdditionalOperationBuilder getAdditionalOperationBuilder(String className)
    {
        return this.classesToLoadWithAdditionalOperations.get(className);
    }

    @Override
    public boolean qualifiesDependentsFor(String ownerClassName, String dependentClassName)
    {
        return classesToLoadWithAdditionalOperations.containsKey(dependentClassName);
    }

    @Override
    public List getKeyHoldersForQualifiedDependents(Operation operation, RelatedFinder finder)
    {
        return finder.findMany(operation);
    }
}
