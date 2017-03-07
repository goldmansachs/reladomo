
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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SourceAttributeType;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.finder.Finder;
import com.gs.fw.finder.Operation;

import java.util.List;
import java.util.Set;

public interface RelatedFinder<Result> extends Finder<Result>
{

    public String getFinderClassName();

    public Result findOne(Operation operation);

    public Result findOneBypassCache(Operation operation);

    public MithraList<? extends Result> findMany(Operation operation);

    public MithraList<? extends Result> findManyBypassCache(Operation operation);

    public com.gs.fw.common.mithra.finder.Operation all();

    public int getSerialVersionId();

    public SourceAttributeType getSourceAttributeType();

    public Attribute getSourceAttribute();

    public Attribute[] getPrimaryKeyAttributes();

    public Attribute[] getPersistentAttributes();

    public AsOfAttribute[] getAsOfAttributes();

    public VersionAttribute getVersionAttribute();

    public List<RelatedFinder> getRelationshipFinders();

    public List<RelatedFinder> getDependentRelationshipFinders();

    public Attribute getAttributeByName(String attributeName);

    public RelatedFinder getRelationshipFinderByName(String relationshipName);

    public Function getAttributeOrRelationshipSelector(String attributeName);

    public MithraObjectPortal getMithraObjectPortal();

    public boolean isPure();

    public boolean isTemporary();

    public int getHierarchyDepth();

    public MithraList<? extends Result> constructEmptyList();

    public void registerForNotification(MithraApplicationClassLevelNotificationListener listener);

    public void registerForNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener);
}
