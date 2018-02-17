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

package com.gs.fw.common.mithra.test.cacheloader;

import com.gs.fw.common.mithra.cacheloader.xmlbinding.CacheLoaderType;
import com.gs.fw.common.mithra.cacheloader.xmlbinding.DependentLoaderType;
import com.gs.fw.common.mithra.cacheloader.xmlbinding.MithraCacheLoaderUnmarshaller;
import com.gs.fw.common.mithra.cacheloader.xmlbinding.TopLevelLoaderType;
import com.gs.fw.common.mithra.mithraruntime.ConnectionManagerType;
import com.gs.fw.common.mithra.mithraruntime.MithraObjectConfigurationType;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import com.gs.fw.common.mithra.mithraruntime.SchemaType;
import com.gs.fw.common.mithra.util.MithraConfigurationManager;
import com.gs.fw.common.mithra.util.Pair;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.block.factory.Predicates;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.ListAdapter;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.junit.Assert;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MithraCacheLoaderRuntimeConfigTestUtil
{
    public void performTestForCacheWith(FileInputStream runtimeConfigFile, FileInputStream cacheLoaderXmlFile)
    {
        try
        {
            Map<String, MithraObjectConfigurationType> allDefinedClasses = this.getAllDefinedClasses(runtimeConfigFile);
            Assert.assertNotNull(allDefinedClasses);
            Assert.assertFalse(allDefinedClasses.isEmpty());

            Pair<List<String>, List<String>> cacheLoaderResult = this.findNotFullCacheTypeClasses(allDefinedClasses, cacheLoaderXmlFile);
            this.assertEmpty("TopLevel objects are not FULL cacheType:" + cacheLoaderResult.getOne().toString(), cacheLoaderResult.getOne());
            this.assertEmpty("Dependent relationship objects are not FULL cacheType:" + cacheLoaderResult.getTwo().toString(), cacheLoaderResult.getTwo());
        }
        finally
        {
            try
            {
                if (runtimeConfigFile != null)
                {
                    runtimeConfigFile.close();
                }
                if (cacheLoaderXmlFile != null)
                {
                    cacheLoaderXmlFile.close();
                }
            }
            catch (IOException e)
            {
            }
        }
    }

    private Map<String, MithraObjectConfigurationType> getAllDefinedClasses(FileInputStream runtimeConfigFile)
    {
        final Map<String, MithraObjectConfigurationType> allDefinedClasses = UnifiedMap.newMap();

        MithraRuntimeType mithraRuntimeType = new MithraConfigurationManager().parseConfiguration(runtimeConfigFile);

        for (ConnectionManagerType connectionManagerType : mithraRuntimeType.getConnectionManagers())
        {
            for (SchemaType schemaType : connectionManagerType.getSchemas())
            {
                this.addToAllDefinedClasses(allDefinedClasses, schemaType.getMithraObjectConfigurations());
            }
            this.addToAllDefinedClasses(allDefinedClasses, connectionManagerType.getMithraObjectConfigurations());
        }
        return allDefinedClasses;
    }

    private void addToAllDefinedClasses(Map<String, MithraObjectConfigurationType> allDefinedClasses, List<MithraObjectConfigurationType> typeList)
    {
        for (MithraObjectConfigurationType configurationType : typeList)
        {
            allDefinedClasses.put(configurationType.getClassName(), configurationType);
        }
    }

    private Pair<List<String>, List<String>> findNotFullCacheTypeClasses(final Map<String, MithraObjectConfigurationType> allDefinedClasses, FileInputStream cacheLoaderXmlFile)
    {
        try
        {
            CacheLoaderType cacheLoaderType = new MithraCacheLoaderUnmarshaller().parse(new BufferedInputStream(cacheLoaderXmlFile), "");
            final List<String> topLevelNotFullClassNames = this.detectNotFullTopLevelObjects(allDefinedClasses, cacheLoaderType);
            final List<String> dependentNotFullClassNames = this.detectNotFullDependentObjects(allDefinedClasses, cacheLoaderType);
            return Pair.of(topLevelNotFullClassNames,dependentNotFullClassNames);
        }
        catch (Exception e)
        {
            Assert.fail("Encountered exception:" + e.getMessage());
            return null;
        }
    }

    private List<String> detectNotFullTopLevelObjects(final Map<String, MithraObjectConfigurationType> allDefinedClasses, CacheLoaderType cacheLoaderType)
    {
        final List<String> notFullClassNames = FastList.newList();

        MutableListMultimap<String,TopLevelLoaderType> classesByName = ListAdapter.adapt(cacheLoaderType.getTopLevelLoaders()).groupBy(new Function<TopLevelLoaderType, String>()
        {
            @Override
            public String valueOf(final TopLevelLoaderType topLevelLoaderType)
            {
                MithraObjectConfigurationType objectConfigurationType = allDefinedClasses.get(topLevelLoaderType.getClassToLoad());
                if (objectConfigurationType == null || !objectConfigurationType.getCacheType().isFull())
                {
                    notFullClassNames.add(topLevelLoaderType.getClassToLoad());
                }
                return topLevelLoaderType.getClassToLoad() + topLevelLoaderType.getSourceAttributes() + topLevelLoaderType.getFactoryClass();
            }
        });
        RichIterable<RichIterable<TopLevelLoaderType>> listOfDupes = classesByName.multiValuesView().select(Predicates.attributeGreaterThan(Functions.getSizeOf(), 1));
        this.assertEmpty("Found duplicates in TopLevel Loader:" + this.printListOfTopLevelLoaderType(listOfDupes), listOfDupes.toList());

        return notFullClassNames;
    }

    private List<String> detectNotFullDependentObjects(final Map<String, MithraObjectConfigurationType> allDefinedClasses, CacheLoaderType cacheLoaderType)
    {
        final List<String> notFullClassNames = FastList.newList();
        MutableListMultimap<String,DependentLoaderType> classesByName = ListAdapter.adapt(cacheLoaderType.getDependentLoaders()).groupBy(new Function<DependentLoaderType, String>()
        {
            @Override
            public String valueOf(final DependentLoaderType dependentLoaderType)
            {
                final String className = dependentLoaderType.getRelationship().substring(0, dependentLoaderType.getRelationship().lastIndexOf("."));
                MithraObjectConfigurationType objectConfigurationType = allDefinedClasses.get(className);
                if (objectConfigurationType == null || !objectConfigurationType.getCacheType().isFull())
                {
                    notFullClassNames.add(dependentLoaderType.getRelationship());
                }
                return dependentLoaderType.getRelationship() + dependentLoaderType.getHelperFactoryClass();
            }
        });
        RichIterable<RichIterable<DependentLoaderType>> listOfDupes = classesByName.multiValuesView().select(Predicates.attributeGreaterThan(Functions.getSizeOf(), 1));
        this.assertEmpty("Found duplicates in Dependent Relationship:" + this.printListOfDependentLoaderType(listOfDupes), listOfDupes.toList());
        return notFullClassNames;
    }

    private String printListOfDependentLoaderType(RichIterable<RichIterable<DependentLoaderType>> items)
    {
        final StringBuilder builder = new StringBuilder("[[");
        for (int i =0; i < items.toList().size(); i++)
        {
            RichIterable listOfBreaks = items.toList().get(i);
            DependentLoaderType dependentLoaderType = (DependentLoaderType)listOfBreaks.getFirst();
            builder.append(dependentLoaderType.getRelationship() + "/" +  dependentLoaderType.getHelperFactoryClass() + ",");
        }
        builder.append("]]");
        return builder.toString();
    }

    private String printListOfTopLevelLoaderType(RichIterable<RichIterable<TopLevelLoaderType>> items)
    {
        final StringBuilder builder = new StringBuilder("[[");
        for (int i =0; i < items.toList().size(); i++)
        {
            RichIterable listOfBreaks = items.toList().get(i);
            TopLevelLoaderType topLevelLoaderType = (TopLevelLoaderType)listOfBreaks.getFirst();
            builder.append(topLevelLoaderType.getClassToLoad() + "/" + topLevelLoaderType.getSourceAttributes() + "/" + topLevelLoaderType.getFactoryClass() + ",");
        }
        builder.append("]]");
        return builder.toString();
    }

    private void assertEmpty(String collectionName, Collection<?> actualCollection)
    {
        Assert.assertNotNull(collectionName + " should not be null", collectionName);

        if (!actualCollection.isEmpty())
        {
            Assert.fail(collectionName + " should be empty; actual size:<" + actualCollection.size() + '>');
        }
    }
}
