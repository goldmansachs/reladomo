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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.common.mithra.util.ReflectionMethodCache;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;



public class FinderMethodMap
{

    private static Object[] emptyObjectArray = new Object[] {};
    private HashMap attributeToMethodMap = new HashMap();
    private HashMap relationshipToMethodMap = new HashMap();
    private HashSet normalAttributes = new HashSet();
    private HashSet relationshipAttributes = new HashSet();

    private Class relatedFinderClass;

    public FinderMethodMap(Class relatedFinderClass)
    {
        this.relatedFinderClass = relatedFinderClass;
    }

    protected Method populateAttributeToMethodMap(String attributeName)
    {
        Method method = null;
        try
        {
            method = ReflectionMethodCache.getZeroArgMethod(relatedFinderClass, attributeName);
            attributeToMethodMap.put(attributeName, method);
        }
        catch (ReflectionMethodCache.ReflectionMethodCacheException e)
        {
            // nothing to do, we'll just return null
        }
        return method;
    }

    public Attribute getAttributeByName(String attributeName, RelatedFinder target)
    {
        Attribute result = null;
        if (attributeName.indexOf('.') > 0)
        {
            StringTokenizer tokenizer = new StringTokenizer(attributeName, ".");
            String firstRelationship = tokenizer.nextToken();
            RelatedFinder relatedFinder = getRelationshipFinderByName(firstRelationship, target);
            if (relatedFinder != null && relatedFinder instanceof ToOneFinder)
            {
                result = relatedFinder.getAttributeByName(attributeName.substring(firstRelationship.length()+1, attributeName.length()));
            }
        }
        else
        {
            Method method = null;
            synchronized(attributeToMethodMap)
            {
                method = (Method) attributeToMethodMap.get(attributeName);
                if (method == null && normalAttributes.contains(attributeName))
                {
                    method = populateAttributeToMethodMap(attributeName);
                }
            }
            if (method != null)
            {
                try
                {
                    boolean oldState = method.isAccessible();
                    method.setAccessible(true);
                    result = (Attribute) method.invoke(target, emptyObjectArray);
                    method.setAccessible(oldState);
                }
                catch (IllegalAccessException e)
                {
                    throw new MithraBusinessException("unexpected exception", e);
                }
                catch (InvocationTargetException e)
                {
                    throw new MithraBusinessException("unexpected exception", e);
                }
            }
        }
        return result;
    }

    public Function getAttributeOrRelationshipSelectorFunction(String attributeName, RelatedFinder target)
    {
        Function result = null;
        if (attributeName.indexOf('.') > 0)
        {
            StringTokenizer tokenizer = new StringTokenizer(attributeName, ".");
            String firstRelationship = tokenizer.nextToken();
            RelatedFinder relatedFinder = getRelationshipFinderByName(firstRelationship, target);
            if (relatedFinder != null && relatedFinder instanceof ToOneFinder)
            {
                result = relatedFinder.getAttributeOrRelationshipSelector(attributeName.substring(firstRelationship.length() + 1, attributeName.length()));
            }
        }
        else
        {
            result = this.getAttributeByName(attributeName, target);
            if (result == null)
            {
                result = (Function) this.getRelationshipFinderByName(attributeName, target);
            }
        }
        return result;
    }

    public Function getAttributeOrRelationshipSelector(String attributeName, RelatedFinder target)
    {
        return getAttributeOrRelationshipSelectorAttributeValueSelector(attributeName, target);
    }

    public Function getAttributeOrRelationshipSelectorAttributeValueSelector(String attributeName, RelatedFinder target)
    {
        return getAttributeOrRelationshipSelectorFunction(attributeName, target);
    }

    protected Method populateRelationshipToMethodMap(String relationshipName)
    {
        Method method = null;
        try
        {
            method = ReflectionMethodCache.getZeroArgMethod(relatedFinderClass, relationshipName);
            relationshipToMethodMap.put(relationshipName, method);
        }
        catch (ReflectionMethodCache.ReflectionMethodCacheException e)
        {
            // nothing to do, we'll just return null
        }
        return method;
    }

    public RelatedFinder getRelationshipFinderByName(String nestedRelationshipName, RelatedFinder target)
    {
        RelatedFinder result = null;
        String[] relationshipNames = nestedRelationshipName.split("\\.", 2);
        String relationshipName = relationshipNames[0];
        synchronized(relationshipToMethodMap)
        {
            Method method = (Method) relationshipToMethodMap.get(relationshipName);
            if (method == null && relationshipAttributes.contains(relationshipName))
            {
                method = populateRelationshipToMethodMap(relationshipName);
            }
            if (method != null)
            {
                try
                {
                    result = (RelatedFinder) method.invoke(target, emptyObjectArray);
                }
                catch (IllegalAccessException e)
                {
                    throw new MithraBusinessException("unexpected exception", e);
                }
                catch (InvocationTargetException e)
                {
                    throw new MithraBusinessException("unexpected exception", e);
                }
            }
        }
        if(relationshipNames.length > 1 && result != null && result instanceof ToOneFinder)
        {
            return result.getRelationshipFinderByName(relationshipNames[1]);
        }
        return result;
    }

    public void addNormalAttributeName(String attributeName)
    {
        this.normalAttributes.add(attributeName);
    }

    public void addRelationshipName(String relationshipName)
    {
        this.relationshipAttributes.add(relationshipName);
    }
}
