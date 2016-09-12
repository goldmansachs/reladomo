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

import com.gs.fw.common.mithra.util.HashUtil;

import java.util.Map;



public class ObjectWithMapperStack<E> implements Comparable
{

    private MapperStackImpl mapperStack = MapperStackImpl.EMPTY_MAPPER_STACK_IMPL;
    private E object;
    private int hashcode;

    public ObjectWithMapperStack(MapperStackImpl mapperStack, E object)
    {
        if (mapperStack != null)
        {
            this.mapperStack = mapperStack.isFullyEmpty() ? MapperStackImpl.EMPTY_MAPPER_STACK_IMPL : (MapperStackImpl)mapperStack.clone();
        }
        this.object = object;
    }

    public ObjectWithMapperStack(MapperStackImpl mapperStack, E object, Map<MapperStackImpl, MapperStackImpl> stackPool)
    {
        MapperStackImpl clone = mapperStack.isFullyEmpty() ? MapperStackImpl.EMPTY_MAPPER_STACK_IMPL : stackPool.get(mapperStack);
        if (clone == null)
        {
            clone = (MapperStackImpl)mapperStack.clone();
            stackPool.put(clone, clone);
        }
        this.mapperStack = clone;
        this.object = object;
    }

    public MapperStackImpl getMapperStack()
    {
        return mapperStack;
    }

    public E getObject()
    {
        return object;
    }

    public void resetWithoutClone(MapperStackImpl mapperStack, E object)
    {
        this.mapperStack = mapperStack;
        this.object = object;
        hashcode = 0;
    }

    public ObjectWithMapperStack<E> copyWithInternalClone(Map<MapperStackImpl, MapperStackImpl> msiPool)
    {
        return new ObjectWithMapperStack<E>(mapperStack, this.object, msiPool);
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof ObjectWithMapperStack))
        {
            return false;
        }

        final ObjectWithMapperStack objectWithMapperStack = (ObjectWithMapperStack) o;

        if (mapperStack != objectWithMapperStack.mapperStack && !mapperStack.equals(objectWithMapperStack.mapperStack))
        {
            return false;
        }
        return object.equals(objectWithMapperStack.object);

    }

    public int hashCode()
    {
        if (hashcode == 0)
        {
            hashcode = HashUtil.combineHashes(mapperStack.hashCode(), object.hashCode());
        }
        return hashcode;
    }

    public int compareTo(Object o)
    {
        ObjectWithMapperStack other = (ObjectWithMapperStack) o;
        return this.mapperStack.compareTo(other.mapperStack);
    }
}
