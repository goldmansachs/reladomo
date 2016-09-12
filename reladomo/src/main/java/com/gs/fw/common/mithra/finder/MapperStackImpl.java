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
import com.gs.fw.common.mithra.util.InternalList;

import java.util.Map;



public class MapperStackImpl
implements MapperStack, Cloneable, Comparable
{

    private static final InternalList EMPTY_LIST = new InternalList(0);
    private InternalList mapperStack = EMPTY_LIST;
    private InternalList mapperContainerStack = EMPTY_LIST;

    public static final MapperStackImpl EMPTY_MAPPER_STACK_IMPL = new MapperStackImpl(EMPTY_LIST, EMPTY_LIST);

    public MapperStackImpl()
    {
    }

    public MapperStackImpl(InternalList mapperStack, InternalList mapperContainerStack)
    {
        this.mapperStack = mapperStack;
        this.mapperContainerStack = mapperContainerStack;
    }

    public void pushMapper(Mapper mapper)
    {
        if (this.mapperStack == EMPTY_LIST)
        {
            this.mapperStack = new InternalList(3);
        }
        this.mapperStack.add(mapper);
    }

    public Mapper popMapper()
    {
        return (Mapper) this.mapperStack.remove(this.mapperStack.size()-1);
    }

    public void pushMapperContainer(Object mapper)
    {
        if (this.mapperContainerStack == EMPTY_LIST)
        {
            this.mapperContainerStack = new InternalList(3);
        }
        this.mapperContainerStack.add(new MapperContainer(mapper, this.mapperStack.size()));
    }

    public void popMapperContainer()
    {
        this.mapperContainerStack.remove(this.mapperContainerStack.size()-1);
    }

    public MapperStackImpl getCurrentMapperList()
    {
        return this;
    }

    public ObjectWithMapperStack constructWithMapperStack(Object o)
    {
        return new ObjectWithMapperStack(this, o);
    }

    public ObjectWithMapperStack constructWithMapperStackPoppedToLastContainerBoundary(Object o)
    {
        MapperStackImpl copy = this;
        if (this.isAtContainerBoundry())
        {
            copy = (MapperStackImpl) this.clone();
            while (copy.isAtContainerBoundry())
            {
                copy.popMapperContainer();
            }
        }
        return new ObjectWithMapperStack(copy, o);
    }

    public ObjectWithMapperStack constructWithMapperStack(Object o, Map<MapperStackImpl, MapperStackImpl> msiPool)
    {
        return new ObjectWithMapperStack(this, o, msiPool);
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o, Map<MapperStackImpl, MapperStackImpl> msiPool)
    {
        MapperStackImpl copy = cloneWithoutLastMapper();
        return new ObjectWithMapperStack(copy, o, msiPool);
    }

    private MapperStackImpl cloneWithoutLastMapper()
    {
        MapperStackImpl copy;
        if (this.mapperStack.size() == 1 && (this.mapperContainerStack.size() == 0 || (this.mapperContainerStack.size() == 1 && ((MapperContainer)this.mapperContainerStack.get(0)).getMapperStackLength() == 0)))
        {
            copy = EMPTY_MAPPER_STACK_IMPL;
        }
        else
        {
            copy = (MapperStackImpl) this.clone();
            copy.popMapper();
            while (copy.isAtContainerBoundry())
            {
                copy.popMapperContainer();
            }
        }
        return copy;
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o)
    {
        MapperStackImpl copy = cloneWithoutLastMapper();
        return new ObjectWithMapperStack(copy, o);
    }

    public MapperStackImpl ifAtContainerBoundaryCloneAndPop()
    {
        MapperStackImpl copy = this;
        if (this.isAtContainerBoundry())
        {
            if (this.mapperContainerStack.size() == 1 && this.mapperStack.size() == 0)
            {
                copy = EMPTY_MAPPER_STACK_IMPL;
            }
            else
            {
                copy = (MapperStackImpl) this.clone();
                while (copy.isAtContainerBoundry())
                {
                    copy.popMapperContainer();
                }
            }
        }
        return copy;
    }

    public boolean isEmpty()
    {
        return this.mapperStack.isEmpty();
    }

    public Mapper getLastMapper()
    {
        return (Mapper)this.mapperStack.get(this.mapperStack.size()-1);
    }

    public InternalList getMapperStack()
    {
        return mapperStack;
    }

    public InternalList getMapperContainerStack()
    {
        return mapperContainerStack;
    }

    public boolean isAtContainerBoundry()
    {
        if (!this.mapperContainerStack.isEmpty())
        {
            MapperContainer lastContainer = (MapperContainer) this.mapperContainerStack.get(this.mapperContainerStack.size() - 1);
            return lastContainer.getMapperStackLength() == this.mapperStack.size();
        }
        return false;
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof MapperStackImpl))
        {
            return false;
        }

        final MapperStackImpl mapperStack1 = (MapperStackImpl) o;

        return mapperContainerStack.equals(mapperStack1.mapperContainerStack) && mapperStack.equals(mapperStack1.mapperStack);
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(mapperStack.hashCode(), mapperContainerStack.hashCode());
    }

    public Object clone()
    {
        InternalList mapperStack = this.mapperStack.isEmpty() ? EMPTY_LIST : this.mapperStack.copy();
        InternalList mapperContainerStack = this.mapperContainerStack.isEmpty() ? EMPTY_LIST : this.mapperContainerStack.copy();

        MapperStackImpl result = new MapperStackImpl(mapperStack, mapperContainerStack);
        return result;
    }

    public int compareTo(Object o)
    {
        MapperStackImpl other = (MapperStackImpl) o;
        int result = this.mapperStack.size() - other.mapperStack.size();
        if (result == 0)
        {
            result = this.mapperContainerStack.size() - other.mapperContainerStack.size();
        }
        return result;
    }

    public void clear()
    {
        this.mapperContainerStack.clear();
        this.mapperStack.clear();
    }

    public boolean isFullyEmpty()
    {
        return this.mapperContainerStack.isEmpty() && this.mapperStack.isEmpty();
    }

    public boolean isGreaterThanEqualsWithEqualityCheck(MapperStackImpl other)
    {
        int matchedHead = 0;
        int end = Math.min(this.mapperStack.size(), other.mapperStack.size());
        for(;matchedHead < end; matchedHead++)
        {
            if (!this.mapperStack.get(matchedHead).equals(other.mapperStack.get(matchedHead))) break;
        }
        return matchedHead == other.mapperStack.size() && this.mapperContainerStack.size() >= other.mapperContainerStack.size();
    }

    private static class MapperContainer
    {
        private Object container;
        private int mapperStackLength;

        public MapperContainer(Object container, int mapperStackLength)
        {
            this.container = container;
            this.mapperStackLength = mapperStackLength;
        }

        public int getMapperStackLength()
        {
            return mapperStackLength;
        }

        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof MapperContainer))
            {
                return false;
            }

            final MapperContainer mapperContainer = (MapperContainer) o;

            return container.equals(mapperContainer.container);

        }

        public int hashCode()
        {
            return container.hashCode();
        }
    }
}
