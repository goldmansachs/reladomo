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

package com.gs.fw.common.mithra.util;

import org.eclipse.collections.api.LazyShortIterable;
import org.eclipse.collections.api.ShortIterable;
import org.eclipse.collections.api.bag.primitive.MutableShortBag;
import org.eclipse.collections.api.block.function.primitive.ObjectShortToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ShortToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.ShortPredicate;
import org.eclipse.collections.api.block.procedure.primitive.ShortProcedure;
import org.eclipse.collections.api.iterator.ShortIterator;
import org.eclipse.collections.api.list.primitive.MutableShortList;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.primitive.ImmutableShortSet;
import org.eclipse.collections.api.set.primitive.MutableShortSet;
import org.eclipse.collections.api.set.primitive.ShortSet;
import org.eclipse.collections.impl.set.mutable.primitive.ShortHashSet;
import java.io.Serializable;

public class ConstantShortSet implements ShortSet, Serializable
{
    static final long serialVersionUID = 9009637564896448807L;
    private final ShortSet delegate;
    private final int hashCode;

    public ConstantShortSet(short[] shorts)
    {
        this.delegate = ShortHashSet.newSetWith(shorts).freeze();
        this.hashCode = this.delegate.hashCode();
    }

    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        return this.delegate.equals(other);
    }

    public int hashCode()
    {
        return this.hashCode;
    }


    @Override
    public ShortSet select(ShortPredicate predicate)
    {
        return delegate.select(predicate);
    }

    @Override
    public ShortSet reject(ShortPredicate predicate)
    {
        return delegate.reject(predicate);
    }

    @Override
    public <V> SetIterable<V> collect(ShortToObjectFunction<? extends V> function)
    {
        return delegate.collect(function);
    }

    @Override
    public ShortSet freeze()
    {
        return this;
    }

    @Override
    public ImmutableShortSet toImmutable()
    {
        return delegate.toImmutable();
    }

    @Override
    public ShortIterator shortIterator()
    {
        return delegate.shortIterator();
    }

    @Override
    public short[] toArray()
    {
        return delegate.toArray();
    }

    @Override
    public boolean contains(short value)
    {
        return delegate.contains(value);
    }

    @Override
    public boolean containsAll(short... source)
    {
        return delegate.containsAll(source);
    }

    @Override
    public boolean containsAll(ShortIterable source)
    {
        return delegate.containsAll(source);
    }

    @Override
    public void each(ShortProcedure procedure) {
        delegate.each(procedure);
    }

    @Override
    public void forEach(ShortProcedure procedure)
    {
        delegate.forEach(procedure);
    }

    @Override
    public short detectIfNone(ShortPredicate predicate, short ifNone)
    {
        return delegate.detectIfNone(predicate, ifNone);
    }

    @Override
    public int count(ShortPredicate predicate)
    {
        return delegate.count(predicate);
    }

    @Override
    public boolean anySatisfy(ShortPredicate predicate)
    {
        return delegate.anySatisfy(predicate);
    }

    @Override
    public boolean allSatisfy(ShortPredicate predicate)
    {
        return delegate.allSatisfy(predicate);
    }

    @Override
    public boolean noneSatisfy(ShortPredicate predicate)
    {
        return delegate.noneSatisfy(predicate);
    }

    @Override
    public MutableShortList toList()
    {
        return delegate.toList();
    }

    @Override
    public MutableShortSet toSet()
    {
        return delegate.toSet();
    }

    @Override
    public MutableShortBag toBag()
    {
        return delegate.toBag();
    }

    @Override
    public LazyShortIterable asLazy()
    {
        return delegate.asLazy();
    }

    @Override
    public <T> T injectInto(T injectedValue, ObjectShortToObjectFunction<? super T, ? extends T> function)
    {
        return delegate.injectInto(injectedValue, function);
    }

    @Override
    public long sum()
    {
        return delegate.sum();
    }

    @Override
    public short max()
    {
        return delegate.max();
    }

    @Override
    public short maxIfEmpty(short defaultValue)
    {
        return delegate.maxIfEmpty(defaultValue);
    }

    @Override
    public short min()
    {
        return delegate.min();
    }

    @Override
    public short minIfEmpty(short defaultValue)
    {
        return delegate.minIfEmpty(defaultValue);
    }

    @Override
    public double average()
    {
        return delegate.average();
    }

    @Override
    public double median()
    {
        return delegate.median();
    }

    @Override
    public short[] toSortedArray()
    {
        return delegate.toSortedArray();
    }

    @Override
    public MutableShortList toSortedList()
    {
        return delegate.toSortedList();
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }

    @Override
    public boolean notEmpty()
    {
        return delegate.notEmpty();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public String makeString()
    {
        return delegate.makeString();
    }

    @Override
    public String makeString(String separator)
    {
        return delegate.makeString(separator);
    }

    @Override
    public String makeString(String start, String separator, String end)
    {
        return delegate.makeString(start, separator, end);
    }

    @Override
    public void appendString(Appendable appendable)
    {
        delegate.appendString(appendable);
    }

    @Override
    public void appendString(Appendable appendable, String separator)
    {
        delegate.appendString(appendable, separator);
    }

    @Override
    public void appendString(Appendable appendable, String start, String separator, String end)
    {
        delegate.appendString(appendable, start, separator, end);
    }


}
