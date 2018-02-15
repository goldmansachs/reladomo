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

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.LazyIntIterable;
import org.eclipse.collections.api.bag.primitive.MutableIntBag;
import org.eclipse.collections.api.block.function.primitive.IntToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectIntToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.IntPredicate;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.SetIterable;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import java.io.Serializable;


public class ConstantIntSet implements IntSet, Serializable
{
    static final long serialVersionUID = -8269491323809863515L;
    private final IntSet delegate;
    private final int hashCode;


    public ConstantIntSet(int[] ints)
    {
        this.delegate = IntHashSet.newSetWith(ints).freeze();
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
    public IntSet select(IntPredicate predicate)
    {
        return delegate.select(predicate);
    }

    @Override
    public IntSet reject(IntPredicate predicate)
    {
        return delegate.reject(predicate);
    }

    @Override
    public <V> SetIterable<V> collect(IntToObjectFunction<? extends V> function)
    {
        return delegate.collect(function);
    }

    @Override
    public IntSet freeze()
    {
        return this;
    }

    @Override
    public ImmutableIntSet toImmutable()
    {
        return delegate.toImmutable();
    }

    @Override
    public IntIterator intIterator()
    {
        return delegate.intIterator();
    }

    @Override
    public int[] toArray()
    {
        return delegate.toArray();
    }

    @Override
    public boolean contains(int value)
    {
        return delegate.contains(value);
    }

    @Override
    public boolean containsAll(int... source)
    {
        return delegate.containsAll(source);
    }

    @Override
    public boolean containsAll(IntIterable source)
    {
        return delegate.containsAll(source);
    }

    @Override
    public void each(IntProcedure procedure) {
        delegate.each(procedure);
    }

    @Override
    public void forEach(IntProcedure procedure)
    {
        delegate.forEach(procedure);
    }

    @Override
    public int detectIfNone(IntPredicate predicate, int ifNone)
    {
        return delegate.detectIfNone(predicate, ifNone);
    }

    @Override
    public int count(IntPredicate predicate)
    {
        return delegate.count(predicate);
    }

    @Override
    public boolean anySatisfy(IntPredicate predicate)
    {
        return delegate.anySatisfy(predicate);
    }

    @Override
    public boolean allSatisfy(IntPredicate predicate)
    {
        return delegate.allSatisfy(predicate);
    }

    @Override
    public boolean noneSatisfy(IntPredicate predicate)
    {
        return delegate.noneSatisfy(predicate);
    }

    @Override
    public MutableIntList toList()
    {
        return delegate.toList();
    }

    @Override
    public MutableIntSet toSet()
    {
        return delegate.toSet();
    }

    @Override
    public MutableIntBag toBag()
    {
        return delegate.toBag();
    }

    @Override
    public LazyIntIterable asLazy()
    {
        return delegate.asLazy();
    }

    @Override
    public <T> T injectInto(T injectedValue, ObjectIntToObjectFunction<? super T, ? extends T> function)
    {
        return delegate.injectInto(injectedValue, function);
    }

    @Override
    public long sum()
    {
        return delegate.sum();
    }

    @Override
    public int max()
    {
        return delegate.max();
    }

    @Override
    public int maxIfEmpty(int defaultValue)
    {
        return delegate.maxIfEmpty(defaultValue);
    }

    @Override
    public int min()
    {
        return delegate.min();
    }

    @Override
    public int minIfEmpty(int defaultValue)
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
    public int[] toSortedArray()
    {
        return delegate.toSortedArray();
    }

    @Override
    public MutableIntList toSortedList()
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
