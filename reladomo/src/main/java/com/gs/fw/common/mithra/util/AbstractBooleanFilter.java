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

import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;


public abstract class AbstractBooleanFilter implements BooleanFilter
{
    private static final FalseFilter FALSE = FalseFilter.instance();
    private static final TrueFilter TRUE = TrueFilter.instance();

    public BooleanFilter and(Filter that)
    {
        if (FALSE.equals(that) || TRUE.equals(that))
        {
            return ((BooleanFilter) that).and(this);
        }
        if (that instanceof AndFilter)
        {
            return ((AndFilter) that).and(this);
        }
        return new AndFilter(FastList.newListWith(this, that));
    }

    public BooleanFilter or(Filter that)
    {
        if (FALSE.equals(that) || TRUE.equals(that))
        {
            return ((BooleanFilter) that).or(this);
        }
        if (that instanceof OrFilter)
        {
            return ((OrFilter) that).or(this);
        }
        return new OrFilter(FastList.newListWith(this, that));
    }

    public BooleanFilter negate()
    {
        return new Negate(this);
    }

    public static class AndFilter extends AbstractBooleanFilter
    {
        private final List<Filter> nodes;

        public AndFilter(List<Filter> nodes)
        {
            this.nodes = nodes;
        }

        public boolean matches(Object o)
        {
            for (int i = 0; i < nodes.size(); i++)
            {
                if (!nodes.get(i).matches(o)) return false;
            }

            return true;
        }

        public BooleanFilter and(Filter that)
        {
            List list = FastList.newList(this.nodes);
            if (that instanceof AndFilter)
            {
                list.addAll(((AndFilter) that).nodes);
            }
            else
            {
                list.add(that);
            }

            return new AndFilter(list);
        }

        @Override
        public String toString()
        {
            return "{" + toStringSeparatedBy(nodes, " && ") + '}';
        }
    }

    public static class OrFilter extends AbstractBooleanFilter
    {
        private final List<Filter> nodes;

        public OrFilter(List<Filter> nodes)
        {
            this.nodes = nodes;
        }

        public boolean matches(Object o)
        {
            for (int i = 0; i < nodes.size(); i++)
            {
                if (nodes.get(i).matches(o))
                {
                    return true;
                }
            }

            return false;
        }

        public BooleanFilter or(Filter that)
        {
            List list = FastList.newList(this.nodes);
            if (that instanceof OrFilter)
            {
                list.addAll(((OrFilter) that).nodes);
            }
            else
            {
                list.add(that);
            }

            return new OrFilter(list);
        }

        @Override
        public String toString()
        {
            return "{" + toStringSeparatedBy(nodes, "|| ") + '}';
        }
    }

    public static final class BooleanFilterAdapter extends AbstractBooleanFilter
    {
        private final Filter filter;

        public BooleanFilterAdapter(Filter filter)
        {
            this.filter = filter;
        }

        @Override
        public boolean matches(Object o)
        {
            return this.filter.matches(o);
        }

        @Override
        public String toString()
        {
            return this.filter.toString();
        }
    }

    private static class Negate extends AbstractBooleanFilter
    {
        private final Filter filter;

        public Negate(Filter filter)
        {
            this.filter = filter;
        }

        @Override
        public boolean matches(Object o)
        {
            return !this.filter.matches(o);
        }

        @Override
        public String toString()
        {
            return "Negate{" + filter + '}';
        }
    }

    private static String toStringSeparatedBy(List list, String separator)
    {
        StringBuilder builder = new StringBuilder(list.size()*10+separator.length()*list.size());
        for(int i=0;i<list.size() - 1; i++)
        {
            builder.append(list.get(i).toString()).append(separator);
        }
        builder.append(list.get(list.size() - 1));
        return builder.toString();
    }
}
