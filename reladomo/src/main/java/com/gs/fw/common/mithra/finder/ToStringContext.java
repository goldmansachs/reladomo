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

package com.gs.fw.common.mithra.finder;


import com.gs.fw.common.mithra.MithraObjectPortal;
import org.eclipse.collections.impl.list.mutable.FastList;

public class ToStringContext
{
    private static final int NOTHING_PENDING = 0;
    private static final int PENDING_AND = 10;
    private static final int PENDING_OR = 20;

    private StringBuilder stringBuilder = new StringBuilder(32);
    private int pendingBrackets;
    private int pendingBoolean;
    private FastList<String> currentPath = new FastList<String>(4);

    public ToStringContext(MithraObjectPortal portal)
    {
        currentPath.add(portal.getBusinessClassName());
    }

    public ToStringContext append(CharSequence cs)
    {
        checkPending();
        if (stringBuilder.length() > 0) stringBuilder.append(' ');
        stringBuilder.append(cs);
        return this;
    }

    public ToStringContext append(double cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(int cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(boolean cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(byte cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(short cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(char cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(long cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public ToStringContext append(float cs)
    {
        checkPending();
        stringBuilder.append(' ').append(cs);
        return this;
    }

    public void beginAnd()
    {
        if (pendingBrackets == 0 && pendingBoolean == NOTHING_PENDING && stringBuilder.length() > 0)
        {
            pendingBoolean = PENDING_AND;
        }
    }

    public void endAnd()
    {
        if (pendingBoolean == PENDING_AND)
        {
            pendingBoolean = NOTHING_PENDING;
        }
    }

    public void beginOr()
    {
        if (pendingBrackets == 0 && pendingBoolean == NOTHING_PENDING && stringBuilder.length() > 0)
        {
            pendingBoolean = PENDING_OR;
        }
    }

    public void endOr()
    {
        if (pendingBoolean == PENDING_OR)
        {
            pendingBoolean = NOTHING_PENDING;
        }
    }

    public void beginBracket()
    {
        pendingBrackets++;
    }

    public void endBracket()
    {
        if (pendingBrackets > 0)
        {
            pendingBrackets--;
        }
        else
        {
            stringBuilder.append(" )");
        }
    }

    public CharSequence getCurrentAttributePrefix()
    {
        return this.currentPath.get(this.currentPath.size() - 1);
    }

    private void checkPending()
    {
        if (pendingBoolean == PENDING_AND)
        {
            stringBuilder.append(" &");
        }
        else if (pendingBoolean == PENDING_OR)
        {
            stringBuilder.append(" |");
        }
        for(int i=0;i<pendingBrackets;i++)
        {
            stringBuilder.append(" (");
        }
        pendingBoolean = NOTHING_PENDING;
        pendingBrackets = 0;
    }

    public void pushMapper(Mapper mapper)
    {
        StringBuilder sb = new StringBuilder(this.currentPath.get(this.currentPath.size() - 1));
        sb.append('.');
        mapper.appendName(sb);
        this.currentPath.add(sb.toString());
    }

    public Mapper popMapper()
    {
        this.currentPath.remove(this.currentPath.size() - 1);
        return null;
    }

    public static String createAndToString(Operation op)
    {
        ToStringContext toStringContext = new ToStringContext(op.getResultObjectPortal());
        op.zToString(toStringContext);
        return toStringContext.toString();
    }

    @Override
    public String toString()
    {
        return this.stringBuilder.toString();
    }
}
