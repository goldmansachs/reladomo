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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.List;
import java.util.Set;


public class WhereClause
{
    // why is StringBuilder final? This class would extend StringBuilder instead of being a bunch of static junk

    private static final char BRACKET_REPLACEMENT_CHAR = '\r';
    private static final char AND_REPLACEMENT_CHAR = '\n';
    private static final char OR_REPLACEMENT_CHAR = '\t';
    private static final String AND_REPLACEMENT_STRING = "\n\n\n\n";
    private static final String OR_REPLACEMENT_STRING = "\t\t\t\t";

    private StringBuilder whereClause = new StringBuilder(16);
    private List<SqlParameterSetter> sqlParameterSetters = null;
    private WhereClauseOwner owner;
    private int orCount;
    private List<TempTableJoin> tempTableJoins;
    private Set<String> reachableColumns;
    private BooleanStack booleanStack; // true means boolean operation was added to the where clause

    public WhereClause(WhereClauseOwner owner)
    {
        this.owner = owner;
    }

    public int setSqlParameters(SqlQuery query, PreparedStatement ps, int count, WhereClauseOwner source) throws SQLException
    {
        if (source == this.owner && sqlParameterSetters != null)
        {
            for (int i = 0; i < sqlParameterSetters.size(); i++)
            {
                SqlParameterSetter sps = sqlParameterSetters.get(i);
                count += sps.setSqlParameters(ps, count, query);
            }
        }
        return count;
    }

    public boolean isInOrClause()
    {
        return orCount > 0;
    }

    public void clear()
    {
        this.whereClause.setLength(0);
        if (this.sqlParameterSetters != null)
        {
            this.sqlParameterSetters.clear();
        }
        if (tempTableJoins != null)
        {
            tempTableJoins.clear();
        }
        if (this.reachableColumns != null)
        {
            this.reachableColumns.clear();
        }
    }

    public int length()
    {
        return whereClause.length();
    }

    public WhereClause insert(int offset, String str)
    {
        checkOffsetToFill(offset);
        if (offset == whereClause.length())
        {
            whereClause.append(str);
        }
        else
        {
            whereClause.insert(offset, str);
        }
        return this;
    }

    private void checkOffsetToFill(int offset)
    {
        int start = offset - 1;
        if (start < 0) return;
        for(int i=start; i >= 0; i--)
        {
            switch (this.whereClause.charAt(i))
            {
                case BRACKET_REPLACEMENT_CHAR:
                    whereClause.setCharAt(i   , '(');
                    break;
                case AND_REPLACEMENT_CHAR:
                    whereClause.setCharAt(i-3 , ' ');
                    whereClause.setCharAt(i-2 , 'a');
                    whereClause.setCharAt(i-1 , 'n');
                    whereClause.setCharAt(i   , 'd');
                    return;
                case OR_REPLACEMENT_CHAR:
                    whereClause.setCharAt(i-3 , ' ');
                    whereClause.setCharAt(i-2 , 'o');
                    whereClause.setCharAt(i-1 , 'r');
                    whereClause.setCharAt(i   , ' ');
                    return;
                default:
                    return;
            }
        }
    }

    public WhereClause insert(int offset, CharSequence s)
    {
        checkOffsetToFill(offset);
        if (offset == whereClause.length())
        {
            whereClause.append(s);
        }
        else
        {
            whereClause.insert(offset, s);
        }
        return this;
    }

    public WhereClause insert(int offset, char c)
    {
        checkOffsetToFill(offset);
        if (offset == whereClause.length())
        {
            whereClause.append(c);
        }
        else
        {
            whereClause.insert(offset, c);
        }
        return this;
    }

    public WhereClause append(String str)
    {
        checkOffsetToFill(whereClause.length());
        whereClause.append(str);
        return this;
    }

    public WhereClause append(CharSequence s)
    {
        checkOffsetToFill(whereClause.length());
        whereClause.append(s);
        return this;
    }

    public WhereClause append(char c)
    {
        checkOffsetToFill(whereClause.length());
        whereClause.append(c);
        return this;
    }

    public WhereClause append(int i)
    {
        checkOffsetToFill(whereClause.length());
        whereClause.append(i);
        return this;
    }

    private boolean isBlank(int start, int end)
    {
        for(int i=start;i<end;i++)
        {
            if (!isBlank(whereClause.charAt(i))) return false;
        }
        return true;
    }

    private boolean isBlank(char c)
    {
        return c == ' ' || c == BRACKET_REPLACEMENT_CHAR || c == AND_REPLACEMENT_CHAR || c == OR_REPLACEMENT_CHAR;
    }

    public void beginBracket()
    {
        whereClause.append(BRACKET_REPLACEMENT_CHAR);
    }

    public boolean endBracket()
    {
        return endBracket(whereClause.length());
    }

    public boolean endBracket(int positionToLookFrom)
    {
        int firstPosition = positionToLookFrom - 1;
        if (whereClause.charAt(firstPosition) == BRACKET_REPLACEMENT_CHAR)
        {
            remove(firstPosition, 1);
            return false;
        }
        insert(positionToLookFrom, ')');
        return true;
    }

    private void remove(int position, int toRemove)
    {
        if (position == whereClause.length() - 1)
        {
            whereClause.setLength(whereClause.length() - toRemove);
        }
        else
        {
            whereClause.replace(position - toRemove, position, "");
        }
    }

    public void beginAnd()
    {
        pushBooleanOp(beginAnd(0, whereClause != null ? whereClause.length() : 0));
    }

    protected boolean beginAnd(int start, int end)
    {
        if (isBlank(start, end)) return false;
        if (hasBracketAndOrInPosition(end)) return false;
        whereClause.append(AND_REPLACEMENT_STRING);
        return true;
    }

    private void pushBooleanOp(boolean val)
    {
        if (this.booleanStack == null)
        {
            this.booleanStack = new BooleanStack();
        }
        this.booleanStack.push(val);
    }

    private boolean popBooleanStack()
    {
        return this.booleanStack.pop();
    }

    private boolean hasBracketAndOrInPosition(int end)
    {
        char lastChar = whereClause.charAt(end - 1);
        return lastChar == AND_REPLACEMENT_CHAR || lastChar == BRACKET_REPLACEMENT_CHAR || lastChar == OR_REPLACEMENT_CHAR;
    }

    public void endAnd()
    {
        boolean inserted = popBooleanStack();
        endAnd(inserted, whereClause != null ? whereClause.length() : 0);
    }

    public boolean endAnd(boolean insertedReplacement, int positionToLookFrom)
    {
        if (!insertedReplacement) return false;
        int firstPosition = positionToLookFrom - 1;
        if (whereClause.charAt(firstPosition) == AND_REPLACEMENT_CHAR)
        {
            remove(firstPosition, AND_REPLACEMENT_STRING.length());
            return false;
        }
        return true;
    }

    public void beginOr()
    {
        orCount++;
        pushBooleanOp(beginOr(0, whereClause != null ? whereClause.length() : 0));
    }

    protected boolean beginOr(int start, int end)
    {
        if (isBlank(start, end)) return false;
        if (hasBracketAndOrInPosition(end)) return false;
        whereClause.append(OR_REPLACEMENT_STRING);
        return true;
    }

    public void endOr()
    {
        boolean inserted = popBooleanStack();
        orCount--;
        endOr(inserted, whereClause != null ? whereClause.length() : 0);
    }

    protected boolean endOr(boolean insertedReplacement, int positionToLookFrom)
    {
        if (!insertedReplacement) return false;
        int firstPosition = positionToLookFrom - 1;
        if (whereClause.charAt(firstPosition) == OR_REPLACEMENT_CHAR)
        {
            remove(firstPosition, OR_REPLACEMENT_STRING.length());
            return false;
        }
        return true;
    }

    public String toString()
    {
        return this.whereClause.toString();
    }

    public WhereClause appendWithSpace(CharSequence sql)
    {
        return this.append(' ').append(sql);
    }

    public void addSqlParameterSetter(SqlParameterSetter sqlParameterSetter)
    {
        if (this.sqlParameterSetters == null)
        {
            this.sqlParameterSetters = FastList.newList();
        }
        this.sqlParameterSetters.add(sqlParameterSetter);
    }

    public WhereClauseOwner getOwner()
    {
        return owner;
    }

    public void addTempTableJoin(TempTableJoin tempTableJoin)
    {
        if (this.tempTableJoins == null) this.tempTableJoins = FastList.newList();
        tempTableJoins.add(tempTableJoin);
    }

    public void appendTempTableFromClause(Object owner, StringBuilder fromClause)
    {
        if (owner == this.getOwner() && this.tempTableJoins != null)
        {
            for(int i=0;i<tempTableJoins.size();i++)
            {
                TempTableJoin tempTableJoin = tempTableJoins.get(i);
                fromClause.append(' ').append(tempTableJoin.isInnerJoin() ? " inner ": " left ").append("join ");
                fromClause.append(tempTableJoin.getTempTableName());
                fromClause.append(" on ").append(tempTableJoin.getOnClause());
            }
        }
    }

    public int replaceTextWithAnd(int start, int length, String newText)
    {
        if (newText.length() == 0)
        {
            return removeTextWithAnd(start, length);
        }
        this.whereClause.replace(start, start + length, newText);
        return newText.length() - length; 
    }

    private int removeTextWithAnd(int start, int length)
    {
        //3 possibilities
        // 1) the only text:
        if (length == this.whereClause.length()-1) // subtract 1 for leading space
        {
            this.whereClause.setLength(0);
            return -(length + 1);
        }
        // 2) and before this text
        int andLength = 4;
        if (start > 1 && this.whereClause.subSequence(start-4, start).equals("and "))
        {
            start -= 4;
            if (this.whereClause.charAt(start - 1) == ' ')
            {
                start--;
                andLength++;
            }
        }
        else
        {
            // 3) and after this text
            if (this.whereClause.charAt(start +length + 4) == ' ')
            {
                andLength++;
            }
        }
        this.whereClause.delete(start, start + length + andLength);
        return -(length + andLength);
    }

    public void addReachableColumn(String fullyQualifiedColumn)
    {
        if (this.reachableColumns == null)
        {
            this.reachableColumns = UnifiedSet.newSet(4);
        }
        this.reachableColumns.add(fullyQualifiedColumn);
    }

    public boolean isColumnReachable(String fullyQualifiedColumnName)
    {
        return this.reachableColumns != null && this.reachableColumns.contains(fullyQualifiedColumnName);
    }

    private static class BooleanStack
    {
        private int size;
        private BitSet set = new BitSet();

        public void push(boolean val)
        {
            if (val)
            {
                set.set(size);
            }
            else
            {
                set.clear(size);
            }
            size++;
        }

        public boolean pop()
        {
            size--;
            return set.get(size);
        }

        public boolean peek()
        {
            return set.get(size - 1);
        }
    }

    public interface WhereClauseOwner
    {
        public WhereClause getWhereClause();

        public WhereClause getParentWhereClause(SqlQuery sqlQuery);
    }
}
