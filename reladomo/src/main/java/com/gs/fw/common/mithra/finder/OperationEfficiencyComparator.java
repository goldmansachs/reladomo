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

import java.util.Comparator;



public class OperationEfficiencyComparator implements Comparator
{

    public int compare(Object o1, Object o2)
    {
        Operation left = (Operation) o1;
        Operation right = (Operation) o2;

        int leftWeight = 100;
        if(left.zHasAsOfOperation()) leftWeight -= 50;
        if (left.usesUniqueIndex()) leftWeight -= 20;
        else if (left.usesNonUniqueIndex()) leftWeight -= 10;
        int rightWeight = 100;
        if(right.zHasAsOfOperation()) rightWeight -= 50;
        if (right.usesUniqueIndex()) rightWeight -= 20;
        else if (right.usesNonUniqueIndex()) rightWeight -= 10;
        int result = leftWeight - rightWeight;
        if (result == 0)
        {
            if (left.zIsEstimatable() && right.zIsEstimatable())
            {
                result = left.zEstimateMaxReturnSize() - right.zEstimateMaxReturnSize();
                if (result == 0)
                {
                    result = left.zEstimateReturnSize() - right.zEstimateReturnSize();
                }
            }
            if (result == 0)
            {
                result = computeDifficultyDiff(left, right);
            }
        }
        return result;
    }

    private int computeDifficultyDiff(Operation left, Operation right)
    {
        int leftDifficulty = computeDifficulty(left);
        int rightDificulty = computeDifficulty(right);
        int result = leftDifficulty - rightDificulty;
        if (result == 0)
        {
            // ensure unique ordering always
            int hash1 = left.hashCode();
            int hash2 = right.hashCode();
            result = hash1<hash2 ? -1 : (hash1==hash2 ? 0 : 1);
        }
        return result;
    }

    //todo: add zGetDifficulty method to the operation itself.
    private int computeDifficulty(Operation left)
    {
        int difficulty = 0;
        if (left instanceof MappedOperation)
        {
            difficulty = 10*getMappingDepth(left);
        }
        return difficulty;
    }

    private int getMappingDepth(Operation op)
    {
        int result = 0;
        do
        {
            MappedOperation mappedOperation = (MappedOperation) op;
            result++;
            if (!mappedOperation.getMapper().isSingleLevelJoin())
            {
                result++;
            }
            op = mappedOperation.getUnderlyingOperation();
        } while(op instanceof MappedOperation);

        return result;
    }
}
