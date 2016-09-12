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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;


public class AbstractTxOperations
{
    private static final int MAX_COMBINE_LOOKAHEAD = 10;
    protected final InternalList operations = new InternalList();

    protected void combineForward()
    {
        boolean cleanUp = false;
        DoNothingTransactionOperation doNothing = DoNothingTransactionOperation.getInstance();
        for (int i = 0; i < this.operations.size() - 2;i++)
        {
            TransactionOperation op = (TransactionOperation) this.operations.get(i);
            if (op == doNothing) continue;
            TransactionOperation combined = null;
            TransactionOperation next = (TransactionOperation) this.operations.get(i+1);
            int passThroughDirection = op.getPassThroughDirection(next);
            if ((passThroughDirection & TransactionOperation.COMBINE_DIRECTION_FORWARD) != 0)
            {
                int currentPos = i;
                for (int j = 0; j < MAX_COMBINE_LOOKAHEAD && j + currentPos + 2 < operations.size();j++)
                {
                    TransactionOperation combineTarget = (TransactionOperation) operations.get(j + currentPos + 2);
                    combined = op.combine(combineTarget);
                    if (combined != null)
                    {
                        op = combined;
                        this.operations.set(j + currentPos + 2, combined);
                        this.operations.set(currentPos, doNothing);
                        cleanUp = true;
                        if (currentPos > 0)
                        {
                            Object prev = this.operations.get(currentPos - 1);
                            if (prev != doNothing)
                            {
                                next = (TransactionOperation) this.operations.get(currentPos + 1);
                                combined = ((TransactionOperation) prev).combine(next);
                                if (combined != null)
                                {
                                    this.operations.set(currentPos-1, doNothing);
                                    this.operations.set(currentPos+1, combined);
                                }
                            }
                        }
                        currentPos += j + 2;
                        j = -1;

                        if (currentPos + 1 < operations.size())
                        {
                            combineTarget = (TransactionOperation) operations.get(currentPos + 1);
                        }
                    }
                    if ((op.getPassThroughDirection(combineTarget) & TransactionOperation.COMBINE_DIRECTION_FORWARD) == 0)
                    {
                        break;
                    }
                }
            }
        }
        if (cleanUp) cleanUpDoNothing(doNothing);
    }

    protected void combineBackward()
    {
        boolean cleanUp = false;
        DoNothingTransactionOperation doNothing = DoNothingTransactionOperation.getInstance();
        for (int i = this.operations.size() - 1; i > 0;i--)
        {
            TransactionOperation op = (TransactionOperation) this.operations.get(i);
            if (op == doNothing) continue;
            TransactionOperation prev = (TransactionOperation) this.operations.get(i-1);
            int passThroughDirection = op.getPassThroughDirection(prev);
            int currentPos = i;
            if ((passThroughDirection & TransactionOperation.COMBINE_DIRECTION_BACKWARD) != 0)
            {
                for (int j = 0; j < MAX_COMBINE_LOOKAHEAD && currentPos - j - 2 >= 0;j++)
                {
                    TransactionOperation combineTarget = (TransactionOperation) operations.get(currentPos - j - 2);
                    TransactionOperation combined = combineTarget.combine(op);
                    if (combined != null)
                    {
                        op = combined;
                        this.operations.set(currentPos - j - 2, combined);
                        this.operations.set(currentPos, doNothing);
                        cleanUp = true;
                        if (currentPos+1 < this.operations.size())
                        {
                            Object next = this.operations.get(currentPos + 1);
                            if (next != doNothing)
                            {
                                prev = (TransactionOperation) this.operations.get(currentPos-1);
                                combined = prev.combine((TransactionOperation) next);
                                if (combined != null)
                                {
                                    this.operations.set(currentPos+1, doNothing);
                                    this.operations.set(currentPos-1, combined);
                                }
                            }
                        }
                        currentPos -= j + 2;
                        j = -1;
                        if (currentPos - 1 >= 0)
                        {
                            combineTarget = (TransactionOperation) operations.get(currentPos - 1);
                        }
                    }
                    if ((op.getPassThroughDirection(combineTarget) & TransactionOperation.COMBINE_DIRECTION_BACKWARD) == 0)
                    {
                        break;
                    }
                }
            }
        }
        if (cleanUp) cleanUpDoNothing(doNothing);
    }

    private void cleanUpDoNothing(DoNothingTransactionOperation doNothing)
    {
        int cleanUpCount = 0;
        int currentInsert = 0;

        for(int i=0;i<this.operations.size();i++)
        {
            Object cur = this.operations.get(i);
            if (cur != doNothing)
            {
                this.operations.set(currentInsert, cur);
                currentInsert++;
            }
            else cleanUpCount++;
        }

        for(int i=0;i<cleanUpCount;i++)
        {
            this.operations.remove(this.operations.size() - 1);
        }
    }

    protected boolean combineUpdates()
    {
        boolean cleanUp = false;
        DoNothingTransactionOperation doNothing = DoNothingTransactionOperation.getInstance();
        for (int i = 0; i < operations.size() - 1;i++)
        {
            TransactionOperation op = (TransactionOperation) this.operations.get(i);
            TransactionOperation next = (TransactionOperation) this.operations.get(i + 1);
            TransactionOperation combined = op.combineUpdate(next);
            if (combined != null)
            {
                this.operations.set(i, doNothing);
                this.operations.set(i + 1, combined);
                cleanUp = true;
            }
        }
        if (cleanUp)
        {
            cleanUpDoNothing(doNothing);
        }
        return cleanUp;
    }

    protected void pairCombine()
    {
        boolean cleanUp = false;
        DoNothingTransactionOperation doNothing = DoNothingTransactionOperation.getInstance();
        for (int i = 0; i < operations.size() - 1;i++)
        {
            TransactionOperation op = (TransactionOperation) this.operations.get(i);
            TransactionOperation next = (TransactionOperation) this.operations.get(i + 1);
            TransactionOperation combined = op.combine(next);
            if (combined != null)
            {
                this.operations.set(i, doNothing);
                this.operations.set(i + 1, combined);
                cleanUp = true;
            }
        }
        if (cleanUp) cleanUpDoNothing(doNothing);
    }

    protected void combineAll()
    {
        if (combineUpdates())
        {
            pairCombine();
        }
        int count = 0;
        boolean done = false;
        while(this.operations.size() > 1 && !done && count < 4)
        {
            count++;
            int size = this.operations.size();
            combineForward();
            combineBackward();
            done = size == this.operations.size();
        }
    }

    protected boolean addUpdateWithConsolidation(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper, MithraObjectPortal ownerPortal)
    {
        boolean consolidated = false;
        for (int i = operations.size() - 1; i >= 0 && !consolidated; i--)
        {
            TransactionOperation op = (TransactionOperation) this.operations.get(i);
            if (op.getMithraObject() == obj && op.getPortal() == ownerPortal)
            {
                if (op instanceof UpdateOperation)
                {
                    ((UpdateOperation) op).addOperation(attributeUpdateWrapper);
                    consolidated = true;
                }
                else if (op instanceof InsertOperation)
                {
                    // it is possible that op is instanceof InsertOperation, in which case, the update will simply be written as part of the insert
                    consolidated = true;
                }
            }
            else
            {
                break; // don't combine interlaced operations
            }
        }

        if (!consolidated)
        {
            this.operations.add(new UpdateOperation(obj, attributeUpdateWrapper));
        }
        return !consolidated;
    }
}
