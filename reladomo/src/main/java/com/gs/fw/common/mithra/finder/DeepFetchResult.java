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


import com.gs.fw.common.mithra.util.ListFactory;

import java.util.Collections;
import java.util.List;


public class DeepFetchResult
{

    private int percentComplete;
    private Object localResult;
    private DeepFetchNode node;
    private List immediateParentList;

    private List result;

    private static final DeepFetchResult NOTHING_TO_DO = new DeepFetchResult(ListFactory.EMPTY_LIST, 100);
    private static final DeepFetchResult INCOMPLETE_RESULT = new DeepFetchResult();

    public DeepFetchResult()
    {
    }

    public DeepFetchResult(List immediateParentList)
    {
        this.immediateParentList = immediateParentList;
    }

    public DeepFetchResult(List result, int percentComplete)
    {
        this.result = result;
        this.percentComplete = percentComplete;
    }

    public List getImmediateParentList()
    {
        return immediateParentList;
    }

    public void setLocalResult(Object localResult)
    {
        this.localResult = localResult;
    }

    public Object getLocalResult()
    {
        return localResult;
    }

    public List getResult()
    {
        return result;
    }

    public void setResult(List result)
    {
        this.result = result;
    }

    public int getPercentComplete()
    {
        return percentComplete;
    }

    public void setPercentComplete(int percentComplete)
    {
        this.percentComplete = percentComplete;
    }

    public DeepFetchNode getNode()
    {
        return node;
    }

    public void setNode(DeepFetchNode node)
    {
        this.node = node;
    }

    public static DeepFetchResult nothingToDo()
    {
        return NOTHING_TO_DO;
    }

    public static DeepFetchResult incompleteResult()
    {
        return INCOMPLETE_RESULT;
    }

    public boolean hasNothingToDo()
    {
        return this.result == ListFactory.EMPTY_LIST && this.percentComplete == 100;
    }
}
