


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

package com.gs.fw.common.mithra.test.domain;

public class Trial extends TrialAbstract
{

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof TrialAbstract))
        {
            return false;
        }

        final TrialAbstract trialAbstract = (TrialAbstract) o;

        if (!getDescription().equals(trialAbstract.getDescription()))
        {
            return false;
        }
        if (!getTrialId().equals(trialAbstract.getTrialId()))
        {
            return false;
        }

        return true;
    }

    public int hashCode()
    {
        int result;
        result = getTrialId().hashCode();
        result = 29 * result + getDescription().hashCode();
        return result;
    }
}
