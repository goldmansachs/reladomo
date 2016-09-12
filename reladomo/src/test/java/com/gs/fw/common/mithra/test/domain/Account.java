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

public class Account extends AccountAbstract
{

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AccountAbstract that = (AccountAbstract) o;

        if (this.getAccountNumber() != null ? !this.getAccountNumber().equals(that.getAccountNumber()) : that.getAccountNumber() != null) return false;
        if (this.getCode() != null ? !this.getCode().equals(that.getCode()) : that.getCode() != null) return false;
        if (this.getPnlGroupId() != null ? !this.getPnlGroupId().equals(that.getPnlGroupId()) : that.getPnlGroupId() != null) return false;
        if (this.getTrialId() != null ? !this.getTrialId().equals(that.getTrialId()) : that.getTrialId() != null) return false;

        return true;
    }

    public int hashCode()
    {
        int result;
        result = (this.getAccountNumber() != null ? this.getAccountNumber().hashCode() : 0);
        result = 29 * result + (this.getCode() != null ? this.getCode().hashCode() : 0);
        result = 29 * result + (this.getPnlGroupId() != null ? this.getPnlGroupId().hashCode() : 0);
        result = 29 * result + (this.getTrialId() != null ? this.getTrialId().hashCode() : 0);
        return result;
    }
}
