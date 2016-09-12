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


public class Synonym extends SynonymAbstract
{

    public boolean equals(Object obj)
    {
        if(obj == this)
        {
            return true;
        }

        if(obj instanceof Synonym)
        {
            Synonym s = (Synonym)obj;
            return (this.getSynonym() == null? s.getSynonym() == null:this.getSynonym().equals(s.getSynonym())) &&
            (this.getSynonymEndDate() == null? s.getSynonymEndDate() == null:this.getSynonymEndDate().equals(s.getSynonymEndDate())) &&
            (this.getSynonymTypeCode() == null? s.getSynonymTypeCode() == null:this.getSynonymTypeCode().equals(s.getSynonymTypeCode()))&&
            (this.getProductId() == s.getProductId());
        }
        return false;
    }

    public int hashCode()
    {
        int result = 17;
        long productId = this.getProductId();
        result = result*29 + (this.getSynonym() != null ? this.getSynonym().hashCode() : 0);
        result = result*29 + (this.getSynonymEndDate() != null ? this.getSynonymEndDate().hashCode() : 0);
        result = result*29 + (this.getSynonymTypeCode() != null ? this.getSynonymTypeCode().hashCode() : 0);
        result = result*29 + (int)(productId^(productId>>>32));
        return result;
    }
}
