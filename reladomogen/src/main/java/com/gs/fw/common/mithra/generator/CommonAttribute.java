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

package com.gs.fw.common.mithra.generator;

import java.util.List;


public interface CommonAttribute
{
    public String getName();
    public String getGetter();
    public String getSetter();
    public String getFinderAttributeType();
    public String getFinderAttributeSuperClassType();
    public String getTypeAsString();
    public boolean isFinalGetter();
    public boolean hasParameters();
    public List validateAndUseMissingValuesFromSuperClass(CommonAttribute superClassAttribute);
}
