/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.reladomo.graphql;

import org.junit.Assert;
import org.junit.Test;

public class StringKitTest
{
    @Test
    public void decapitalize() {
        Assert.assertEquals("morning", StringKit.decapitalize("goodMorning", "good".length()));
    }

    @Test
    public void capitalize() {
        Assert.assertEquals("Morning", StringKit.capitalize("morning"));
    }

    @Test
    public void englishPlural() {
        Assert.assertEquals("GreenCompanies", StringKit.englishPluralize("GreenCompany"));
        Assert.assertEquals("apples", StringKit.englishPluralize("apple"));

        // mistakes
        Assert.assertEquals("fruits", StringKit.englishPluralize("fruit"));
        Assert.assertEquals("SuperMens", StringKit.englishPluralize("SuperMen"));
    }
}
