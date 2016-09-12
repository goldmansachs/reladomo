
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

package com.gs.fw.common.mithra.test.generator;


import java.io.File;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.generator.objectxmlgenerator.DatabaseIndexValidator;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.Assert;

public class DatabaseIndexValidatorTest
    extends MithraTestAbstract
{
    private DatabaseIndexValidator newGenerator(String classList)
    {
        DatabaseIndexValidator maxLenGenerator = new DatabaseIndexValidator();
        maxLenGenerator.setUserName("sa");
        maxLenGenerator.setPassword("");
        maxLenGenerator.setDriver("org.h2.Driver");
        maxLenGenerator.setUrl("jdbc:h2:mem:A");
        maxLenGenerator.setDatabaseType("h2");
        maxLenGenerator.setXml(getBaseDir() + classList);
        return maxLenGenerator;
    }

    private String getBaseDir()
    {
        String testRoot = System.getProperty("mithra.xml.root");
        return testRoot + "indexvalidator" + File.separator;
    }

    public void testValidator()
    {
        DatabaseIndexValidator validator = this.newGenerator("ClassList.xml");
        validator.validateIndices();
        Assert.assertTrue(validator.getTablesMissingFromDb().keySet().containsAll(UnifiedSet.newSetWith("BcpSimple")));
        Assert.assertTrue(validator.getMissingKeyViolations().keySet().containsAll(UnifiedSet.newSetWith("OrderWi")));
    }
}