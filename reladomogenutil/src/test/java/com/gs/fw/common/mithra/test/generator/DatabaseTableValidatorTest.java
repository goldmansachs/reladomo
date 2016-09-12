
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
import com.gs.fw.common.mithra.generator.objectxmlgenerator.DatabaseTableValidator;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.Assert;

public class DatabaseTableValidatorTest
    extends MithraTestAbstract
{
    private DatabaseTableValidator newGenerator(String classList)
    {
        DatabaseTableValidator databaseTableValidator = new DatabaseTableValidator();
        databaseTableValidator.setUserName("sa");
        databaseTableValidator.setPassword("");
        databaseTableValidator.setDriver("org.h2.Driver");
        databaseTableValidator.setUrl("jdbc:h2:mem:A");
        databaseTableValidator.setDatabaseType("h2");
        databaseTableValidator.setXml(getBaseDir() + classList);
        return databaseTableValidator;
    }

    private String getBaseDir()
    {
        String testRoot = System.getProperty("mithra.xml.root");
        return testRoot + "tablevalidator" + File.separator;
    }

    public void testValidator()
    {
        DatabaseTableValidator validator = this.newGenerator("ClassList.xml");
        validator.validateTables();
        Assert.assertTrue(validator.getTablesMissingFromDb().keySet().containsAll(UnifiedSet.newSetWith("BcpSimple")));
        Assert.assertTrue(validator.getMissingColumnsViolations().keySet().containsAll(UnifiedSet.newSetWith("OrderWi-FROM_Z")));
        Assert.assertTrue(validator.getColumnTypeViolations().keySet().containsAll(UnifiedSet.newSetWith("OrderWi-USER_ID")));
        Assert.assertTrue(validator.getColumnSizeViolations().keySet().contains("OrderWi-STATE"));
        String missingMaxLength = validator.getColumnSizeViolations().get("OrderWi-TRACKING_ID");
        Assert.assertEquals("MaxLength is missing for object OrderWi column TRACKING_ID db column size: 15", missingMaxLength);
    }
}