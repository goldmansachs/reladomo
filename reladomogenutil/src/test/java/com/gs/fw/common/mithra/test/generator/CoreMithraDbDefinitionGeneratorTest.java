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

import com.gs.fw.common.mithra.generator.StdOutLogger;
import com.gs.fw.common.mithra.generator.dbgenerator.CoreMithraDbDefinitionGenerator;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class CoreMithraDbDefinitionGeneratorTest
        extends MithraTestAbstract
{
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        temporaryFolder.create();
    }

    @Override
    protected void tearDown() throws Exception
    {
        temporaryFolder.delete();
        super.tearDown();
    }

    public void testH2DdlGeneration() throws Exception
    {
        CoreMithraDbDefinitionGenerator generator = newGenerator("ClassList.xml");
        generator.execute();

        String expectedDir = getTestRoot() + "h2ddlgen" + File.separator;

        assertGeneratedFileMatchesExpected("ORDERS_WI.ddl", expectedDir);
        assertGeneratedFileMatchesExpected("ORDERS_WI.idx", expectedDir);
        assertGeneratedFileMatchesExpected("BCP_SIMPLE_ABC.ddl", expectedDir);
        assertGeneratedFileMatchesExpected("BCP_SIMPLE_ABC.idx", expectedDir);
        assertGeneratedFileMatchesExpected("ANIMAL_GROUP_TBL.ddl", expectedDir);
        assertGeneratedFileMatchesExpected("ANIMAL_GROUP_TBL.idx", expectedDir);
    }

    private CoreMithraDbDefinitionGenerator newGenerator(String classList)
    {
        CoreMithraDbDefinitionGenerator generator = new CoreMithraDbDefinitionGenerator();
        generator.setLogger(new StdOutLogger());
        generator.setDatabaseType("h2");
        generator.setXml(getTestRoot() + "tablevalidator" + File.separator + classList);
        generator.setGeneratedDir(temporaryFolder.getRoot().getAbsolutePath());
        return generator;
    }

    private String getTestRoot()
    {
        return System.getProperty("mithra.xml.root");
    }

    private void assertGeneratedFileMatchesExpected(String fileName, String expectedDir) throws IOException
    {
        File generatedFile = new File(temporaryFolder.getRoot(), fileName);
        File expectedFile = new File(expectedDir, fileName);

        Assert.assertTrue("Expected generated file to exist: " + fileName, generatedFile.exists());
        Assert.assertTrue("Expected baseline file to exist: " + expectedFile.getAbsolutePath(), expectedFile.exists());

        String expectedContent = new String(Files.readAllBytes(expectedFile.toPath()));
        String actualContent = new String(Files.readAllBytes(generatedFile.toPath()));
        Assert.assertEquals("Mismatch in " + fileName, expectedContent, actualContent);
    }
}
