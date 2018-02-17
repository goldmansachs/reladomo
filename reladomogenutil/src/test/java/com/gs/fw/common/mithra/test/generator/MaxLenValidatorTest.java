
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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test.generator;

import com.gs.fw.common.mithra.generator.MithraGeneratorImport;
import com.gs.fw.common.mithra.generator.objectxmlgenerator.MaxLenValidator;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.Assert;
import org.apache.tools.ant.BuildException;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class MaxLenValidatorTest
        extends MithraTestAbstract
{
    private static final String EXPECTED_STATE_ATTR_RESULT =
            "<Attribute name=\"state\" javaType=\"String\" columnName=\"STATE\" maxLength=\"20\" truncate=\"true\"/>";

    private static final String EXPECTED_TRACKING_ID_ATTR_RESULT =
            "<Attribute name=\"trackingId\" javaType=\"String\" columnName=\"TRACKING_ID\" maxLength=\"15\" truncate=\"true\"/>";

    private class MaxLenGeneratorForTest
            extends MaxLenValidator
    {
        final Map<String, String> result = UnifiedMap.newMap();

        @Override
        public String readFile(String filename) throws IOException
        {
            if (result.containsKey(filename))
            {
                return result.get(filename);
            }
            String fileAsString = super.readFile(filename);
            result.put(filename, fileAsString);
            return fileAsString;
        }

        @Override
        public void writeFile(String filename, String text) throws IOException
        {
            result.put(filename, text);
        }

        public Map<String, String> getResult()
        {
            return this.result;
        }
    }


    public void testSimpleCase()
    {
        Result resultContainer = createAndExecuteMaxLenGenerator("ClassList.xml");
        this.validateResults(resultContainer);
    }

    private void validateResults(Result resultContainer)
    {
        Map<String, String> result = resultContainer.getResult();
        Assert.assertEquals(1, result.size());
        String text = result.values().iterator().next();
        Assert.assertFalse(resultContainer.hasException());
        Assert.assertTrue(text.contains(EXPECTED_STATE_ATTR_RESULT));
        Assert.assertTrue(text.contains(EXPECTED_TRACKING_ID_ATTR_RESULT));
    }

    public void testTablePerSubclass()
    {
        Result resultContainer = createAndExecuteMaxLenGenerator("ClassListTablePerSubclass.xml");
        String text = resultContainer.findValue("MissingLenOrderWiSuper");
        Assert.assertFalse(resultContainer.hasException());
        Assert.assertTrue(text.contains(EXPECTED_STATE_ATTR_RESULT));
        Assert.assertFalse(text.contains(EXPECTED_TRACKING_ID_ATTR_RESULT));
        text = resultContainer.findValue("MissingLenOrderWiChild");
        Assert.assertFalse(text.contains(EXPECTED_STATE_ATTR_RESULT));
        Assert.assertTrue(text.contains(EXPECTED_TRACKING_ID_ATTR_RESULT));
    }

    public void testTablePerSubclassConflictingMaxLen()
    {
        Result resultContainer = createAndExecuteMaxLenGenerator("ClassListTablePerSubclassConflictingMaxLen.xml");
        Assert.assertTrue(resultContainer.hasException());
        Assert.assertTrue(resultContainer.findValue("MissingLenOrderWiSuper").contains("<Attribute name=\"description\" javaType=\"String\" columnName=\"DESCRIPTION\"/>"));
    }

    public void testTablePerClass()
    {
        Result resultContainer = createAndExecuteMaxLenGenerator("ClassListTablePerClass.xml");
        Assert.assertFalse(resultContainer.hasException());
        Assert.assertEquals(2, resultContainer.getResult().size());
        String text = resultContainer.findValue("MissingLenReadOnlyAnimal");
        Assert.assertTrue(text.contains("columnName=\"NAME\" maxLength=\"20\" truncate=\"true\""));
        text = resultContainer.findValue("MissingLenReadOnlyCow");
        Assert.assertTrue(text.contains("columnName=\"FARM\" maxLength=\"25\" truncate=\"true\""));
    }

    public void testWithViolations()
    {
        Result resultContainer = this.createAndExecuteMaxLenGenerator("ExceptionsClassList.xml");
        Assert.assertTrue(resultContainer.hasException());
        String missingFieldResult = resultContainer.findValue("MissingField");
        Assert.assertTrue(missingFieldResult.contains(EXPECTED_STATE_ATTR_RESULT));
    }

    private Result createAndExecuteMaxLenGenerator(String classList)
    {
        Result result = new Result();
        MaxLenGeneratorForTest maxLenGenerator = null;
        try
        {
            maxLenGenerator = this.newGenerator(classList);
            maxLenGenerator.execute();
        }
        catch (BuildException e)
        {
            result.setException(e);
        }
        finally
        {
            result.setResult(maxLenGenerator.getResult());
        }
        return result;
    }

    public void testExceptionThrownWithLdapAndUrlSpecified()
    {
        MaxLenGeneratorForTest generatorForTest = newGenerator("ClassListTablePerClass.xml");
        generatorForTest.setLdapName("abc");
        try
        {
            generatorForTest.execute();
            fail("Since both URL and LDAP name are specified exception is expected");
        }
        catch (Exception e)
        {
            //expected
        }
    }

    public void testWithImports()
    {
        MaxLenGeneratorForTest generatorForTest = newGenerator("ClassList.xml");
        MithraGeneratorImport anImport = new MithraGeneratorImport();
        anImport.setDir(getBaseDir());
        anImport.setFilename("ExceptionsClassList.xml");
        generatorForTest.addConfiguredMithraImport(anImport);
        generatorForTest.execute();
        this.validateResults(new Result(generatorForTest.getResult()));
    }

    private MaxLenGeneratorForTest newGenerator(String classList)
    {
        MaxLenGeneratorForTest maxLenGenerator = new MaxLenGeneratorForTest();
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
        return testRoot + "maxlengenerator" + File.separator;
    }


    private static class Result
    {
        private Map<String, String> result;
        private Exception exception;

        private Result()
        {
        }

        private Result(Map<String, String> result)
        {
            this.result = result;
        }

        public Map<String, String> getResult()
        {
            return result;
        }

        public void setResult(Map<String, String> result)
        {
            this.result = result;
        }

        public Exception getException()
        {
            return exception;
        }

        public void setException(Exception exception)
        {
            this.exception = exception;
        }

        public boolean hasException()
        {
            return this.exception != null;
        }

        public String findValue(final String name)
        {
            String key = null;
            for (String each : result.keySet())
            {
                if (each.indexOf(name) >= 0)
                {
                    key = each;
                    break;
                }
            }
            Assert.assertNotNull(key);
            return result.get(key);
        }
    }
}