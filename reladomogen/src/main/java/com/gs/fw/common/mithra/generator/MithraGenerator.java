
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

import org.apache.tools.ant.BuildException;

import java.io.File;
import java.io.FileNotFoundException;

public class MithraGenerator extends AbstractMithraGenerator
{
    private CoreMithraGenerator coreGenerator;

    public MithraGenerator()
    {
        this(new CoreMithraGenerator());
    }

    public MithraGenerator(CoreMithraGenerator gen)
    {
        super(gen);
        this.coreGenerator = gen;
    }

    public boolean isGenerateImported()
    {
        return this.coreGenerator.isGenerateImported();
    }

    public void setGenerateImported(boolean generateImported)
    {
        this.coreGenerator.setGenerateImported(generateImported);
    }

    public void setGenerateConcreteClasses(boolean generateConcreteClasses)
    {
        this.coreGenerator.setGenerateConcreteClasses(generateConcreteClasses);
    }

    public void setWarnAboutConcreteClasses(boolean warnAboutConreteClasses)
    {
        this.coreGenerator.setWarnAboutConcreteClasses(warnAboutConreteClasses);
    }

    public void setGenerateEcListMethod(boolean generateEcListMethod)
    {
        this.coreGenerator.setGenerateEcListMethod(generateEcListMethod);
    }

    public void setGenerateGscListMethod(boolean generateGscListMethod)
    {
        this.coreGenerator.setGenerateGscListMethod(generateGscListMethod);
    }

    @Deprecated
    public void setGenerateLegacyCaramel(boolean generateLegacyCaramel)
    {
        this.coreGenerator.setGenerateLegacyCaramel(generateLegacyCaramel);
    }

    public void setCodeFormat(String format)
    {
        this.coreGenerator.setCodeFormat(format);
    }

    public void execute() throws BuildException
	{
        this.coreGenerator.execute();
	}

    public void setGenerateFileHeaders(boolean generateFileHeaders)
    {
        this.coreGenerator.setGenerateFileHeaders(generateFileHeaders);
    }

    public File parseAndValidate()
            throws FileNotFoundException
    {
        return this.coreGenerator.parseAndValidate();
    }

    public static void main(String[] args)
    {
        MithraGenerator gen = new MithraGenerator() {
            public void log(String s)
            {
                System.out.println(s);
            }

            public void log(String s, int i)
            {
                System.out.println(s);
            }
        };
        long startTime = System.currentTimeMillis();
        gen.setGeneratedDir("H:/temp/Mithra/src");
        gen.setXml("H:/projects/Mithra/Mithra/xml/mithra/test/MithraClassList.xml");
        gen.setNonGeneratedDir("H:/temp/Mithra/src");
        gen.setGenerateGscListMethod(true);
        gen.setCodeFormat(CoreMithraGenerator.FORMAT_FAST);

        MithraGeneratorImport generatorImport = new MithraGeneratorImport();
        generatorImport.setDir("H:/projects/Mithra/Mithra/xml/mithra/test/");
        generatorImport.setFilename("MithraClassListToImport.xml");
        gen.addConfiguredMithraImport(generatorImport);

        generatorImport = new MithraGeneratorImport();
        generatorImport.setDir("H:/projects/Mithra/Mithra/xml/mithra/test/testmithraimport");
        generatorImport.setFilename("MithraTestImportClassList.xml");
        gen.addConfiguredMithraImport(generatorImport);

        gen.execute();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
    }
}