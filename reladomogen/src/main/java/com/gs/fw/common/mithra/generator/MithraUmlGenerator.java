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


public class MithraUmlGenerator extends MithraGenerator
{
    private CoreMithraUmlGenerator coreUmlGenerator;

    public MithraUmlGenerator()
    {
        this(new CoreMithraUmlGenerator());
    }

    public MithraUmlGenerator(CoreMithraUmlGenerator gen)
    {
        super(gen);
        this.coreUmlGenerator = (CoreMithraUmlGenerator) this.baseGenerator;
    }

    public String getOutputFile()
    {
        return this.coreUmlGenerator.getOutputFile();
    }

    public void setOutputFile(String outputFile)
    {
        this.coreUmlGenerator.setOutputFile(outputFile);
    }

    public void execute() throws BuildException
    {
        this.coreUmlGenerator.execute();
    }
}
