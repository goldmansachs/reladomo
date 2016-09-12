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

import java.util.List;


public class MithraGraphGenerator extends AbstractMithraGenerator
{
    private CoreMithraGraphGenerator coreGraphGenerator;

    public MithraGraphGenerator()
    {
        this(new CoreMithraGraphGenerator());
    }

    public MithraGraphGenerator(CoreMithraGraphGenerator gen)
    {
        super(gen);
        this.coreGraphGenerator = (CoreMithraGraphGenerator) this.baseGenerator;
    }

    public void setIncludeList(String includeList)
    {
        this.coreGraphGenerator.setIncludeList(includeList);
    }

    public void setShowAttributes(String showAttributes)
    {
        this.coreGraphGenerator.setShowAttributes(showAttributes);
    }

    public List getIncludeListArray()
    {
        return this.coreGraphGenerator.getIncludeListArray();
    }

    public void setOutputFile(String outputFile)
    {
        this.coreGraphGenerator.setOutputFile(outputFile);
    }

    public void setFollowRelationshipDepth(int followRelationshipDepth)
    {
        this.coreGraphGenerator.setFollowRelationshipDepth(followRelationshipDepth);
    }

    public void setCollapseRelationships(boolean collapseRelationships)
    {
        this.coreGraphGenerator.setCollapseRelationships(collapseRelationships);
    }

    public void execute() throws BuildException
    {
        this.coreGraphGenerator.execute();
    }

    public static void main(String[] args)
    {
        MithraGraphGenerator gen = new MithraGraphGenerator() {
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
        gen.setXml("H:/projects/glew/xml/mithra/GlewMithraClassList.xml");
        MithraGeneratorImport xmlImport = new MithraGeneratorImport();
        xmlImport.setDir("H:/projects/glew/xml/mithra/");
        xmlImport.setFilename("GlewMithraRefDataClassList.xml");
        gen.addConfiguredMithraImport(xmlImport);
        gen.setOutputFile("c:/temp/glew.graphml");
        gen.setShowAttributes("none");
//        gen.setIncludeList("LewAgreement");
//        gen.setFollowRelationshipDepth(1);
        gen.execute();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
     }
}
