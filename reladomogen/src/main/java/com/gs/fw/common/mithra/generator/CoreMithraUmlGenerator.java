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

import com.gs.fw.common.mithra.generator.filesystem.FauxFile;

import java.io.*;
import java.util.Iterator;

public class CoreMithraUmlGenerator extends CoreMithraGenerator
{
    private String outputFile;

    public String getOutputFile()
    {
        return outputFile;
    }

    public void setOutputFile(String outputFile)
    {
        this.outputFile = outputFile;
    }

    public void execute()
    {
        try
        {
            parseAndValidate();
            OutputStream fos = this.fauxFileSystem.newFile(this.getOutputFile()).newFileOutputStream();
            PrintWriter writer = new PrintWriter(fos);
            for (Iterator iterator = this.getMithraObjects().values().iterator(); iterator.hasNext();)
            {
                MithraObjectTypeWrapper mithraObjectTypeWrapper = (MithraObjectTypeWrapper) iterator.next();

                writer.write("/**\n");

                RelationshipAttribute[] relationshipAttributes = mithraObjectTypeWrapper.getRelationshipAttributes();
                for(int i=0;i<relationshipAttributes.length;i++)
                {
                    writer.write("* @");
                    if (relationshipAttributes[i].isRelatedDependent())
                    {
                        writer.write("composed ");
                    }
                    else
                    {
                        writer.write("assoc ");
                    }
                    if (relationshipAttributes[i].getCardinality().isFromMany())
                    {
                        writer.write("* ");
                    }
                    else
                    {
                        writer.write("1 ");
                    }
                    writer.write(relationshipAttributes[i].getName());
                    if (relationshipAttributes[i].getCardinality().isToMany())
                    {
                        writer.write(" * ");
                    }
                    else
                    {
                        writer.write(" 1 ");
                    }
                    writer.write(relationshipAttributes[i].getRelatedObject().getClassName());
                    writer.write("\n");
                }

                writer.write("*/\n");
                writer.write("public class "+mithraObjectTypeWrapper.getClassName());
                if (mithraObjectTypeWrapper.getSuperClassWrapper() != null)
                {
                    writer.write(" extends ");
                    writer.write(mithraObjectTypeWrapper.getSuperClassWrapper().getClassName());
                }
                writer.write(" {}\n\n");
            }
            writer.close();
            fos.close();
        }
        catch (FileNotFoundException e)
        {
            throw new MithraGeneratorException("could not find file", e);
        }
        catch (IOException e)
        {
            throw new MithraGeneratorException("could not write file", e);
        }
    }
}
