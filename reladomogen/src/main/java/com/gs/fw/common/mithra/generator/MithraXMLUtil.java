
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

import com.gs.fw.common.mithra.generator.metamodel.MithraBaseObjectType;
import com.gs.fw.common.mithra.generator.metamodel.MithraGeneratorParserException;
import com.gs.fw.common.mithra.generator.metamodel.MithraGeneratorUnmarshaller;
import com.gs.fw.common.mithra.generator.metamodel.MithraObjectType;

import java.io.IOException;
import java.io.InputStream;


public class MithraXMLUtil
{


	private static MithraBaseObjectType getMithraObject(String name)
	{
        MithraBaseObjectType result = null;
		String resourceName = name + ".xml";
        MithraGeneratorUnmarshaller unmarshaller = new MithraGeneratorUnmarshaller();
		try
		{
			InputStream is = MithraXMLUtil.class.getClassLoader().getResourceAsStream(resourceName);
			if(is == null)
			{
				throw new RuntimeException("unable to find " + resourceName + " in classpath");
			}
			result = (MithraBaseObjectType)unmarshaller.parse(is, "for " + resourceName);
		}
        catch(IOException e)
        {
            throw new MithraGeneratorException("unable to parse " + resourceName, e);
        }
		catch (MithraGeneratorParserException e)
		{
			throw new MithraGeneratorException("unable to parse " + resourceName, e);
		}

		return result;

	}

	public static void main(String[] args) throws Exception
	{
	}

}
