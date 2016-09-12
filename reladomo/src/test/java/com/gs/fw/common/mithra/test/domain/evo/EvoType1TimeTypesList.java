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

package com.gs.fw.common.mithra.test.domain.evo;
import com.gs.fw.finder.Operation;
import java.util.*;

public class EvoType1TimeTypesList extends EvoType1TimeTypesListAbstract
{
	public EvoType1TimeTypesList()
	{
		super();
	}

	public EvoType1TimeTypesList(int initialSize)
	{
		super(initialSize);
	}

	public EvoType1TimeTypesList(Collection c)
	{
		super(c);
	}

	public EvoType1TimeTypesList(Operation operation)
	{
		super(operation);
	}
}
