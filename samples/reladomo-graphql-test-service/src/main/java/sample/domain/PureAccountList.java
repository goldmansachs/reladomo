/*
 Copyright 2019 Goldman Sachs.
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
package sample.domain;
import com.gs.fw.finder.Operation;
import java.util.*;
public class PureAccountList extends PureAccountListAbstract
{
	public PureAccountList()
	{
		super();
	}

	public PureAccountList(int initialSize)
	{
		super(initialSize);
	}

	public PureAccountList(Collection c)
	{
		super(c);
	}

	public PureAccountList(Operation operation)
	{
		super(operation);
	}
}
