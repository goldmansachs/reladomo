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

package com.gs.fw.common.mithra.test.domain.bcp;

public class TrailingSpaces extends TrailingSpacesAbstract
{
	public TrailingSpaces()
	{
		super();
		// You must not modify this constructor. Mithra calls this internally.
		// You can call this constructor. You can also add new constructors.
	}

	/**
	 * Convenience constructor to set all test strings to the same value.
	 */
	public TrailingSpaces(int id, String description, String testString) {
		super();
		this.setId(id);
		this.setDescription(description);
		this.setCharNotNull(testString);
		this.setCharNullable(testString);
		this.setVarCharNotNull(testString);
		this.setVarCharNullable(testString);
	}
}
