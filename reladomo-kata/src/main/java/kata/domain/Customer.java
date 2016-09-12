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

package kata.domain;

public class Customer
        extends CustomerAbstract
{
	public Customer()
	{
		super();
		// You must not modify this constructor. Mithra calls this internally.
		// You can call this constructor. You can also add new constructors.
	}

    // Added as a convenience, particularly for ExercisesCrud questions 4, 7, & 8
	public Customer(final String name, final String country)
	{
		this();
		this.setName(name);
        this.setCountry(country);
	}

    @Override
    public String toString()
    {
        return "Customer[id=" + this.getCustomerId() + "; name=" + this.getName() + "; country=" + this.getCountry() + ']';
    }
}
