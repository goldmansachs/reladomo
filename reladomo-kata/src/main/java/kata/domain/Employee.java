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

import java.sql.Timestamp;

/********************************************************************************
 * File        : $Source:  $
 * Version     : $Revision:  $
 * Date        : $Date:  $
 * Modified by : $Author:  $
 * ******************************************************************************
 */
public class Employee extends EmployeeAbstract
{
    public Employee(Timestamp businessDate, Timestamp processingDate)
    {
        super(businessDate, processingDate
        );
        // You must not modify this constructor. Mithra calls this internally.
        // You can call this constructor. You can also add new constructors.
    }

    public Employee(Timestamp businessDate)
    {
        super(businessDate);
    }

    public Employee(
            Timestamp businessDate,
            int employeeId,
            String firstName,
            String lastName,
            int age)
    {
        super(businessDate);
        this.setEmployeeId(employeeId);
        this.setFirstName(firstName);
        this.setLastName(lastName);
        this.setAge(age);
    }
}
