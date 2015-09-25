//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

import com.microsoft.azure.storage.table.TableServiceEntity;

/**
 * Define a customer entity for demonstrating the Table Service.
 * An entity's partition and row key uniquely identify the entity in the table.
 * Entities with the same partition key can be queried faster than those with different partition keys.
 *
 * NOTE: For the purposes of the sample we use the customer's first name as the row key and last name
 *       as the partition key. In reality this would not be a good PK and RK combination as it would
 *       likely not be guaranteed to be unique which is one of the requirements for an entity. *
 */
public class CustomerEntity extends TableServiceEntity {

    public CustomerEntity(String lastName, String firstName) {
        this.partitionKey = lastName;
        this.rowKey = firstName;
    }

    public CustomerEntity() { }

    public String email;
    public String phoneNumber;

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhoneNumber() {
        return this.phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
}
