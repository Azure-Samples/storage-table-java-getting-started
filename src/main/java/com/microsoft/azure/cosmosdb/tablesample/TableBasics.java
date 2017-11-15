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
package com.microsoft.azure.cosmosdb.tablesample;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

/**
 * This sample illustrates basic usage of the Azure table storage service.
 */
public class TableBasics {

    protected static CloudTableClient tableClient = null;
    protected static CloudTable table1 = null;
    protected static CloudTable table2 = null;
    protected final static String tableNamePrefix = "tablebasics";

    /**
     * Azure Storage Table Sample
     *
     * @throws Exception
     */
    public void runSamples() throws Exception {

        System.out.println("Azure Storage Table sample - Starting.");

        try {
            // Create a table client for interacting with the table service
            tableClient = TableClientProvider.getTableClientReference();

            // Create a new table with a randomized name
            String tableName1 = tableNamePrefix + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("\nCreate a table with name \"%s\"", tableName1));
            table1 = createTable(tableClient, tableName1);
            System.out.println("\tSuccessfully created the table.");

            // Create a sample entities for use
            CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
            customer1.setEmail("walter@contoso.com");
            customer1.setHomePhoneNumber("425-555-0101");

            // Create and insert new customer entities
            System.out.println("\nInsert the new entities.");
            table1.execute(TableOperation.insert(customer1));
            System.out.println("\tSuccessfully inserted the new entities.");

            // Demonstrate how to read the entity using a point query
            System.out.println("\nRead the inserted entitities using point queries.");
            customer1 = table1.execute(TableOperation.retrieve("Harp", "Walter", CustomerEntity.class)).getResultAsType();
            if (customer1 != null) {
                System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s\t%s", customer1.getPartitionKey(), customer1.getRowKey(), customer1.getEmail(), customer1.getHomePhoneNumber(), customer1.getWorkPhoneNumber()));
            }

            // Demonstrate how to update and merge the entity
            System.out.println("\nUpdate an existing entity by adding the work phone number and merging with the existing entity.");
            CustomerEntity mergeCustomer = new CustomerEntity(customer1.getPartitionKey(), customer1.getRowKey());
            mergeCustomer.setEtag(customer1.getEtag());
            mergeCustomer.setWorkPhoneNumber("425-555-0105");
            // Note the new entity does not have the home phone number or the email set, but the merged entity should retain the old one
            table1.execute(TableOperation.merge(mergeCustomer));
            System.out.println("\tSuccessfully updated the existing entity.");

            // Display the updated entity
            System.out.println("\nRead the updated entities.");
            customer1 = table1.execute(TableOperation.retrieve("Harp", "Walter", CustomerEntity.class)).getResultAsType();
            if (customer1 != null) {
                System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s\t%s", customer1.getPartitionKey(), customer1.getRowKey(), customer1.getEmail(), customer1.getHomePhoneNumber(), customer1.getWorkPhoneNumber()));
            }

            // Demonstrate how to replace the entity
            System.out.println("\nUpdate an existing entity by updating the work phone number and replacing the existing entity.");
            CustomerEntity replaceCustomer = new CustomerEntity(customer1.getPartitionKey(), customer1.getRowKey());
            replaceCustomer.setEmail(customer1.getEmail());
            replaceCustomer.setEtag(customer1.getEtag());
            replaceCustomer.setWorkPhoneNumber("425-555-0106");
            // Note the new entity does not have the home phone number set, so the replaced entity should NOT retain the old one
            table1.execute(TableOperation.replace(replaceCustomer));
            System.out.println("\tSuccessfully updated the existing entity.");

            // Display the replaced entity
            System.out.println("\nRead the updated entities.");
            customer1 = table1.execute(TableOperation.retrieve("Harp", "Walter", CustomerEntity.class)).getResultAsType();
            if (customer1 != null) {
                System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s\t%s", customer1.getPartitionKey(), customer1.getRowKey(), customer1.getEmail(), customer1.getHomePhoneNumber(), customer1.getWorkPhoneNumber()));
            }

            // Demonstrate how to delete an entity
            System.out.println("\nDelete an entity.");
            table1.execute(TableOperation.delete(customer1));
            System.out.println("\tSuccessfully deleted the entity.");

            // Create a new table with a randomized name
            String tableName2 = tableNamePrefix + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("\nCreate a table with name \"%s\"", tableName2));
            table2 = createTable(tableClient, tableName2);
            System.out.println("\tSuccessfully created the table.");

            // Demonstrate batch table operations
            System.out.println("\nInsert a batch of new entities.");
            batchInsertOfCustomerEntities(table2);
            System.out.println("\tSuccessfully inserted the batch of entities.");

            // Query a range of data within a partition
            System.out.println("\nRetrieve entities with surname of Smith and first names >= 40 and <= 60.");
            partitionRangeQuery(table2, "Smith", "0040", "0060");

            // Query for all the data within a partition
            System.out.println("\nRetrieve entities with surname of Harp.");
            partitionScan(table1, "Harp");
            System.out.println("\n11. Retrieve entities with surname of Smith.");
            partitionScan(table2, "Smith");

            // Enumerate all tables in the storage account
            System.out.println("\nEnumerate all tables in the storage account.");
            for (String tableName : tableClient.listTables()) {
                System.out.println(String.format("\t%s", tableName));
            }
        }
        catch (Throwable t) {
            PrintHelper.printException(t);
        }
        finally {
            // Delete the tables (If you do not want to delete the tables comment out the block of code below)
            System.out.print("\nDelete any tables that were created.");

            if (table1 != null && table1.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the table: %s", table1.getName()));
            }

            if (table2 != null && table2.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the table: %s", table2.getName()));
            }
        }

        System.out.println("\nAzure Storage Table sample - Completed.\n");
    }

    /**
     * Creates and returns a table for the sample application to use.
     *
     * @param tableClient CloudTableClient object
     * @param tableName Name of the table to create
     * @return The newly created CloudTable object
     *
     * @throws StorageException
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     */
    private static CloudTable createTable(CloudTableClient tableClient, String tableName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Create a new table
        CloudTable table = tableClient.getTableReference(tableName);
        try {
            if (table.createIfNotExists() == false) {
                throw new IllegalStateException(String.format("Table with name \"%s\" already exists.", tableName));
            }
        }
        catch (StorageException s) {
            if (s.getCause() instanceof java.net.ConnectException) {
                System.out.println("Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
            }
            throw s;
        }

        return table;
    }

    /**
     * Demonstrate inserting of a large batch of entities. Some considerations for batch operations:
     *  1. You can perform updates, deletes, and inserts in the same single batch operation.
     *  2. A single batch operation can include up to 100 entities.
     *  3. All entities in a single batch operation must have the same partition key.
     *  4. While it is possible to perform a query as a batch operation, it must be the only operation in the batch.
     *  5. Batch size must be <= 4MB
     *
     * @param table The {@link CloudTable} object
     *
     * @throws StorageException
     */
    private static void batchInsertOfCustomerEntities(CloudTable table) throws StorageException {

        // Create the batch operation
        TableBatchOperation batchOperation1 = new TableBatchOperation();
        for (int i = 1; i <= 50; i++) {
            CustomerEntity entity = new CustomerEntity("Smith", String.format("%04d", i));
            entity.setEmail(String.format("smith%04d@contoso.com", i));
            entity.setHomePhoneNumber(String.format("425-555-%04d", i));
            entity.setWorkPhoneNumber(String.format("425-556-%04d", i));
            batchOperation1.insertOrMerge(entity);
        }

        // Execute the batch operation
        table.execute(batchOperation1);

        // Create the batch operation (Note the overwrite for some entities part of the previous batch operation)
        TableBatchOperation batchOperation2 = new TableBatchOperation();
        for (int i = 45; i <= 100; i++) {
            CustomerEntity entity = new CustomerEntity("Smith", String.format("%04d", i));
            entity.setEmail(String.format("smith%04d@contoso.com", i));
            entity.setWorkPhoneNumber(String.format("425-556-%04d", i));
            batchOperation2.insertOrReplace(entity);
        }

        // Execute the batch operation
        table.execute(batchOperation2);
    }

    /**
     * Demonstrate a partition range query whereby we are searching within a partition for a set of entities that are within a specific range.
     *
     * @param table The {@link CloudTable} object
     * @param partitionKey The partition within which to search
     * @param startRowKey The lowest bound of the row key range within which to search
     * @param endRowKey The highest bound of the row key range within which to search
     *
     * @throws StorageException
     */
    private static void partitionRangeQuery(CloudTable table, String partitionKey, String startRowKey, String endRowKey) throws StorageException {

        // Create the range scan query
        TableQuery<CustomerEntity> rangeQuery = TableQuery.from(CustomerEntity.class).where(
            TableQuery.combineFilters(
                TableQuery.generateFilterCondition("PartitionKey", QueryComparisons.EQUAL, partitionKey),
                TableQuery.Operators.AND,
                TableQuery.combineFilters(
                    TableQuery.generateFilterCondition("RowKey", QueryComparisons.GREATER_THAN_OR_EQUAL, startRowKey),
                    TableQuery.Operators.AND,
                    TableQuery.generateFilterCondition("RowKey", QueryComparisons.LESS_THAN_OR_EQUAL, endRowKey))));

        // Iterate through the results
        for (CustomerEntity entity : table.execute(rangeQuery)) {
            System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s\t%s", entity.getPartitionKey(), entity.getRowKey(), entity.getEmail(), entity.getHomePhoneNumber(), entity.getWorkPhoneNumber()));
        }
    }

    /**
     * Demonstrate a partition scan whereby we are searching for all the entities within a partition.
     * Note this is not as efficient as a range scan - but definitely more efficient than a full table scan.
     *
     * @param table The {@link CloudTable} object
     * @param partitionKey The partition within which to search
     *
     * @throws StorageException
     */
    private static void partitionScan(CloudTable table, String partitionKey) throws StorageException {

        // Create the partition scan query
        TableQuery<CustomerEntity> partitionScanQuery = TableQuery.from(CustomerEntity.class).where(
            (TableQuery.generateFilterCondition("PartitionKey", QueryComparisons.EQUAL, partitionKey)));

        // Iterate through the results
        for (CustomerEntity entity : table.execute(partitionScanQuery)) {
            System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s\t%s", entity.getPartitionKey(), entity.getRowKey(), entity.getEmail(), entity.getHomePhoneNumber(), entity.getWorkPhoneNumber()));
        }
    }


}
