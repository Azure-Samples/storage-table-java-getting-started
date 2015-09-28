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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

/*
 * Azure Table Service Sample - Demonstrate how to perform common tasks using the Microsoft Azure Table Service
 * including creating a table, CRUD operations, batch operations and different querying techniques.
 *
 * Documentation References:
 *  - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
 *  - Getting Started with Tables - http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-table-storage/
 *  - Table Service Concepts - http://msdn.microsoft.com/en-us/library/dd179463.aspx
 *  - Table Service REST API - http://msdn.microsoft.com/en-us/library/dd179423.aspx
 *  - Azure Storage Java API - http://azure.github.io/azure-storage-java/
 *  - Storage Emulator - http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
 *
 * Instructions:
 *      This sample can be run using either the Azure Storage Emulator or your Azure Storage
 *      account by updating the config.properties file with your "AccountName" and "Key".
 *
 *      To run the sample using the Storage Emulator (default option - Only available on Microsoft Windows OS)
 *          1.  Start the Azure Storage Emulator by pressing the Start button or the Windows key and searching for it
 *              by typing "Azure Storage Emulator". Select it from the list of applications to start it.
 *          2.  Set breakpoints and run the project.
 *
 *      To run the sample using the Storage Service
 *          1.  Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
 *              uncomment the connection string for the storage service (AccountName=[]...)
 *          2.  Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file.
 *              See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information.
 *          3.  Set breakpoints and run the project.
 */
public class TableBasics {

    protected static CloudTable table = null;
    protected final static String tableNamePrefix = "tablebasics";

    /**
     * Azure Storage Table Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("Azure Storage Table sample - Starting.\n");

        Scanner scan = null;
        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create new table with a randomized name
            String tableName = tableNamePrefix + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("\n1. Create a table with name \"%s\"", tableName));
            try {
                table = createTable(tableName);
            }
            catch (IllegalStateException e) {
                System.out.println(String.format("\tTable already exists."));
                throw e;
            }
            System.out.println("\tSuccessfully created the table.");

            // Create a sample entity for use
            CustomerEntity customer = new CustomerEntity("Harp", "Walter");
            customer.setEmail("Walter@contoso.com");
            customer.setPhoneNumber("425-555-0101");

            // Create and insert a customer entity
            System.out.println("\n2. Insert a new entity.");
            TableOperation insertOrMergeOperation = TableOperation.insertOrMerge(customer);
            table.execute(insertOrMergeOperation);
            System.out.println("\tSuccessfully inserted the new entity.");

            // Demonstrate how to read the entity using a point query
            System.out.println("\n3. Read the inserted entity.");
            TableOperation retrieveOperation = TableOperation.retrieve("Harp", "Walter", CustomerEntity.class);
            customer = table.execute(retrieveOperation).getResultAsType();
            if (customer != null) {
                System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s", customer.getPartitionKey(), customer.getRowKey(), customer.getEmail(), customer.getPhoneNumber()));
            }

            // Demonstrate how to update the entity by changing the phone number
            System.out.println("\n4. Update an existing entity by changing the phone number.");
            customer.setPhoneNumber("425-555-0105");
            TableOperation mergeOperation = TableOperation.merge(customer);
            table.execute(mergeOperation);
            System.out.println("\tSuccessfully updated the existing entity.");

            // Demonstrate how to read the updated entity using a point query
            System.out.println("\n5. Read the updated entity.");
            retrieveOperation = TableOperation.retrieve("Harp", "Walter", CustomerEntity.class);
            customer = table.execute(retrieveOperation).getResultAsType();
            if (customer != null) {
                System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s", customer.getPartitionKey(), customer.getRowKey(), customer.getEmail(), customer.getPhoneNumber()));
            }

            // Demonstrate how to delete an entity
            System.out.println("\n6. Delete the entity.");
            TableOperation deleteEntityOperation = TableOperation.delete(customer);
            table.execute(deleteEntityOperation);
            System.out.println("\tSuccessfully deleted the entity.");

            // Demonstrate upsert and batch table operations
            System.out.println("\n7. Insert a batch of new entities.");
            batchInsertOfCustomerEntities(table);
            System.out.println("\tSuccessfully inserted the batch of entities.");

            // Query a range of data within a partition
            System.out.println("\n8. Retrieve entities with surname of Smith and first names >= 15 and <= 70.");
            partitionRangeQuery(table, "Smith", "0015", "0070");

            // Query for all the data within a partition
            System.out.println("\n9. Retrieve entities with surname of Smith.");
            partitionScan(table, "Smith");
        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            // Delete the table (If you do not want to delete the table comment out the block of code below)
            if (table != null)
            {
                System.out.print("\n9. Delete the table. Press any key to continue...");
                scan.nextLine();
                if (table.deleteIfExists() == true) {
                    System.out.println("\tSuccessfully deleted the table.");
                }
                else {
                    System.out.println("\tNothing to delete.");
                }
            }

            // Close the scanner
            scan.close();
        }

        System.out.println("\nAzure Storage Table sample - Completed.\n");
    }

    /**
     * Validates the connection string and returns the storage account.
     * The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        }
        catch (IllegalArgumentException|URISyntaxException e) {
            System.out.println("\nConnection string specifies an invalid URI.");
            System.out.println("Please confirm the connection string is in the Azure connection string format.");
            throw e;
        }
        catch (InvalidKeyException e) {
            System.out.println("\nConnection string specifies an invalid key.");
            System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
            throw e;
        }

        return storageAccount;
    }

    /**
     * Creates and returns a table for the sample application to use.
     *
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
    private static CloudTable createTable(String tableName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Retrieve the connection string
        Properties prop = new Properties();
        try {
            InputStream propertyStream = TableBasics.class.getClassLoader().getResourceAsStream("config.properties");
            if (propertyStream != null) {
                prop.load(propertyStream);
            }
            else {
                throw new RuntimeException();
            }
        } catch (RuntimeException|IOException e) {
            System.out.println("\nFailed to load config.properties file.");
            throw e;
        }
        String storageConnectionString = prop.getProperty("StorageConnectionString");

        // Retrieve storage account information from connection string.
        CloudStorageAccount storageAccount = createStorageAccountFromConnectionString(storageConnectionString);

        // Create a table client for interacting with the table service
        CloudTableClient tableClient = storageAccount.createCloudTableClient();

        // Create a new table
        CloudTable table = tableClient.getTableReference(tableName);
        try {
            if (table.createIfNotExists() == false) {
                throw new IllegalStateException(String.format("Table with name \"%s\" already exists.", tableName));
            }
        }
        catch (StorageException e) {
            System.out.println("\nCaught storage exception from the client.");
            System.out.println("If running with the default configuration please make sure you have started the storage emulator.");
            throw e;
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
        TableBatchOperation batchOperation = new TableBatchOperation();

        // Generate test data.
        for (int i = 0; i < 100; i++) {
            CustomerEntity entity = new CustomerEntity("Smith", String.format("%04d", i));
            entity.setEmail(String.format("%04d@contoso.com", i));
            entity.setPhoneNumber(String.format("425-555-%04d", i));
            batchOperation.insertOrMerge(entity);
        }

        // Execute the batch operation
        table.execute(batchOperation);
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
            System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s", entity.getPartitionKey(), entity.getRowKey(), entity.getEmail(), entity.getPhoneNumber()));
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
            System.out.println(String.format("\tCustomer: %s,%s\t%s\t%s", entity.getPartitionKey(), entity.getRowKey(), entity.getEmail(), entity.getPhoneNumber()));
        }
    }

    /**
     * Print the exception stack trace
     *
     * @param ex Exception to be printed
     */
    public static void printException(Throwable ex) {

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        ex.printStackTrace(printWriter);
        System.out.println(String.format("Exception details:\n%s\n", stringWriter.toString()));
    }
}
