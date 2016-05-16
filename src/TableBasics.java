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

    protected static CloudTableClient tableClient = null;
    protected static CloudTable table1 = null;
    protected static CloudTable table2 = null;
    protected final static String tableNamePrefix = "tablebasics";

    /**
     * Azure Storage Table Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("Azure Storage Table sample - Starting.");

        Scanner scan = null;
        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create a table client for interacting with the table service
            tableClient = getTableClientReference();

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
            printException(t);
        }
        finally {
            // Delete the tables (If you do not want to delete the tables comment out the block of code below)
            System.out.print("\nDelete any tables that were created. Press any key to continue...");
            scan.nextLine();

            if (table1 != null && table1.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the table: %s", table1.getName()));
            }

            if (table2 != null && table2.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the table: %s", table2.getName()));
            }

            // Close the scanner
            scan.close();
        }

        System.out.println("\nAzure Storage Table sample - Completed.\n");
    }

    /**
     * Validates the connection string and returns the storage table client.
     * The connection string must be in the Azure connection string format.
     *
     * @return The newly created CloudTableClient object
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudTableClient getTableClientReference() throws RuntimeException, IOException, IllegalArgumentException, URISyntaxException, InvalidKeyException {

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

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(prop.getProperty("StorageConnectionString"));
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

        return storageAccount.createCloudTableClient();
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

    /**
     * Print the exception stack trace
     *
     * @param ex Exception to be printed
     */
    public static void printException(Throwable t) {

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        if (t instanceof StorageException) {
            if (((StorageException) t).getExtendedErrorInformation() != null) {
                System.out.println(String.format("\nError: %s", ((StorageException) t).getExtendedErrorInformation().getErrorMessage()));
            }
        }
        System.out.println(String.format("Exception details:\n%s", stringWriter.toString()));
    }
}
