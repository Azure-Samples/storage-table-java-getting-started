---
services: storage
platforms: java
author: sribhat-MSFT
---

# Getting Started with Azure Table Service in Java

This demo demonstrates how to perform common tasks using Azure Table storage 
and Azure Cosmos DB Table API including creating a table, CRUD operations, 
batch operations and different querying techniques. 

If you don't have a Microsoft Azure subscription you can get a FREE trial 
account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

## Running this sample

### Azure Cosmos DB Table API

1. Go to your Azure Cosmos DB Table API instance in the Azure Portal and select 
"Connection String" in the menu, select the Read-write Keys tab and copy the value 
in the "CONNECTION STRING" field.
2. Open the src/resources/config.properties file and set 
StorageConnectionString to your connection string.
3. Make sure you have maven and a reasonably new Java SDK installed
4. Run 'mvn install' from the root of the directory
5. Run 'java -jar target/storage-java-table-0.0.1-SNAPSHOT-jar-with-dependencies.jar' 
from the root of the directory

#### More Information
-[Introduction to Azure Cosmos DB Table API](https://docs.microsoft.com/en-us/azure/cosmos-db/table-introduction)

### Azure Table storage

This sample can be run using either the Azure Storage Emulator or with your 
Azure Storage account by updating the config.properties file with your 
connection string.

To run the sample using the Storage Emulator (Only available on Microsoft 
Windows OS):

1. Download and Install the Azure Storage Emulator [here](http://azure.microsoft.com/en-us/downloads/).
2. Start the Azure Storage Emulator by pressing the Start button or the Windows 
key and searching for it by typing "Azure Storage Emulator". Select it from the 
list of applications to start it.
3. Open the src/resources/config.properties file and set 
StorageConnectionString = UseDevelopmentStorage=true.
3. Make sure you have maven and a reasonably new Java SDK installed
4. Run 'mvn install' from the root of the directory
5. Run 'java -jar target/storage-java-table-0.0.1-SNAPSHOT-jar-with-dependencies.jar' 
from the root of the directory

To run the sample using the Storage Service:

1. Go to your Azure Storage account in the Azure Portal and under "SETTINGS" 
click on "Access keys". Copy either key1 or key2's "CONNECTION STRING".
1. Open the src/resources/config.properties file and set 
StorageConnectionString to the previously copied value..
3. Make sure you have maven and a reasonably new Java SDK installed
4. Run 'mvn install' from the root of the directory
5. Run 'java -jar target/storage-java-table-0.0.1-SNAPSHOT-jar-with-dependencies.jar' 
from the root of the directory

#### More information

[What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)

[Getting Started with Tables](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-table-storage/)

[Table Service Concepts](http://msdn.microsoft.com/en-us/library/dd179463.aspx)

[Table Service REST API](http://msdn.microsoft.com/en-us/library/dd179423.aspx)

[Azure Storage Java API](http://azure.github.io/azure-storage-java/)

[Storage Emulator](http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/)

