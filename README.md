---
services: Storage
platforms: Java
author: sribhat-MSFT
---

# Azure Storage: Tables

Azure Table Service Sample - Demonstrate how to perform common tasks using the Microsoft Azure Table Service including creating a table, CRUD operations, batch operations and different querying techniques.

The Azure Table storage service stores large amounts of structured data. The service is a NoSQL datastore which accepts authenticated calls from inside and outside the Azure cloud. Azure tables are ideal for storing structured, non-relational data. You can use the Table service to store and query huge sets of structured, non-relational data, and your tables will scale as demand increases.

Note: If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

## Running this sample

This sample can be run using either the Azure Storage Emulator or your Azure Storage account by updating the config.properties file with your "AccountName" and "Key".

To run the sample using the Storage Emulator (default option - Only available on Microsoft Windows OS):

1. Start the Azure Storage Emulator by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
2.  Set breakpoints and run the project.

To run the sample using the Storage Service:

1. Open the app.config file and comment out the connection string for the emulator "UseDevelopmentStorage=True" and uncomment the connection string for the storage service "AccountName=[]".
2. Create a Storage Account through the Azure Portal and provide your account name and account key in the config.properties file.
3. Set breakpoints and run the project.

## More information

[What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)

[Getting Started with Tables](http://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-table-storage/)

[Table Service Concepts](http://msdn.microsoft.com/en-us/library/dd179463.aspx)

[Table Service REST API](http://msdn.microsoft.com/en-us/library/dd179423.aspx)

[Azure Storage Java API](http://azure.github.io/azure-storage-java/)

[Storage Emulator](http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/)

