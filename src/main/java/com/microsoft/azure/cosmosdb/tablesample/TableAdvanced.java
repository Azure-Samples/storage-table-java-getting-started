/*
  Copyright Microsoft Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package com.microsoft.azure.cosmosdb.tablesample;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

/**
 * This sample illustrates advanced usage of the Azure table storage service.
 */
class TableAdvanced {

    /**
     * Executes the samples.
     *
     * @throws URISyntaxException Uri has invalid syntax
     * @throws InvalidKeyException Invalid key
     */
    void runSamples() throws InvalidKeyException, URISyntaxException, IOException {
        System.out.println();
        System.out.println();
        if (TableClientProvider.isAzureCosmosdbTable()) {
            return;
        }

        PrintHelper.printSampleStartInfo("Table Advanced");
        // Create a table service client
        CloudTableClient tableClient = TableClientProvider.getTableClientReference();

        try {
            System.out.println("Service properties sample");
            serviceProperties(tableClient);
            System.out.println();

            System.out.println("CORS rules sample");
            corsRules(tableClient);
            System.out.println();

            System.out.println("Table Acl sample");
            tableAcl(tableClient);
            System.out.println();

            // This will fail unless the account is RA-GRS enabled.
//            System.out.println("Service stats sample");
//            serviceStats(tableClient);
//            System.out.println();
        } catch (Throwable t) {
            PrintHelper.printException(t);
        }

        PrintHelper.printSampleCompleteInfo("Table Advanced");
    }

    /**
     * Manage the service properties including logging hour and minute metrics.
     * @param tableClient Azure Storage Table Service
     */
    private void serviceProperties(CloudTableClient tableClient) throws StorageException {
        System.out.println("Get service properties");
        ServiceProperties originalProps = tableClient.downloadServiceProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            ServiceProperties props = new ServiceProperties();

            props.getLogging().setLogOperationTypes(EnumSet.allOf(LoggingOperations.class));
            props.getLogging().setRetentionIntervalInDays(2);
            props.getLogging().setVersion("1.0");

            final MetricsProperties hours = props.getHourMetrics();
            hours.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
            hours.setRetentionIntervalInDays(1);
            hours.setVersion("1.0");

            final MetricsProperties minutes = props.getMinuteMetrics();
            minutes.setMetricsLevel(MetricsLevel.SERVICE);
            minutes.setRetentionIntervalInDays(1);
            minutes.setVersion("1.0");

            tableClient.uploadServiceProperties(props);

            System.out.println();
            System.out.println("Logging");
            System.out.printf("version: %s%n", props.getLogging().getVersion());
            System.out.printf("retention interval: %d%n", props.getLogging().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getLogging().getLogOperationTypes());
            System.out.println();
            System.out.println("Hour Metrics");
            System.out.printf("version: %s%n", props.getHourMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getHourMetrics().getMetricsLevel());
            System.out.println();
            System.out.println("Minute Metrics");
            System.out.printf("version: %s%n", props.getMinuteMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getMinuteMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getMinuteMetrics().getMetricsLevel());
            System.out.println();
        }
        finally {
            // Revert back to original service properties
            tableClient.uploadServiceProperties(originalProps);
        }
    }

    /**
     * Set CORS rules sample.
     * @param tableClient Azure Storage Table Service
     */
    private void corsRules(CloudTableClient tableClient) throws StorageException {
        // Get service properties
        System.out.println("Get service properties");
        ServiceProperties originalProps = tableClient.downloadServiceProperties();

        try {
            // Setr CORS rules
            System.out.println("Set CORS rules");
            CorsRule ruleAllowAll = new CorsRule();
            ruleAllowAll.getAllowedOrigins().add("*");
            ruleAllowAll.getAllowedMethods().add(CorsHttpMethods.GET);
            ruleAllowAll.getAllowedHeaders().add("*");
            ruleAllowAll.getExposedHeaders().add("*");
            ServiceProperties props = new ServiceProperties();
            props.getCors().getCorsRules().add(ruleAllowAll);
            tableClient.uploadServiceProperties(props);
        }
        finally {
            // Revert back to original service properties
            tableClient.uploadServiceProperties(originalProps);
        }
    }

    /**
     * Manage table access properties
     * @param tableClient Azure Storage Table Service
     */
    private void tableAcl(CloudTableClient tableClient) throws StorageException, URISyntaxException, InterruptedException {
        // Get a reference to a table
        // The table name must be lower case
        CloudTable table = tableClient.getTableReference("table"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            // Create the table
            System.out.println("Create table");
            table.createIfNotExists();

            // Get permissions
            TablePermissions permissions = table.downloadPermissions();

            System.out.println("Set table permissions");
            final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            cal.add(Calendar.MINUTE, -30);
            final Date start = cal.getTime();
            cal.add(Calendar.MINUTE, 30);
            final Date expiry = cal.getTime();

            SharedAccessTablePolicy policy = new SharedAccessTablePolicy();
            policy.setPermissions(EnumSet.of(SharedAccessTablePermissions.ADD, SharedAccessTablePermissions.DELETE, SharedAccessTablePermissions.UPDATE));
            policy.setSharedAccessStartTime(start);
            policy.setSharedAccessExpiryTime(expiry);
            permissions.getSharedAccessPolicies().put("key1", policy);

            // Set table permissions
            table.uploadPermissions(permissions);
            Thread.sleep(30000);

            System.out.println("Get table permissions");
            // Get table permissions
            permissions = table.downloadPermissions();

            HashMap<String, SharedAccessTablePolicy> accessPolicies = permissions.getSharedAccessPolicies();
            Iterator it = accessPolicies.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                SharedAccessTablePolicy value = (SharedAccessTablePolicy) pair.getValue();
                System.out.printf(" %s: %n", pair.getKey());
                System.out.printf("  Permissions: %s%n", value.permissionsToString());
                System.out.printf("  Start: %s%n", value.getSharedAccessStartTime());
                System.out.printf("  Expiry: %s%n", value.getSharedAccessStartTime());
                it.remove();
            }

            System.out.println("Clear table permissions");
            // Clear permissions
            permissions.getSharedAccessPolicies().clear();
            table.uploadPermissions(permissions);
        }
        finally {
            // Delete the table
            System.out.println("Delete table");
            table.deleteIfExists();
        }
    }

    /**
     * Retrieve statistics related to replication for the Table service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     * @param tableClient Azure Storage Table Service
     */
    private void serviceStats(CloudTableClient tableClient) throws StorageException {
        // Get service stats
        System.out.println("Service Stats:");
        ServiceStats stats = tableClient.getServiceStats();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }

}
