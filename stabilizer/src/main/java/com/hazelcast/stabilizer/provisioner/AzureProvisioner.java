package com.hazelcast.stabilizer.provisioner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import javax.print.event.PrintJobAttributeEvent;
import javax.xml.parsers.ParserConfigurationException;

import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.VirtualMachineDiskOperations;
import com.microsoft.windowsazure.management.compute.VirtualMachineOperations;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineCreateParameters;
import org.xml.sax.SAXException;
import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.*;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import com.microsoft.windowsazure.management.models.LocationsListResponse;
import com.microsoft.windowsazure.management.models.LocationsListResponse.Location;

public class AzureProvisioner {

    static String uri = "https://management.core.windows.net/";
    static String subscriptionId = "3da94ed7-c058-4d83-8ddd-21b629af04b9";
    static String keyStoreLocation = "/Users/danny/.jclouds/azure.p12";
    static String keyStorePassword = "1qazxsw2";



    /*
    public static void main(String[] args)
            throws IOException, URISyntaxException, ServiceException, ParserConfigurationException, SAXException {
        Configuration config = ManagementConfiguration.configure(
                new URI(uri),
                subscriptionId,
                keyStoreLocation, // the file path to the JKS
                keyStorePassword, // the password for the JKS
                KeyStoreType.pkcs12 // flags that I'm using a JKS keystore
        );

        ComputeManagementClient computClient = ComputeManagementService.create(config);
        computClient.getHostedServicesOperations();

        VirtualMachineCreateParameters vmcp = new VirtualMachineCreateParameters();


        VirtualMachineOperations vmo =  computClient.getVirtualMachinesOperations();


        // create a management client to call the API
        ManagementClient client = ManagementService.create(config);

        // get the list of regions
        LocationsListResponse response = client.getLocationsOperations().list();
        ArrayList<Location> locations = response.getLocations();

        // write them out
        for( int i=0; i<locations.size(); i++){
            System.out.println(locations.get(i).getDisplayName());
        }
    }
    */


}
