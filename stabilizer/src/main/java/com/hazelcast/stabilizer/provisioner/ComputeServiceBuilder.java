package com.hazelcast.stabilizer.provisioner;

import com.google.inject.AbstractModule;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.StabilizerProperties;
import com.microsoft.windowsazure.management.models.LocationsListResponse;
import org.apache.log4j.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.azurecompute.AzureComputeApi;
import org.jclouds.azurecompute.compute.AzureComputeServiceAdapter;
import org.jclouds.azurecompute.domain.Location;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.stabilizer.Utils.newFile;
import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

public class ComputeServiceBuilder {

    private final static Logger log = Logger.getLogger(ComputeServiceBuilder.class);

    private final StabilizerProperties props;

    public ComputeServiceBuilder(StabilizerProperties props) {
        if (props == null) {
            throw new NullPointerException("props can't be null");
        }
        this.props = props;
    }

    public ComputeService build() {
        ensurePublicPrivateKeyExist();

        String cloudProvider = props.get("CLOUD_PROVIDER");
        String identity = props.get("CLOUD_IDENTITY");
        String credential = props.get("CLOUD_CREDENTIAL");
        String endpointName = props.get("END_POINT");

        log.info("Using CLOUD_PROVIDER: " + cloudProvider);


        ContextBuilder contextBuilder = newContextBuilder(cloudProvider);

        return contextBuilder.overrides(newOverrideProperties())
                .credentials(identity, credential)
                .endpoint(endpointName)
                .modules(getModules())
                .buildView(ComputeServiceContext.class)
                .getComputeService();
    }







    static String uri = "https://management.core.windows.net/";
    static String subscriptionId = "3da94ed7-c058-4d83-8ddd-21b629af04b9";
    static String keyStoreLocation = "/Users/danny/.jclouds/azure.p12";
    static String keyStorePassword = "1qazxsw2";
    static String cloudProvider = "azurecompute";

    public static void main(String[] args){




        ContextBuilder contextBuilder = ContextBuilder.newBuilder(cloudProvider);
        AzureComputeApi api  = contextBuilder
                .credentials(keyStoreLocation, keyStorePassword)
                .endpoint(uri + subscriptionId)
                .buildApi(AzureComputeApi.class);

        List<Location> locations = api.getLocationApi().list();
        for(Location l : locations){
            System.out.println(l);
        }

        List imaglist = api.getImageApi().list();
        for(Object o : imaglist){
            System.out.println(o);
        }

        api.getVirtualMachineApiForDeploymentInService(

                "HiDeploy" , "Compute"
        ).start("Hiname");
}











    private ContextBuilder newContextBuilder(String cloudProvider) {
        try {
            return ContextBuilder.newBuilder(cloudProvider);
        } catch (NoSuchElementException e) {
            Utils.exitWithError(log, "Unrecognized cloud-provider [" + cloudProvider + "]");
            return null;
        }
    }

    private List<AbstractModule> getModules() {
        return asList(new SLF4JLoggingModule(), new SshjSshClientModule());
    }

    private void ensurePublicPrivateKeyExist() {
        File publicKey = newFile("~", ".ssh", "id_rsa.pub");
        if (!publicKey.exists()) {
            Utils.exitWithError(log, "Could not found public key: " + publicKey.getAbsolutePath() + "\n" +
                    "To create a public/private execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }

        File privateKey = newFile("~", ".ssh", "id_rsa");
        if (!privateKey.exists()) {
            Utils.exitWithError(log, "Public key " + publicKey.getAbsolutePath() + " was found," +
                    " but private key: " + privateKey.getAbsolutePath() + " is missing\n" +
                    "To create a public/private key execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }
    }


    static private Properties newOverrideProperties2() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties properties = new Properties();
        properties.setProperty(POLL_INITIAL_PERIOD, "50");
        properties.setProperty(POLL_MAX_PERIOD, "1000");
        return properties;
    }


    private Properties newOverrideProperties() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties properties = new Properties();
        properties.setProperty(POLL_INITIAL_PERIOD, props.get("CLOUD_POLL_INITIAL_PERIOD", "50"));
        properties.setProperty(POLL_MAX_PERIOD, props.get("CLOUD_POLL_MAX_PERIOD", "1000"));
        return properties;
    }
}
