package com.hazelcast.stabilizer.agent;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;

public class AgentCli {

    private final OptionParser parser = new OptionParser();
    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private final OptionSpec<String> cloudIdentitySpec = parser.accepts("cloudIdentity",
            "Cloud identity")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> cloudCredentialSpec = parser.accepts("cloudCredential",
            "Cloud credential")
            .withOptionalArg().defaultsTo("ABC").ofType(String.class);
    private final OptionSpec<String> cloudProviderSpec = parser.accepts("cloudProvider",
            "Cloud provider")
            .withRequiredArg().ofType(String.class);


    public static void init(Agent agent, String[] args) throws IOException {
        AgentCli agentOptionSpec = new AgentCli();

        OptionSet options = agentOptionSpec.parser.parse(args);

        if (options.has(agentOptionSpec.helpSpec)) {
            agentOptionSpec.parser.printHelpOn(System.out);
            System.exit(0);
        }

        agent.cloudIdentity = options.valueOf(agentOptionSpec.cloudIdentitySpec);
        agent.cloudCredential = options.valueOf(agentOptionSpec.cloudCredentialSpec);
        agent.cloudProvider = options.valueOf(agentOptionSpec.cloudProviderSpec);
    }
}
