package it.unitn.ds1.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.DataStoreManager;
import it.unitn.ds1.Messages;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.StorageNode;
import it.unitn.ds1.types.OperationType;
import it.unitn.ds1.utils.OperationDelays;
import it.unitn.ds1.utils.FzfIntegration;

/**
 * Interactive command-line testing tool for the distributed storage system.
 * Enhanced with fzf integration for better command discovery.
 */
public class InteractiveTest {
    
    private static final Logger logger = LoggerFactory.getLogger(InteractiveTest.class);
    
    // System components
    private static ActorSystem system;
    private static TreeMap<Integer, ActorRef> nodeRegistry;
    private static List<ActorRef> clients;
    private static List<Integer> crashedNodes;
    
    // Configuration
    private static final int MAX_NODES = 50;
    private static final int MAX_CLIENTS = 10;
    
    // Node and client IDs
    private static int nextNodeId = 10;
    private static int nextClientId = 1;

    // Command definitions for fuzzy search
    private static final Map<String, CommandInfo> COMMANDS = new HashMap<>();

    // DataStore Manager
    private static final DataStoreManager dataStoreManager = DataStoreManager.getInstance();
    
    static {
        initializeCommands();
    }

    private static class CommandInfo {
        String name;
        String description;
        String usage;
        String category;
        
        CommandInfo(String name, String description, String usage, String category) {
            this.name = name;
            this.description = description;
            this.usage = usage;
            this.category = category;
        }
        
        @Override
        public String toString() {
            return String.format("%-15s - %s (Usage: %s)", name, description, usage);
        }
    }

    private static void initializeCommands() {
        // System Information
        COMMANDS.put("help", new CommandInfo("help", "Show help message", "help", "System"));
        COMMANDS.put("status", new CommandInfo("status", "Show system status", "status", "System"));
        COMMANDS.put("nodes", new CommandInfo("nodes", "List all nodes and their states", "nodes", "System"));
        COMMANDS.put("clients", new CommandInfo("clients", "List all clients", "clients", "System"));
        COMMANDS.put("data", new CommandInfo("data", "Show data distribution across nodes", "data", "System"));
        
        // Basic Operations
        COMMANDS.put("get", new CommandInfo("get", "Get value for key", "get <key> [nodeId] [clientId]", "Operations"));
        COMMANDS.put("put", new CommandInfo("put", "Store key-value pair", "put <key> <value> [nodeId] [clientId]", "Operations"));
        
        // Node Management
        COMMANDS.put("addnode", new CommandInfo("addnode", "Add a new node to the system", "addnode [nodeId]", "Nodes"));
        COMMANDS.put("removenode", new CommandInfo("removenode", "Remove a node from the system", "removenode <nodeId>", "Nodes"));
        COMMANDS.put("crash", new CommandInfo("crash", "Crash a specific node", "crash <nodeId>", "Nodes"));
        COMMANDS.put("recover", new CommandInfo("recover", "Recover a crashed node", "recover <nodeId> [peerNodeId]", "Nodes"));
        COMMANDS.put("join", new CommandInfo("join", "Join node to system via peer", "join <nodeId> <peerNodeId>", "Nodes"));
        COMMANDS.put("leave", new CommandInfo("leave", "Make node leave gracefully", "leave <nodeId>", "Nodes"));
        
        // Client Management
        COMMANDS.put("addclient", new CommandInfo("addclient", "Add a new client", "addclient", "Clients"));
        COMMANDS.put("removeclient", new CommandInfo("removeclient", "Remove a client", "removeclient <id>", "Clients"));
        
        // Testing Scenarios
        COMMANDS.put("test", new CommandInfo("test", "Run test scenarios", "test <scenario>", "Testing"));
        COMMANDS.put("benchmark", new CommandInfo("benchmark", "Run performance benchmark", "benchmark <ops>", "Testing"));
        COMMANDS.put("stress", new CommandInfo("stress", "Run stress test", "stress <duration>", "Testing"));
        
        // System Control
        COMMANDS.put("clear", new CommandInfo("clear", "Clear screen", "clear", "System"));
        COMMANDS.put("reset", new CommandInfo("reset", "Reset system to initial state", "reset", "System"));
        COMMANDS.put("quit", new CommandInfo("quit", "Exit the program", "quit", "System"));
    }

    public static void main(String[] args) {
        // Check for command line mode
        if (args.length > 0) {
            handleCommandLineMode(args);
            return;
        }
        
        try {
            initializeSystem();
            displayWelcomeMessage();
            new InteractiveTest().runInteractiveSession();
        } catch (Exception e) {
            logger.error("Interactive test execution failed", e);
        } finally {
            cleanup();
        }
    }

    private static void handleCommandLineMode(String[] args) {
        String command = args[0].toLowerCase();
        
        switch (command) {
            case "--help", "-h" -> displayCommandLineHelp();
            case "--list-commands" -> listAllCommands();
            case "--fzf-test" -> testFzfIntegration();
            default -> {
                System.out.println("Unknown option: " + command);
                displayCommandLineHelp();
                System.exit(1);
            }
        }
    }

    private static void displayCommandLineHelp() {
        System.out.println("Distributed Storage System Interactive Test");
        System.out.println("\nUsage:");
        System.out.println("  gradle run                    # Start interactive mode");
        System.out.println("  gradle run --args=\"--help\"     # Show this help");
        System.out.println("  gradle run --args=\"--list-commands\" # List all available commands");
        System.out.println("  gradle run --args=\"--fzf-test\" # Test fzf integration");
        System.out.println("\nInteractive Mode Commands:");
        System.out.println("  Type '?' or 'search' in interactive mode for fuzzy command search");
        System.out.println("  Type 'help' for full command reference");
    }

    private static void listAllCommands() {
        System.out.println("Available Commands:");
        System.out.println("==================");
        
        Map<String, List<CommandInfo>> categorized = COMMANDS.values().stream()
            .collect(Collectors.groupingBy(cmd -> cmd.category));
        
        for (Map.Entry<String, List<CommandInfo>> entry : categorized.entrySet()) {
            System.out.println("\n" + entry.getKey() + ":");
            for (CommandInfo cmd : entry.getValue()) {
                System.out.printf("  %-15s - %s%n", cmd.name, cmd.description);
            }
        }
    }

    private static void testFzfIntegration() {
        System.out.println("Testing fzf integration...");
        System.out.println("fzf available: " + FzfIntegration.isFzfAvailable());
        
        List<String> testOptions = new ArrayList<>();
        for (CommandInfo cmd : COMMANDS.values()) {
            testOptions.add(cmd.toString());
        }
        
        String selection = FzfIntegration.selectWithFzf(testOptions, "Select a command");
        if (selection != null) {
            System.out.println("You selected: " + selection);
        } else {
            System.out.println("No selection made");
        }
    }

    private static void displayWelcomeMessage() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("    INTERACTIVE DISTRIBUTED STORAGE SYSTEM TEST");
        if (FzfIntegration.isFzfAvailable()) {
            System.out.println("                  🔍 fzf integration enabled");
        }
        System.out.println("=".repeat(80));
        System.out.println("Current system state:");
        System.out.println("- Active nodes: " + (nodeRegistry.size() - crashedNodes.size()));
        System.out.println("- Crashed nodes: " + crashedNodes.size());
        System.out.println("- Clients: " + clients.size());
        System.out.println("- Next node ID: " + nextNodeId);
        System.out.println("- Replication factor (N): " + dataStoreManager.N + " (W=" + dataStoreManager.W + ", R=" + dataStoreManager.R + ")");
        System.out.println("\nCommands: Type 'help', '?' (fzf), 'search <term>', or 'quit'");
        System.out.println("=".repeat(80) + "\n");
    }

    private void runInteractiveSession() {
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            try {
                System.out.print("storage> ");
                
                // Check if scanner has next line available
                if (!scanner.hasNextLine()) {
                    System.out.println("\nInput stream closed. Exiting...");
                    break;
                }
                
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) continue;
                
                // Check for fzf command search
                if (input.equals("?")) {
                    handleFzfCommandSearch();
                    continue;
                }
                
                // Check for fuzzy search
                if (input.startsWith("search")) {
                    handleFuzzySearch(input, scanner);
                    continue;
                }
                
                String[] parts = input.split("\\s+");
                String command = parts[0].toLowerCase();
                
                try {
                    switch (command) {
                        case "help", "h" -> displayHelp();
                        case "status", "s" -> displayStatus();
                        case "nodes" -> displayNodes();
                        case "clients" -> displayClients();
                        case "data" -> displayData();
                        
                        // Basic Operations
                        case "get" -> handleGet(parts);
                        case "put", "update" -> handleUpdate(parts);
                        
                        // Node Management
                        case "addnode" -> handleAddNode(parts);
                        case "removenode" -> handleRemoveNode(parts);
                        case "crash" -> handleCrash(parts);
                        case "recover" -> handleRecover(parts);
                        case "join" -> handleJoin(parts);
                        case "leave" -> handleLeave(parts);
                        
                        // Client Management
                        case "addclient" -> handleAddClient();
                        case "removeclient" -> handleRemoveClient(parts);
                        
                        // Testing Scenarios
                        case "test" -> handleTestScenario(parts);
                        case "benchmark" -> handleBenchmark(parts);
                        case "stress" -> handleStressTest(parts);
                        
                        // System Control
                        case "clear" -> clearScreen();
                        case "reset" -> handleReset();
                        case "quit", "exit", "q" -> {
                            System.out.println("Shutting down system...");
                            return;
                        }
                        
                        default -> {
                            // Try fuzzy search for similar commands
                            List<CommandInfo> suggestions = fuzzySearchCommands(command);
                            if (!suggestions.isEmpty()) {
                                System.out.println("Unknown command: " + command);
                                System.out.println("Did you mean:");
                                suggestions.stream().limit(3).forEach(cmd -> 
                                    System.out.println("  " + cmd.name + " - " + cmd.description));
                            } else {
                                System.out.println("Unknown command: " + command + ". Type 'help' or '?' for available commands.");
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error executing command: " + e.getMessage());
                    logger.error("Command execution error", e);
                }
            } catch (NoSuchElementException e) {
                System.out.println("\nInput stream closed or EOF reached. Exiting...");
                break;
            } catch (IllegalStateException e) {
                System.out.println("\nScanner closed. Exiting...");
                break;
            }
        }
        
        scanner.close();
    }

    private void handleFzfCommandSearch() {
        List<String> commandOptions = new ArrayList<>();
        
        // Group commands by category for better display
        Map<String, List<CommandInfo>> categorized = COMMANDS.values().stream()
            .collect(Collectors.groupingBy(cmd -> cmd.category));
        
        for (Map.Entry<String, List<CommandInfo>> entry : categorized.entrySet()) {
            commandOptions.add("── " + entry.getKey() + " ──");
            for (CommandInfo cmd : entry.getValue()) {
                commandOptions.add(cmd.toString());
            }
            commandOptions.add(""); // Empty line for separation
        }
        
        String selection = FzfIntegration.selectWithFzf(commandOptions, "Select command");
        
        if (selection != null && !selection.startsWith("──") && !selection.isEmpty()) {
            // Extract command name from selection
            String commandName = selection.split(" ")[0];
            if (COMMANDS.containsKey(commandName)) {
                System.out.println("Selected: " + commandName);
                System.out.print("Enter parameters (or press Enter for no parameters): ");
                
                try (// Use the main scanner instead of creating a new one
                Scanner scanner = new Scanner(System.in)) {
                    try {
                        if (scanner.hasNextLine()) {
                            String params = scanner.nextLine().trim();
                            
                            String fullCommand = params.isEmpty() ? commandName : commandName + " " + params;
                            System.out.println("Executing: " + fullCommand);
                            
                            // Execute the command by simulating input
                            executeCommand(fullCommand);
                        }
                    } catch (NoSuchElementException e) {
                        System.out.println("Input not available, skipping parameter input.");
                    }
                    // Don't close this scanner as it's used by the main loop
                }
            }
        }
    }

    private void executeCommand(String fullCommand) {
        String[] parts = fullCommand.split("\\s+");
        String command = parts[0].toLowerCase();
        
        try {
            switch (command) {
                case "help", "h" -> displayHelp();
                case "status", "s" -> displayStatus();
                case "nodes" -> displayNodes();
                case "clients" -> displayClients();
                case "data" -> displayData();
                
                // Basic Operations
                case "get" -> handleGet(parts);
                case "put", "update" -> handleUpdate(parts);
                
                // Node Management
                case "addnode" -> handleAddNode(parts);
                case "removenode" -> handleRemoveNode(parts);
                case "crash" -> handleCrash(parts);
                case "recover" -> handleRecover(parts);
                case "join" -> handleJoin(parts);
                case "leave" -> handleLeave(parts);
                
                // Client Management
                case "addclient" -> handleAddClient();
                case "removeclient" -> handleRemoveClient(parts);
                
                // Testing Scenarios
                case "test" -> handleTestScenario(parts);
                case "benchmark" -> handleBenchmark(parts);
                case "stress" -> handleStressTest(parts);
                
                // System Control
                case "clear" -> clearScreen();
                case "reset" -> handleReset();
                
                default -> System.out.println("Command not recognized: " + command);
            }
        } catch (Exception e) {
            System.err.println("Error executing command: " + e.getMessage());
            logger.error("Command execution error", e);
        }
    }

    private static void handleFuzzySearch(String input, Scanner scanner) {
        String searchTerm = "";
        
        if (input.startsWith("search ")) {
            searchTerm = input.substring(7).trim();
        } else {
            System.out.print("Search commands: ");
            try {
                if (scanner.hasNextLine()) {
                    searchTerm = scanner.nextLine().trim();
                } else {
                    System.out.println("Input not available.");
                    return;
                }
            } catch (NoSuchElementException e) {
                System.out.println("Input not available.");
                return;
            }
        }
        
        if (searchTerm.isEmpty()) {
            listAllCommands();
            return;
        }
        
        List<CommandInfo> matches = fuzzySearchCommands(searchTerm);
        
        if (matches.isEmpty()) {
            System.out.println("No commands found matching: " + searchTerm);
        } else {
            System.out.println("\n🔍 Search Results for '" + searchTerm + "':");
            System.out.println("=".repeat(50));
            
            Map<String, List<CommandInfo>> categorized = matches.stream()
                .collect(Collectors.groupingBy(cmd -> cmd.category));
            
            for (Map.Entry<String, List<CommandInfo>> entry : categorized.entrySet()) {
                System.out.println("\n📁 " + entry.getKey() + ":");
                for (CommandInfo cmd : entry.getValue()) {
                    System.out.printf("  %-15s - %s%n", cmd.name, cmd.description);
                    System.out.printf("  └─ Usage: %s%n", cmd.usage);
                }
            }
            System.out.println();
        }
    }

    // Fuzzy search helper for commands
    private static List<CommandInfo> fuzzySearchCommands(String searchTerm) {
        String lowerTerm = searchTerm.toLowerCase();
        return COMMANDS.values().stream()
            .filter(cmd -> cmd.name.toLowerCase().contains(lowerTerm)
                        || cmd.description.toLowerCase().contains(lowerTerm)
                        || cmd.usage.toLowerCase().contains(lowerTerm))
            .collect(Collectors.toList());
    }

    private static void initializeSystem() {
        logger.info("Initializing Interactive Distributed Storage System Test...");
        
        system = ActorSystem.create("Interactive-Storage-System");
        nodeRegistry = new TreeMap<>();
        clients = new ArrayList<>();
        crashedNodes = new ArrayList<>();
        
        // Get the replication factor from DataStoreManager
        DataStoreManager dsManager = DataStoreManager.getInstance();
        int initialNodes = dsManager.N; // Ensure at least N nodes to respect replication constraint
        
        logger.info("Creating {} initial nodes to respect replication factor (N={})", initialNodes, dsManager.N);
        
        // Create initial nodes (at least N nodes to satisfy replication constraint)
        for (int i = 1; i <= initialNodes; i++) {
            int nodeId = i * 10;
            ActorRef node = system.actorOf(StorageNode.props(nodeId), "node-" + nodeId);
            nodeRegistry.put(nodeId, node);
            nextNodeId = Math.max(nextNodeId, nodeId + 10);
        }
        
        // Initialize node registries
        for (ActorRef node : nodeRegistry.values()) {
            node.tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());
        }
        
        // Create initial client and properly set nextClientId
        ActorRef client = system.actorOf(Client.props(), "client-1");
        clients.add(client);
        nextClientId = 2; // Set to 2 since we just created client-1
        
        // Wait for initialization
        sleepForOperation(OperationType.CRASH);
        
        // Validate that we have enough nodes to satisfy replication constraint
        if (nodeRegistry.size() < dsManager.N) {
            logger.error("System initialization failed: Created {} nodes but need at least {} nodes for replication factor N={}", 
                        nodeRegistry.size(), dsManager.N, dsManager.N);
            throw new IllegalStateException("Insufficient nodes for replication constraint");
        }
        
        logger.info("System initialized with {} nodes and {} client (N={}, W={}, R={})", 
                   nodeRegistry.size(), clients.size(), dsManager.N, dsManager.W, dsManager.R);
    }

    private static void displayHelp() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("AVAILABLE COMMANDS");
        System.out.println("=".repeat(60));
        
        if (FzfIntegration.isFzfAvailable()) {
            System.out.println("\n🔍 ENHANCED FEATURES:");
            System.out.println("  ?                    - Interactive command search with fzf");
            System.out.println("  search <term>        - Fuzzy search commands");
        }
        
        System.out.println("\n📊 SYSTEM INFORMATION:");
        System.out.println("  help, h              - Show this help message");
        System.out.println("  status, s            - Show system status");
        System.out.println("  nodes                - List all nodes and their states");
        System.out.println("  clients              - List all clients");
        System.out.println("  data                 - Show data distribution across nodes");
        
        System.out.println("\n📝 BASIC OPERATIONS:");
        System.out.println("  get <key> [nodeId] [clientId]         - Get value for key");
        System.out.println("  put <key> <value> [nodeId] [clientId] - Store key-value pair");
        
        System.out.println("\n🖥️  NODE MANAGEMENT:");
        System.out.println("  addnode [nodeId]     - Add a new node to the system");
        System.out.println("  removenode <nodeId>  - Remove a node from the system");
        System.out.println("  crash <nodeId>       - Crash a specific node");
        System.out.println("  recover <nodeId> [peerNodeId] - Recover a crashed node");
        System.out.println("  join <nodeId> <peerNodeId>    - Join node to system via peer");
        System.out.println("  leave <nodeId>       - Make node leave gracefully");
        
        System.out.println("\n👥 CLIENT MANAGEMENT:");
        System.out.println("  addclient            - Add a new client");
        System.out.println("  removeclient <id>    - Remove a client");
        
        System.out.println("\n🧪 TESTING SCENARIOS:");
        System.out.println("  test basic           - Run basic CRUD operations");
        System.out.println("  test quorum          - Test quorum behavior with failures");
        System.out.println("  test concurrency     - Test concurrent operations");
        System.out.println("  test consistency     - Test data consistency");
        System.out.println("  test membership      - Test node join/leave scenarios");
        System.out.println("  test partition       - Test network partition scenarios");
        System.out.println("  test recovery        - Test crash recovery scenarios");
        System.out.println("  test edge-cases      - Test edge cases and error scenarios");
        
        System.out.println("\n⚡ PERFORMANCE TESTING:");
        System.out.println("  benchmark <ops>      - Run performance benchmark");
        System.out.println("  stress <duration>    - Run stress test for duration (seconds)");
        
        System.out.println("\n🔧 SYSTEM CONTROL:");
        System.out.println("  clear                - Clear screen");
        System.out.println("  reset                - Reset system to initial state");
        System.out.println("  quit, exit, q        - Exit the program");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💡 Examples:");
        System.out.println("  get 42 10 1          - Get key 42 using node 10 and client 1");
        System.out.println("  put 42 hello         - Store 'hello' at key 42 using random node/client");
        System.out.println("  crash 20             - Crash node 20");
        System.out.println("  test basic           - Run basic functionality test");
        System.out.println("=".repeat(60) + "\n");
    }

    private static void displayStatus() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("SYSTEM STATUS");
        System.out.println("=".repeat(50));
        
        int activeNodes = nodeRegistry.size() - crashedNodes.size();
        System.out.println("📊 Overview:");
        System.out.println("  Total nodes: " + nodeRegistry.size());
        System.out.println("  Active nodes: " + activeNodes);
        System.out.println("  Crashed nodes: " + crashedNodes.size());
        System.out.println("  Clients: " + clients.size());
        
        System.out.println("\n⚙️  Configuration:");
        System.out.println("  Max nodes: " + MAX_NODES);
        System.out.println("  Max clients: " + MAX_CLIENTS);
        System.out.println("  Next node ID: " + nextNodeId);
        System.out.println("  Replication factor (N): " + dataStoreManager.N);
        System.out.println("  Write quorum (W): " + dataStoreManager.W);
        System.out.println("  Read quorum (R): " + dataStoreManager.R);
        
        if (!crashedNodes.isEmpty()) {
            System.out.println("\n💥 Crashed nodes: " + crashedNodes);
        }
        
        System.out.println("=".repeat(50) + "\n");
    }

    private static void displayNodes() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE REGISTRY");
        System.out.println("=".repeat(60));
        
        if (nodeRegistry.isEmpty()) {
            System.out.println("No nodes in the system.");
            return;
        }
        
        System.out.printf("%-10s %-15s %-20s%n", "Node ID", "Status", "Actor Reference");
        System.out.println("-".repeat(60));
        
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            Integer nodeId = entry.getKey();
            String status = crashedNodes.contains(nodeId) ? "CRASHED" : "ACTIVE";
            String actorRef = entry.getValue().toString();
            
            System.out.printf("%-10d %-15s %-20s%n", nodeId, status, 
                            actorRef.length() > 20 ? actorRef.substring(0, 17) + "..." : actorRef);
        }
        
        System.out.println("=".repeat(60) + "\n");
    }

    private static void displayClients() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("CLIENT REGISTRY");
        System.out.println("=".repeat(50));
        
        if (clients.isEmpty()) {
            System.out.println("No clients in the system.");
            return;
        }
        
        System.out.printf("%-10s %-30s%n", "Client ID", "Actor Reference");
        System.out.println("-".repeat(50));
        
        for (int i = 0; i < clients.size(); i++) {
            String actorRef = clients.get(i).toString();
            System.out.printf("%-10d %-30s%n", (i + 1), 
                            actorRef.length() > 30 ? actorRef.substring(0, 27) + "..." : actorRef);
        }
        
        System.out.println("=".repeat(50) + "\n");
    }

    private static void displayData() {
        System.out.println("\n📊 Requesting data distribution from all nodes...");
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (!crashedNodes.contains(entry.getKey())) {
                entry.getValue().tell(new Messages.DebugPrintDataStore(), ActorRef.noSender());
            }
        }
        sleepForOperation(OperationType.DEBUG_PRINT);
        System.out.println("Data distribution displayed in logs above.\n");
    }

    private static void handleGet(String[] parts) {
        if (!validateSystemState("get")) return;
        
        if (parts.length < 2) {
            System.out.println("Usage: get <key> [nodeId] [clientId]");
            return;
        }
        
        try {
            int key = Integer.parseInt(parts[1]);
            
            // Add range validation for key
            if (key < 0) {
                System.out.println("❌ Key must be non-negative");
                return;
            }
            
            ActorRef node = parts.length > 2 ? getNodeById(Integer.parseInt(parts[2])) : getRandomActiveNode();
            ActorRef client = parts.length > 3 ? getClientById(Integer.parseInt(parts[3])) : getRandomClient();
            
            // Enhanced validation
            if (parts.length > 2) {
                int nodeId = Integer.parseInt(parts[2]);
                if (!nodeRegistry.containsKey(nodeId)) {
                    System.out.println("❌ Node " + nodeId + " does not exist");
                    return;
                }
                if (crashedNodes.contains(nodeId)) {
                    System.out.println("❌ Node " + nodeId + " is crashed and cannot process requests");
                    return;
                }
            }
            
            if (node == null) {
                System.out.println("❌ No active nodes available or invalid node ID");
                return;
            }
            
            if (client == null) {
                System.out.println("❌ Invalid client ID");
                return;
            }
            
            System.out.println("📖 GET key=" + key + " via node=" + getNodeId(node) + " client=" + getClientId(client));
            node.tell(new Messages.ClientGet(key), client);
            sleepForOperation(OperationType.CLIENT_GET);
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid number format: " + e.getMessage());
        }
    }

    private static void handleUpdate(String[] parts) {
        if (!validateSystemState("update")) return;
        
        if (parts.length < 3) {
            System.out.println("Usage: put <key> <value> [nodeId] [clientId]");
            return;
        }
        
        try {
            int key = Integer.parseInt(parts[1]);
            
            // Add range validation for key
            if (key < 0) {
                System.out.println("❌ Key must be non-negative");
                return;
            }
            
            String value = parts[2];
            
            // Validate value length
            if (value.length() > 1000) {
                System.out.println("❌ Value too long (max: 1000 characters)");
                return;
            }
            
            ActorRef node = parts.length > 3 ? getNodeById(Integer.parseInt(parts[3])) : getRandomActiveNode();
            ActorRef client = parts.length > 4 ? getClientById(Integer.parseInt(parts[4])) : getRandomClient();
            
            // Enhanced validation
            if (parts.length > 3) {
                int nodeId = Integer.parseInt(parts[3]);
                if (!nodeRegistry.containsKey(nodeId)) {
                    System.out.println("❌ Node " + nodeId + " does not exist");
                    return;
                }
                if (crashedNodes.contains(nodeId)) {
                    System.out.println("❌ Node " + nodeId + " is crashed and cannot process requests");
                    return;
                }
            }
            
            if (node == null) {
                System.out.println("❌ No active nodes available or invalid node ID");
                return;
            }
            
            if (client == null) {
                System.out.println("❌ Invalid client ID");
                return;
            }
            
            System.out.println("✏️ PUT key=" + key + " value='" + value + "' via node=" + getNodeId(node) + " client=" + getClientId(client));
            node.tell(new Messages.ClientUpdate(key, value), client);
            sleepForOperation(OperationType.CLIENT_UPDATE);
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid number format");
        }
    }

    private static void handleAddNode(String[] parts) {
        if (system == null) {
            System.out.println("❌ System is not initialized");
            return;
        }
        
        try {
            int nodeId = parts.length > 1 ? Integer.parseInt(parts[1]) : nextNodeId;
            
            // Validate node ID range
            if (nodeId <= 0) {
                System.out.println("❌ Node ID must be positive");
                return;
            }
            
            if (nodeId > 1000) {
                System.out.println("❌ Node ID too large (max: 1000)");
                return;
            }
            
            if (nodeRegistry.containsKey(nodeId)) {
                System.out.println("❌ Node " + nodeId + " already exists");
                return;
            }
            
            // Check system limits
            if (nodeRegistry.size() >= MAX_NODES) {
                System.out.println("❌ Maximum number of nodes (" + MAX_NODES + ") reached");
                return;
            }
            
            // Check if we have enough active nodes for bootstrap
            if (!nodeRegistry.isEmpty() && getRandomActiveNode() == null) {
                System.out.println("❌ No active nodes available for bootstrap");
                return;
            }
            
            ActorRef newNode = system.actorOf(StorageNode.props(nodeId), "node-" + nodeId);
            
            if (nodeRegistry.isEmpty()) {
                nodeRegistry.put(nodeId, newNode);
                newNode.tell(new Messages.UpdateNodeRegistry(nodeRegistry, OperationType.INIT), ActorRef.noSender());
            } else {
                ActorRef bootstrapPeer = getRandomActiveNode();
                System.out.println("🔗 Node " + nodeId + " joining via peer " + getNodeId(bootstrapPeer));
                newNode.tell(new Messages.Join(bootstrapPeer), ActorRef.noSender());
                nodeRegistry.put(nodeId, newNode);
            }
            
            nextNodeId = Math.max(nextNodeId, nodeId + 10);
            sleepForOperation(OperationType.JOIN);
            System.out.println("✅ Node " + nodeId + " added successfully");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID format: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("❌ Failed to create node: " + e.getMessage());
            logger.error("Node creation failed", e);
        }
    }

    private void handleRemoveNode(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: removenode <nodeId>");
            return;
        }
        
        try {
            int nodeId = Integer.parseInt(parts[1]);
            ActorRef node = nodeRegistry.get(nodeId);
            
            if (node == null) {
                System.out.println("❌ Node " + nodeId + " not found");
                return;
            }
            
            if (crashedNodes.contains(nodeId)) {
                System.out.println("❌ Cannot remove crashed node " + nodeId + ". Recover it first or use reset.");
                return;
            }

            if (nodeRegistry.size() <= dataStoreManager.N) {
                System.out.println("❌ Cannot remove node " + nodeId + ". Violates Replication Factor constraint (need at least " + dataStoreManager.N + " nodes)!");
                return;
            }
            
            System.out.println("👋 Node " + nodeId + " leaving gracefully...");
            node.tell(new Messages.Leave(), ActorRef.noSender());
            sleepForOperation(OperationType.LEAVE);
            nodeRegistry.remove(nodeId);
            System.out.println("✅ Node " + nodeId + " removed successfully");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID");
        }
    }

    private static void handleCrash(String[] parts) {
        if (!validateSystemState("crash")) return;
        
        if (parts.length < 2) {
            System.out.println("Usage: crash <nodeId>");
            return;
        }
        
        try {
            int nodeId = Integer.parseInt(parts[1]);
            ActorRef node = nodeRegistry.get(nodeId);
            
            if (node == null) {
                System.out.println("❌ Node " + nodeId + " not found");
                return;
            }
            
            if (crashedNodes.contains(nodeId)) {
                System.out.println("❌ Node " + nodeId + " is already crashed");
                return;
            }
            
            System.out.println("💥 Crashing node " + nodeId + "...");
            node.tell(new Messages.Crash(), ActorRef.noSender());
            crashedNodes.add(nodeId);
            sleepForOperation(OperationType.CRASH);
            System.out.println("✅ Node " + nodeId + " crashed");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID");
        }
    }

    private static void handleRecover(String[] parts) {
        if (!validateSystemState("recover")) return;
        
        if (parts.length < 2) {
            System.out.println("Usage: recover <nodeId> [peerNodeId]");
            return;
        }
        
        try {
            int nodeId = Integer.parseInt(parts[1]);
            ActorRef node = nodeRegistry.get(nodeId);
            
            if (node == null) {
                System.out.println("❌ Node " + nodeId + " not found in registry");
                return;
            }
            
            if (!crashedNodes.contains(nodeId)) {
                System.out.println("❌ Node " + nodeId + " is not crashed (current state: ACTIVE)");
                return;
            }
            
            // Validate peer node if specified
            ActorRef recoveryPeer;
            if (parts.length > 2) {
                int peerNodeId = Integer.parseInt(parts[2]);
                if (!nodeRegistry.containsKey(peerNodeId)) {
                    System.out.println("❌ Peer node " + peerNodeId + " does not exist");
                    return;
                }
                if (crashedNodes.contains(peerNodeId)) {
                    System.out.println("❌ Peer node " + peerNodeId + " is crashed, cannot serve as recovery peer");
                    return;
                }
                recoveryPeer = nodeRegistry.get(peerNodeId);
            } else {
                recoveryPeer = getRandomActiveNode();
            }
            
            if (recoveryPeer == null) {
                System.out.println("❌ No active recovery peer available");
                System.out.println("💡 Try specifying a specific peer node ID");
                return;
            }
            
            System.out.println("🔄 Recovering node " + nodeId + " via peer " + getNodeId(recoveryPeer) + "...");
            node.tell(new Messages.Recovery(recoveryPeer), ActorRef.noSender());
            crashedNodes.remove(Integer.valueOf(nodeId));
            sleepForOperation(OperationType.RECOVERY);
            System.out.println("✅ Node " + nodeId + " recovered");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID format: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("❌ Recovery failed: " + e.getMessage());
            logger.error("Node recovery failed", e);
        }
    }

    private static void handleJoin(String[] parts) {
        if (parts.length < 3) {
            System.out.println("Usage: join <nodeId> <peerNodeId>");
            return;
        }
        
        try {
            int nodeId = Integer.parseInt(parts[1]);
            int peerNodeId = Integer.parseInt(parts[2]);
            
            ActorRef node = nodeRegistry.get(nodeId);
            ActorRef peer = nodeRegistry.get(peerNodeId);
            
            if (node == null) {
                System.out.println("❌ Node " + nodeId + " not found");
                return;
            }
            
            if (peer == null || crashedNodes.contains(peerNodeId)) {
                System.out.println("❌ Peer node " + peerNodeId + " not available");
                return;
            }
            
            System.out.println("🔗 Node " + nodeId + " joining via peer " + peerNodeId + "...");
            node.tell(new Messages.Join(peer), ActorRef.noSender());
            sleepForOperation(OperationType.JOIN);
            System.out.println("✅ Join operation completed");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID");
        }
    }

    private static void handleLeave(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: leave <nodeId>");
            return;
        }
        
        try {
            int nodeId = Integer.parseInt(parts[1]);
            ActorRef node = nodeRegistry.get(nodeId);
            
            if (node == null) {
                System.out.println("❌ Node " + nodeId + " not found");
                return;
            }
            
            if (crashedNodes.contains(nodeId)) {
                System.out.println("❌ Cannot make crashed node leave. Recover it first.");
                return;
            }
            
            System.out.println("👋 Node " + nodeId + " leaving gracefully...");
            node.tell(new Messages.Leave(), ActorRef.noSender());
            sleepForOperation(OperationType.LEAVE);
            System.out.println("✅ Leave operation completed");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid node ID");
        }
    }

    private static void handleAddClient() {
        ActorRef client = system.actorOf(Client.props(), "client-" + nextClientId);
        clients.add(client);
        System.out.println("✅ Client " + nextClientId + " added successfully");
        nextClientId++;
    }

    private static void handleRemoveClient(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: removeclient <clientId>");
            return;
        }
        
        try {
            int clientId = Integer.parseInt(parts[1]);
            
            if (clientId < 1 || clientId > clients.size()) {
                System.out.println("❌ Invalid client ID. Valid range: 1-" + clients.size());
                return;
            }
            
            clients.remove(clientId - 1);
            System.out.println("✅ Client " + clientId + " removed successfully");
            
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid client ID");
        }
    }

    private void handleTestScenario(String[] parts) {
        if (!validateSystemState("test")) return;
        
        if (parts.length < 2) {
            System.out.println("Usage: test <scenario>");
            System.out.println("Available scenarios: basic, quorum, concurrency, consistency, membership, partition, recovery, edge-cases");
            return;
        }
        
        String scenario = parts[1].toLowerCase();
        System.out.println("🧪 Running test scenario: " + scenario);
        
        switch (scenario) {
            case "basic" -> testBasicOperations();
            case "quorum" -> testQuorumBehavior();
            case "concurrency" -> testConcurrency();
            case "consistency" -> testConsistency();
            case "membership" -> testMembership();
            case "partition" -> testPartition();
            case "recovery" -> testRecovery();
            case "edge-cases" -> testEdgeCases();
            default -> System.out.println("❌ Unknown test scenario: " + scenario);
        }
    }

    private static void testBasicOperations() {
        System.out.println("🔧 Testing basic CRUD operations...");
        
        // Store some data
        ActorRef node = getRandomActiveNode();
        ActorRef client = getRandomClient();
        
        if (node == null || client == null) {
            System.out.println("❌ No active nodes or clients available");
            return;
        }
        
        System.out.println("📝 Storing test data...");
        node.tell(new Messages.ClientUpdate(100, "TestValue1"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        node.tell(new Messages.ClientUpdate(101, "TestValue2"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        System.out.println("📖 Reading test data...");
        node.tell(new Messages.ClientGet(100), client);
        sleepForOperation(OperationType.CLIENT_GET);
        
        node.tell(new Messages.ClientGet(101), client);
        sleepForOperation(OperationType.CLIENT_GET);
        
        node.tell(new Messages.ClientGet(999), client); // Non-existent key
        sleepForOperation(OperationType.CLIENT_GET);
        
        System.out.println("✅ Basic operations test completed");
    }

    private static void testQuorumBehavior() {
        System.out.println("🔧 Testing quorum behavior with node failures...");
        
        if (nodeRegistry.size() < 3) {
            System.out.println("❌ Need at least 3 nodes for quorum testing");
            return;
        }
        
        // Store data first
        ActorRef node = getRandomActiveNode();
        ActorRef client = getRandomClient();
        
        System.out.println("📝 Storing data before failures...");
        node.tell(new Messages.ClientUpdate(200, "QuorumTest"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        // Crash some nodes
        List<Integer> nodesToCrash = new ArrayList<>();
        int crashCount = Math.min(2, nodeRegistry.size() / 2);
        
        for (Integer nodeId : nodeRegistry.keySet()) {
            if (nodesToCrash.size() >= crashCount) break;
            if (!crashedNodes.contains(nodeId)) {
                nodesToCrash.add(nodeId);
            }
        }
        
        System.out.println("💥 Crashing " + crashCount + " nodes...");
        for (Integer nodeId : nodesToCrash) {
            nodeRegistry.get(nodeId).tell(new Messages.Crash(), ActorRef.noSender());
            crashedNodes.add(nodeId);
        }
        sleepForOperation(OperationType.CRASH);
        
        // Try operations with reduced nodes
        ActorRef activeNode = getRandomActiveNode();
        if (activeNode != null) {
            System.out.println("📖 Testing read with reduced quorum...");
            activeNode.tell(new Messages.ClientGet(200), client);
            sleepForOperation(OperationType.CLIENT_GET);
            
            System.out.println("✏️ Testing write with reduced quorum...");
            activeNode.tell(new Messages.ClientUpdate(201, "QuorumTest2"), client);
            sleepForOperation(OperationType.CLIENT_UPDATE);
        }
        
        System.out.println("✅ Quorum behavior test completed");
    }

    private static void testConcurrency() {
        System.out.println("🔧 Testing concurrent operations...");
        
        if (clients.size() < 2) {
            System.out.println("Adding additional client for concurrency test...");
            handleAddClient();
        }
        
        ActorRef node1 = getRandomActiveNode();
        ActorRef node2 = getRandomActiveNode();
        ActorRef client1 = clients.get(0);
        ActorRef client2 = clients.size() > 1 ? clients.get(1) : client1;
        
        if (node1 == null || node2 == null) {
            System.out.println("❌ Need at least 2 active nodes for concurrency testing");
            return;
        }
        
        System.out.println("⚡ Concurrent writes to same key...");
        node1.tell(new Messages.ClientUpdate(300, "ConcurrentValue1"), client1);
        node2.tell(new Messages.ClientUpdate(300, "ConcurrentValue2"), client2);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        System.out.println("📖 Reading after concurrent writes...");
        node1.tell(new Messages.ClientGet(300), client1);
        sleepForOperation(OperationType.CLIENT_GET);
        
        System.out.println("✅ Concurrency test completed");
    }

    private static void testConsistency() {
        System.out.println("🔧 Testing data consistency across nodes...");
        
        ActorRef client = getRandomClient();
        
        // Write data via one node
        ActorRef writeNode = getRandomActiveNode();
        if (writeNode == null) {
            System.out.println("❌ No active nodes available");
            return;
        }
        
        System.out.println("📝 Writing data via node " + getNodeId(writeNode) + "...");
        writeNode.tell(new Messages.ClientUpdate(400, "ConsistencyTest"), client);
        sleepForOperation(OperationType.CLIENT_UPDATE);
        
        // Read from all active nodes
        System.out.println("📖 Reading from all active nodes...");
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (!crashedNodes.contains(entry.getKey())) {
                System.out.println("Reading from node " + entry.getKey() + "...");
                entry.getValue().tell(new Messages.ClientGet(400), client);
                sleepForOperation(OperationType.CLIENT_GET);
            }
        }
        
        System.out.println("✅ Consistency test completed");
    }

    private void testMembership() {
        System.out.println("🔧 Testing membership operations...");
        
        // Add a new node
        int newNodeId = nextNodeId;
        System.out.println("➕ Adding new node " + newNodeId + "...");
        handleAddNode(new String[]{"addnode", String.valueOf(newNodeId)});
        
        // Perform operations with new topology
        ActorRef client = getRandomClient();
        ActorRef newNode = nodeRegistry.get(newNodeId);
        
        if (newNode != null) {
            System.out.println("📝 Testing operations with new node...");
            newNode.tell(new Messages.ClientUpdate(500, "MembershipTest"), client);
            sleepForOperation(OperationType.CLIENT_UPDATE);
            
            newNode.tell(new Messages.ClientGet(500), client);
            sleepForOperation(OperationType.CLIENT_GET);
        }
        
        // Remove the node
        System.out.println("➖ Removing node " + newNodeId + "...");
        handleRemoveNode(new String[]{"removenode", String.valueOf(newNodeId)});
        
        System.out.println("✅ Membership test completed");
    }

    private static void testPartition() {
        System.out.println("🔧 Simulating network partition (crash half the nodes)...");
        
        if (nodeRegistry.size() < 4) {
            System.out.println("❌ Need at least 4 nodes for partition testing");
            return;
        }
        
        List<Integer> nodesToPartition = new ArrayList<>();
        int partitionSize = nodeRegistry.size() / 2;
        
        for (Integer nodeId : nodeRegistry.keySet()) {
            if (nodesToPartition.size() >= partitionSize) break;
            if (!crashedNodes.contains(nodeId)) {
                nodesToPartition.add(nodeId);
            }
        }
        
        System.out.println("💥 Creating partition by crashing " + partitionSize + " nodes...");
        for (Integer nodeId : nodesToPartition) {
            nodeRegistry.get(nodeId).tell(new Messages.Crash(), ActorRef.noSender());
            crashedNodes.add(nodeId);
        }
        sleepForOperation(OperationType.CRASH);
        
        // Test operations on remaining partition
        ActorRef activeNode = getRandomActiveNode();
        ActorRef client = getRandomClient();
        
        if (activeNode != null) {
            System.out.println("📝 Testing operations on remaining partition...");
            activeNode.tell(new Messages.ClientUpdate(600, "PartitionTest"), client);
            sleepForOperation(OperationType.CLIENT_UPDATE);
            
            activeNode.tell(new Messages.ClientGet(600), client);
            sleepForOperation(OperationType.CLIENT_GET);
        }
        
        System.out.println("✅ Partition test completed");
    }

    private static void testRecovery() {
        System.out.println("🔧 Testing crash recovery scenarios...");
        
        if (crashedNodes.isEmpty()) {
            // Crash a node first
            ActorRef nodeToCrash = getRandomActiveNode();
            if (nodeToCrash != null) {
                int nodeId = getNodeId(nodeToCrash);
                System.out.println("💥 Crashing node " + nodeId + " for recovery test...");
                nodeToCrash.tell(new Messages.Crash(), ActorRef.noSender());
                crashedNodes.add(nodeId);
                sleepForOperation(OperationType.CRASH);
            }
        }
        
        if (!crashedNodes.isEmpty()) {
            Integer crashedNodeId = crashedNodes.get(0);
            ActorRef recoveryPeer = getRandomActiveNode();
            
            if (recoveryPeer != null) {
                System.out.println("🔄 Recovering node " + crashedNodeId + "...");
                handleRecover(new String[]{"recover", String.valueOf(crashedNodeId), String.valueOf(getNodeId(recoveryPeer))});
                
                // Test operations after recovery
                ActorRef recoveredNode = nodeRegistry.get(crashedNodeId);
                ActorRef client = getRandomClient();
                
                System.out.println("📝 Testing operations after recovery...");
                recoveredNode.tell(new Messages.ClientUpdate(700, "RecoveryTest"), client);
                sleepForOperation(OperationType.CLIENT_UPDATE);
                
                recoveredNode.tell(new Messages.ClientGet(700), client);
                sleepForOperation(OperationType.CLIENT_GET);
            }
        }
        
        System.out.println("✅ Recovery test completed");
    }

    private static void testEdgeCases() {
        System.out.println("🔧 Testing edge cases and error scenarios...");
        
        ActorRef client = getRandomClient();
        
        // Test operations on non-existent keys
        System.out.println("📖 Testing read of non-existent key...");
        ActorRef node = getRandomActiveNode();
        if (node != null) {
            node.tell(new Messages.ClientGet(99999), client);
            sleepForOperation(OperationType.CLIENT_GET);
        }
        
        // Test operations with crashed coordinator
        if (!crashedNodes.isEmpty()) {
            Integer crashedNodeId = crashedNodes.get(0);
            ActorRef crashedNode = nodeRegistry.get(crashedNodeId);
            
            System.out.println("📝 Testing operation on crashed coordinator...");
            crashedNode.tell(new Messages.ClientUpdate(800, "ErrorTest"), client);
            sleepForOperation(OperationType.CLIENT_UPDATE);
        }
        
        // Test double join scenario
        if (nodeRegistry.size() >= 2) {
            List<Integer> activeNodeIds = new ArrayList<>();
            for (Integer nodeId : nodeRegistry.keySet()) {
                if (!crashedNodes.contains(nodeId)) {
                    activeNodeIds.add(nodeId);
                }
            }
            
            if (activeNodeIds.size() >= 2) {
                System.out.println("🔗 Testing double join scenario...");
                ActorRef node1 = nodeRegistry.get(activeNodeIds.get(0));
                ActorRef node2 = nodeRegistry.get(activeNodeIds.get(1));
                node1.tell(new Messages.Join(node2), ActorRef.noSender());
                sleepForOperation(OperationType.JOIN);
            }
        }
        
        System.out.println("✅ Edge cases test completed");
    }

    private static void handleBenchmark(String[] parts) {
        if (!validateSystemState("benchmark")) return;
        
        int numOperations;
        try {
            numOperations = parts.length > 1 ? Integer.parseInt(parts[1]) : 100;
            if (numOperations <= 0 || numOperations > 10000) {
                System.out.println("❌ Number of operations must be between 1 and 10000");
                return;
            }
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid number format for operations count");
            return;
        }
        
        System.out.println("⚡ Running benchmark with " + numOperations + " operations...");
        
        ActorRef node = getRandomActiveNode();
        ActorRef client = getRandomClient();
        
        if (node == null || client == null) {
            System.out.println("❌ No active nodes or clients available");
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < numOperations; i++) {
            if (Math.random() > 0.3) { // 70% writes, 30% reads
                node.tell(new Messages.ClientUpdate(1000 + i, "BenchmarkValue" + i), client);
            } else {
                node.tell(new Messages.ClientGet(1000 + (int)(Math.random() * i)), client);
            }
            
            // Small delay to avoid overwhelming
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        sleepForOperation(OperationType.CLIENT_UPDATE);
        long endTime = System.currentTimeMillis();
        
        double throughput = (double) numOperations / ((endTime - startTime) / 1000.0);
        System.out.println("✅ Benchmark completed: " + numOperations + " operations in " + 
                          (endTime - startTime) + "ms (Throughput: " + String.format("%.2f", throughput) + " ops/sec)");
    }

    private static void handleStressTest(String[] parts) {
        if (!validateSystemState("stress test")) return;
        
        int duration;
        try {
            duration = parts.length > 1 ? Integer.parseInt(parts[1]) : 30;
            if (duration <= 0 || duration > 300) {
                System.out.println("❌ Duration must be between 1 and 300 seconds");
                return;
            }
        } catch (NumberFormatException e) {
            System.out.println("❌ Invalid duration format");
            return;
        }
        
        System.out.println("⚡ Running stress test for " + duration + " seconds...");
        
        long endTime = System.currentTimeMillis() + (duration * 1000L);
        int operationCount = 0;
        
        while (System.currentTimeMillis() < endTime) {
            ActorRef node = getRandomActiveNode();
            ActorRef client = getRandomClient();
            
            if (node == null || client == null) {
                System.out.println("❌ No active nodes or clients available");
                break;
            }
            
            int key = (int) (Math.random() * 1000) + 2000;
            
            if (Math.random() > 0.4) {
                node.tell(new Messages.ClientUpdate(key, "StressValue" + operationCount), client);
            } else {
                node.tell(new Messages.ClientGet(key), client);
            }
            
            operationCount++;
            
            try {
                Thread.sleep(50); // 20 ops/sec per thread
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        System.out.println("✅ Stress test completed: " + operationCount + " operations in " + duration + " seconds");
    }

    private static void clearScreen() {
        // Clear screen for most terminals
        try {
            if (System.getProperty("os.name").contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            } else {
                System.out.print("\033[2J\033[H");
                System.out.flush();
            }
        } catch (Exception e) {
            // Fallback: print multiple newlines
            for (int i = 0; i < 50; i++) {
                System.out.println();
            }
        }
    }

    private static void handleReset() {
        System.out.println("🔄 Resetting system to initial state...");
        
        // Clear crashed nodes list
        crashedNodes.clear();
        
        // Stop current system
        if (system != null) {
            system.terminate();
        }
        
        // Reinitialize
        try {
            Thread.sleep(2000); // Wait for shutdown
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        initializeSystem();
        System.out.println("✅ System reset completed");
    }

    // Helper methods
    private static ActorRef getRandomActiveNode() {
        List<ActorRef> activeNodes = new ArrayList<>();
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (!crashedNodes.contains(entry.getKey())) {
                activeNodes.add(entry.getValue());
            }
        }
        
        if (activeNodes.isEmpty()) return null;
        return activeNodes.get((int) (Math.random() * activeNodes.size()));
    }

    private static ActorRef getRandomClient() {
        if (clients.isEmpty()) return null;
        return clients.get((int) (Math.random() * clients.size()));
    }

    private static ActorRef getNodeById(int nodeId) {
        ActorRef node = nodeRegistry.get(nodeId);
        return (!crashedNodes.contains(nodeId)) ? node : null;
    }

    private static ActorRef getClientById(int clientId) {
        if (clientId < 1 || clientId > clients.size()) return null;
        return clients.get(clientId - 1);
    }

    private static Integer getNodeId(ActorRef node) {
        for (Map.Entry<Integer, ActorRef> entry : nodeRegistry.entrySet()) {
            if (entry.getValue().equals(node)) {
                return entry.getKey();
            }
        }
        return -1;
    }

    private static Integer getClientId(ActorRef client) {
        for (int i = 0; i < clients.size(); i++) {
            if (clients.get(i).equals(client)) {
                return i + 1;
            }
        }
        return -1;
    }

    private static void sleepForOperation(OperationType operationType) {
        try {
            Thread.sleep(OperationDelays.getDelayForOperation(operationType));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted during {} delay", operationType);
        }
    }

    private static void cleanup() {
        if (system != null) {
            logger.info("Shutting down system...");
            system.terminate();
        }
    }

    private static boolean validateSystemState(String operation) {
        if (system == null) {
            System.out.println("❌ System is not initialized or has been terminated");
            return false;
        }
        
        if (nodeRegistry.isEmpty()) {
            System.out.println("❌ No nodes in the system");
            return false;
        }
        
        if (clients.isEmpty()) {
            System.out.println("❌ No clients in the system");
            return false;
        }
        
        int activeNodes = nodeRegistry.size() - crashedNodes.size();
        if (activeNodes == 0) {
            System.out.println("❌ No active nodes available for " + operation);
            return false;
        }
        
        // Check if we have enough total nodes for replication factor
        if (nodeRegistry.size() < dataStoreManager.N) {
            System.out.println("❌ Insufficient nodes for replication constraint: have " + nodeRegistry.size() + 
                             ", need at least " + dataStoreManager.N + " (N=" + dataStoreManager.N + ")");
            return false;
        }
        
        // Check quorum requirements for critical operations
        if (operation.contains("update") || operation.contains("put")) {
            if (activeNodes < dataStoreManager.W) {
                System.out.println("⚠️  Warning: Not enough active nodes for reliable write operations (need at least " + 
                                 dataStoreManager.W + " for W=" + dataStoreManager.W + ", have " + activeNodes + ")");
            }
        }
        
        if (operation.contains("get") || operation.contains("read")) {
            if (activeNodes < dataStoreManager.R) {
                System.out.println("⚠️  Warning: Not enough active nodes for reliable read operations (need at least " + 
                                 dataStoreManager.R + " for R=" + dataStoreManager.R + ", have " + activeNodes + ")");
            }
        }
        
        return true;
    }

}