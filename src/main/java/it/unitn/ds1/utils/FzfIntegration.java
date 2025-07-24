package it.unitn.ds1.utils;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Integration with system's fzf for fuzzy finding commands and tab completion
 */
public class FzfIntegration {
    
    private static boolean fzfAvailable = checkFzfAvailability();
    
    /**
     * Check if fzf is available on the system
     */
    private static boolean checkFzfAvailability() {
        try {
            ProcessBuilder pb = new ProcessBuilder("which", "fzf");
            Process process = pb.start();
            return process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Tab completion using fzf for partial commands
     */
    public static String completeCommand(String partialCommand, List<String> availableCompletions) {
        if (!fzfAvailable || availableCompletions.isEmpty()) {
            return fallbackCompletion(partialCommand, availableCompletions);
        }
        
        try {
            // Filter completions that match the partial command
            List<String> filtered = availableCompletions.stream()
                .filter(cmd -> cmd.toLowerCase().startsWith(partialCommand.toLowerCase()) || 
                              cmd.toLowerCase().contains(partialCommand.toLowerCase()))
                .collect(Collectors.toList());
            
            if (filtered.isEmpty()) {
                filtered = availableCompletions; // Show all if no matches
            }
            
            ProcessBuilder pb = new ProcessBuilder("fzf", 
                "--query=" + partialCommand,
                "--prompt=Complete> ",
                "--height=40%",
                "--reverse", 
                "--border",
                "--info=inline",
                "--select-1", // Auto-select if only one match
                "--exit-0"    // Exit if no match
            );
            
            Process process = pb.start();
            
            // Send filtered options to fzf
            try (PrintWriter writer = new PrintWriter(process.getOutputStream())) {
                for (String option : filtered) {
                    writer.println(option);
                }
            }
            
            // Read completion from fzf
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String completion = reader.readLine();
                
                if (process.waitFor(5, TimeUnit.SECONDS)) {
                    return completion;
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error using fzf for completion: " + e.getMessage());
        }
        
        return fallbackCompletion(partialCommand, availableCompletions);
    }
    
    /**
     * Get completions for command arguments (node IDs, client IDs, etc.)
     */
    public static String completeArgument(String context, List<String> arguments) {
        if (!fzfAvailable || arguments.isEmpty()) {
            return fallbackSelection(arguments, "Select " + context);
        }
        
        try {
            ProcessBuilder pb = new ProcessBuilder("fzf", 
                "--prompt=" + context + "> ",
                "--height=30%",
                "--reverse",
                "--border",
                "--info=inline",
                "--preview=echo 'Available: {}'",
                "--preview-window=down:1"
            );
            
            Process process = pb.start();
            
            // Send arguments to fzf
            try (PrintWriter writer = new PrintWriter(process.getOutputStream())) {
                for (String arg : arguments) {
                    writer.println(arg);
                }
            }
            
            // Read selection from fzf
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String selection = reader.readLine();
                
                if (process.waitFor(10, TimeUnit.SECONDS)) {
                    return selection;
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error using fzf for argument completion: " + e.getMessage());
        }
        
        return fallbackSelection(arguments, "Select " + context);
    }
    public static String selectWithFzf(List<String> options, String prompt) {
        if (!fzfAvailable) {
            System.out.println("fzf not available, falling back to simple selection");
            return fallbackSelection(options, prompt);
        }
        
        try {
            ProcessBuilder pb = new ProcessBuilder("fzf", 
                "--prompt=" + prompt + "> ",
                "--height=50%",
                "--reverse",
                "--border",
                "--info=inline"
            );
            
            Process process = pb.start();
            
            // Send options to fzf via stdin
            try (PrintWriter writer = new PrintWriter(process.getOutputStream())) {
                for (String option : options) {
                    writer.println(option);
                }
            }
            
            // Read selection from fzf
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String selection = reader.readLine();
                
                if (process.waitFor(10, TimeUnit.SECONDS)) {
                    return selection;
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error using fzf: " + e.getMessage());
        }
        
        return fallbackSelection(options, prompt);
    }
    
    /**
     * Fallback selection when fzf is not available
     */
    private static String fallbackSelection(List<String> options, String prompt) {
        if (options.isEmpty()) return null;
        
        System.out.println("\n" + prompt);
        for (int i = 0; i < options.size(); i++) {
            System.out.printf("%2d. %s%n", i + 1, options.get(i));
        }
        
        System.out.print("Select (1-" + options.size() + "): ");
        try (java.util.Scanner scanner = new java.util.Scanner(System.in)) {
            int choice = scanner.nextInt();
            if (choice >= 1 && choice <= options.size()) {
                return options.get(choice - 1);
            }
        } catch (Exception e) {
            // Invalid input
        }
        
        return null;
    }
    
    /**
     * Fallback completion when fzf is not available
     */
    private static String fallbackCompletion(String partialCommand, List<String> availableCompletions) {
        List<String> matches = availableCompletions.stream()
            .filter(cmd -> cmd.toLowerCase().startsWith(partialCommand.toLowerCase()))
            .collect(Collectors.toList());
            
        if (matches.size() == 1) {
            return matches.get(0);
        } else if (matches.size() > 1) {
            System.out.println("\nPossible completions:");
            for (String match : matches) {
                System.out.println("  " + match);
            }
            return partialCommand; // Return original if multiple matches
        }
        
        return partialCommand; // No matches found
    }
    
    public static boolean isFzfAvailable() {
        return fzfAvailable;
    }
}