package it.unitn.ds1.utils;

import it.unitn.ds1.Messages;
import it.unitn.ds1.types.OperationType;

/**
 * Class containing operation-specific delays for different types of operations
 * in the distributed storage system.
 * 
 * These delays represent the time to wait for COMPLETE operations, including
 * all internal message exchanges and quorum responses.
 */
public class OperationDelays {
    
    // Base delay from Messages.DELAY
    private static final int BASE_DELAY = Messages.DELAY;
    
    // Complete client operation delays (end-to-end)
    public static final int CLIENT_GET_DELAY = (int)(BASE_DELAY * 5.0);     // Includes quorum read + response
    public static final int CLIENT_UPDATE_DELAY = (int)(BASE_DELAY * 5.0);  // Includes read-before-write + quorum write
    
    // Node lifecycle operation delays (complete operations)
    public static final int JOIN_DELAY = (int)(BASE_DELAY * 6.5);           // Complete join process with data transfer
    public static final int LEAVE_DELAY = (int)(BASE_DELAY * 3.5);          // Complete leave process with data redistribution
    public static final int CRASH_DELAY = BASE_DELAY;                       // Immediate crash
    public static final int RECOVERY_DELAY = (int)(BASE_DELAY * 4.5);       // Complete recovery with data sync
    
    // System operation delays
    public static final int DEBUG_PRINT_DELAY = 500;                        // Fixed delay for debug output
    
    /**
     * Get delay for a specific operation type
     */
    public static int getDelayForOperation(OperationType operationType) {
        return switch (operationType) {
            case CLIENT_GET -> CLIENT_GET_DELAY;
            case CLIENT_UPDATE -> CLIENT_UPDATE_DELAY;
            case JOIN -> JOIN_DELAY;
            case LEAVE -> LEAVE_DELAY;
            case CRASH -> CRASH_DELAY;
            case RECOVERY -> RECOVERY_DELAY;
            case DEBUG_PRINT -> DEBUG_PRINT_DELAY;
            case INIT -> BASE_DELAY;
        };
    }
}