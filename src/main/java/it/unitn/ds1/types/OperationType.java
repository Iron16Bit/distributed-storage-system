package it.unitn.ds1.types;

/**
 * Enum defining different operation types
 */
public enum OperationType {
    CLIENT_GET,        // Complete client read operation (quorum read)
    CLIENT_UPDATE,     // Complete client write operation (read-before-write + quorum write)
    JOIN,             // Complete node join operation
    LEAVE,            // Complete node leave operation
    CRASH,            // Node crash
    RECOVERY,         // Complete node recovery operation
    DEBUG_PRINT,      // Debug output delay
    INIT,             // Initial Join 
}