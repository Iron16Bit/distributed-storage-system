package it.unitn.ds1;

public class DataStoreManager {
    private static DataStoreManager instance;
    
    public final int N;
    public final int W;
    public final int R;

    private DataStoreManager(int N, int W, int R) {
        this.N = N;
        this.W = W;
        this.R = R;
    }

    private DataStoreManager(int N) {
        this.N = N;
        this.R = N / 2 + 1;
        this.W = N / 2 + 1;
    }

    /**
     * Initialize the singleton instance with specified N, W, R values
     */
    public static void initialize(int N, int W, int R) {
        if (instance != null) {
            throw new IllegalStateException("DataStoreManager already initialized");
        }
        instance = new DataStoreManager(N, W, R);
    }

    /**
     * Initialize the singleton instance with only N (W and R will be calculated as N/2 + 1)
     */
    public static void initialize(int N) {
        if (instance != null) {
            throw new IllegalStateException("DataStoreManager already initialized");
        }
        instance = new DataStoreManager(N);
    }

    /**
     * Get the singleton instance
     */
    public static DataStoreManager getInstance() {
        if (instance == null) {
            // Default initialization if not explicitly initialized
            instance = new DataStoreManager(10, 7, 6);
        }
        return instance;
    }

    /**
     * Reset the singleton (mainly for testing purposes)
     */
    public static void reset() {
        instance = null;
    }

    @Override
    public String toString() {
        return String.format("DataStoreManager{N=%d, W=%d, R=%d}", N, W, R);
    }
}