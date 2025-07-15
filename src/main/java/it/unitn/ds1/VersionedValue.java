package it.unitn.ds1;

public class VersionedValue {
    private String value;
    private int version;
    
    public VersionedValue(String value, int version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return value + " (v" + version + ")";
    }
}
