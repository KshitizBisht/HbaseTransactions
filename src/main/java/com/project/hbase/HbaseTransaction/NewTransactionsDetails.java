package com.project.hbase.HbaseTransaction;

public class NewTransactionsDetails {
    protected String rowId;
    protected byte[] cf;
    protected byte[] cq;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public byte[] getCf() {
        return cf;
    }

    public void setCf(byte[] cf) {
        this.cf = cf;
    }

    public byte[] getCq() {
        return cq;
    }

    public void setCq(byte[] cq) {
        this.cq = cq;
    }


    public NewTransactionsDetails(String rowId, byte[] cf, byte[] cq){
        this.rowId = rowId;
        this.cf = cf;
        this.cq = cq;
    }
    
}
