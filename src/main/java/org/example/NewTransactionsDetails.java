package org.example;


import java.util.Arrays;

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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rowId == null) ? 0 : rowId.hashCode());
    result = prime * result + Arrays.hashCode(cf);
    result = prime * result + Arrays.hashCode(cq);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NewTransactionsDetails other = (NewTransactionsDetails) obj;
    if (rowId == null) {
      if (other.rowId != null)
        return false;
    } else if (!rowId.equals(other.rowId))
      return false;
    if (!Arrays.equals(cf, other.cf))
      return false;
    if (!Arrays.equals(cq, other.cq))
      return false;
    return true;
  }

}
