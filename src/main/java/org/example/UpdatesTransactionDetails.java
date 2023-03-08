package org.example;

import java.util.Arrays;

public class UpdatesTransactionDetails {
  protected String tableName;
  protected String rowId;
  protected byte[] columnFamily;
  protected byte[] columnQuantifier;

  protected byte[] value;



  public UpdatesTransactionDetails(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
    this.tableName = tableName;
    this.rowId = rowId;
    this.columnFamily = columnFamily;
    this.columnQuantifier = columnQuantifier;
    this.value = value;
  }




  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    result = prime * result + ((rowId == null) ? 0 : rowId.hashCode());
    result = prime * result + Arrays.hashCode(columnFamily);
    result = prime * result + Arrays.hashCode(columnQuantifier);
    return result;
  }



//  @Override
//  public boolean equals(Object obj) {
//    if (this == obj)
//      return true;
//    if (obj == null)
//      return false;
//    if (getClass() != obj.getClass())
//      return false;
//    UpdatesTransactionDetails other = (UpdatesTransactionDetails) obj;
//    if (tableName == null) {
//      if (other.tableName != null)
//        return false;
//    } else if (!tableName.equals(other.tableName))
//      return false;
//    if (rowId == null) {
//      if (other.rowId != null)
//        return false;
//    } else if (!rowId.equals(other.rowId))
//      return false;
//    if (!Arrays.equals(columnFamily, other.columnFamily))
//      return false;
//    if (!Arrays.equals(columnQuantifier, other.columnQuantifier))
//      return false;
//    return true;
//  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    UpdatesTransactionDetails other = (UpdatesTransactionDetails) obj;

    return tableName.equals(other.tableName) &&
            rowId.equals(other.rowId) &&
            Arrays.equals(columnFamily, other.columnFamily) &&
            Arrays.equals(columnQuantifier, other.columnQuantifier);
  }





}
