package org.example;

import org.apache.hadoop.hbase.client.Put;

public class MapStage {
  Put put;

  public MapStage(Put put) {
    this.put = put;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((put == null) ? 0 : put.hashCode());
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
    MapStage other = (MapStage) obj;
    if (put == null) {
      if (other.put != null)
        return false;
    } else if (!put.equals(other.put))
      return false;
    return true;
  }



}
