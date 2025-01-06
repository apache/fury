package org.apache.fury.serializer.compatible.classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class SubclassMissingField {
  private boolean privateBoolean = true;
  private int privateInt = 10;
  private String privateString = "notNull";
  private Map<String, String> privateMap;
  private List<String> privateList;

  public SubclassMissingField() {
    privateMap = new HashMap<>();
    privateMap.put("a", "b");
    privateList = new ArrayList<>();
    privateList.add("a");
  }
}
