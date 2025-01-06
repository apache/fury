package org.apache.fury.serializer.compatible.classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class SubclassCompleteField {
  private boolean privateBoolean = true;
  private int privateInt = 10;
  private String privateString = "notNull";
  private Map<String, String> privateMap;
  private List<String> privateList;

  private boolean privateBoolean2 = true;
  private int privateInt2 = 10;
  private String privateString2 = "notNull";
  private Map<String, String> privateMap2;
  private List<String> privateList2;

  public SubclassCompleteField() {
    privateMap = new HashMap<>();
    privateMap.put("a", "b");
    privateList = new ArrayList<>();
    privateList.add("a");

    privateMap2 = new HashMap<>();
    privateMap2.put("a", "b");
    privateList2 = new ArrayList<>();
    privateList2.add("a");
  }
}
