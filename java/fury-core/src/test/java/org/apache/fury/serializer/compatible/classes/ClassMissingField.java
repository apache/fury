package org.apache.fury.serializer.compatible.classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@Getter
public class ClassMissingField<T> {
  private T privateFieldSubClass;
  private List<String> privateList;
  private Map<String, String> privateMap;
  private String privateString = "missing";
  private int privateInt = 999;
  private boolean privateBoolean = false;

  public ClassMissingField(T privateFieldSubClass) {
    privateMap = new HashMap<>();
    privateMap.put("z", "x");
    privateList = new ArrayList<>();
    privateList.add("c");
    this.privateFieldSubClass = privateFieldSubClass;
  }
}
