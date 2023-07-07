package io.fury.codegen;

import io.fury.codegen.Code.ExprCode;

public class ExprState {
  private final ExprCode exprCode;
  private int accessCount;

  public ExprState(ExprCode exprCode) {
    this.exprCode = exprCode;
    accessCount = 1;
  }

  public ExprCode getExprCode() {
    return exprCode;
  }

  public void incAccessCount() {
    accessCount++;
  }

  public int getAccessCount() {
    return accessCount;
  }
}
