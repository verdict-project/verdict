package org.verdictdb.core.execution;

import java.util.Arrays;
import java.util.List;

public class MethodInvocationInformation {
  
  private String methodName;
  
  private Object[] arguments;
  
  private Class<?>[] parameters;
  
  public MethodInvocationInformation(String methodName, Class<?>[] parameters, Object[] arguments) {
    this.methodName = methodName;
    this.parameters = parameters;
    this.arguments = arguments;
  }
  
  public MethodInvocationInformation(String methodName, Class<?>[] parameters) {
    this(methodName, parameters, new Object[0]);
  }

  public String getMethodName() {
    return methodName;
  }

  public Object[] getArguments() {
    return arguments;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  public void setArguments(Object[] arguments) {
    this.arguments = arguments;
  }

  public Class<?>[] getMethodParameters() {
    return parameters;
  }
  
}