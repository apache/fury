/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.codegen;

import static io.fury.codegen.Code.ExprCode;
import static io.fury.codegen.CodeGenerator.alignIndent;
import static io.fury.codegen.CodeGenerator.indent;
import static io.fury.type.TypeUtils.getArrayType;
import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import io.fury.codegen.Expression.Reference;
import io.fury.collection.Tuple2;
import io.fury.collection.Tuple3;
import io.fury.util.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CodegenContext can be an any scope in a class, such as class, method, local and so on.
 *
 * <p>All constructor of generated class will call {@code initialize()} to initialize object. We
 * don't use instance initialize, so user can add init code which depends on used-passed
 * constructor's args.
 *
 * @author chaokunyang
 */
public class CodegenContext {
  public static Set<String> JAVA_RESERVED_WORDS;

  static {
    JAVA_RESERVED_WORDS = new HashSet<>();
    JAVA_RESERVED_WORDS.addAll(
        Arrays.asList(
            "abstract",
            "assert",
            "boolean",
            "break",
            "byte",
            "case",
            "catch",
            "char",
            "class",
            "const",
            "continue",
            "default",
            "do",
            "double",
            "else",
            "enum",
            "extends",
            "final",
            "finally",
            "float",
            "for",
            "goto",
            "if",
            "implements",
            "import",
            "instanceof",
            "int",
            "interface",
            "long",
            "native",
            "new",
            "package",
            "private",
            "protected",
            "public",
            "return",
            "short",
            "static",
            "strictfp",
            "super",
            "switch",
            "synchronized",
            "this",
            "throw",
            "throws",
            "transient",
            "try",
            "void",
            "volatile",
            "while",
            "true",
            "false",
            "null"));
    JAVA_RESERVED_WORDS = ImmutableSet.copyOf(JAVA_RESERVED_WORDS);
  }

  Map<String, Long> newValNameIds = new HashMap<>();
  Set<String> valNames = new HashSet<>();

  /**
   * State used for expression elimination/reuse.
   *
   * <p>Takes the first expression and requests it to generate a Java source code for the expression
   * tree
   *
   * <p>The exprCode's code of subsequent same expression will be null, because the code is already
   * added to current context
   */
  Map<Expression, ExprState> exprState = new HashMap<>();

  String pkg;
  LinkedHashSet<String> imports = new LinkedHashSet<>();
  String className;
  String[] superClasses;
  String[] interfaces;
  List<Tuple3<Boolean, String, String>> fields = new ArrayList<>();
  /**
   * all initCodes would be placed into a method called initialize(), which will be called by
   * constructor.
   */
  List<String> initCodes = new ArrayList<>();

  List<String> constructors = new ArrayList<>();
  LinkedHashMap<String, String> methods = new LinkedHashMap<>();

  private CodegenContext instanceInitCtx;

  public CodegenContext() {}

  public CodegenContext(LinkedHashSet<String> imports) {
    this.imports = imports;
  }

  public CodegenContext(Set<String> valNames, LinkedHashSet<String> imports) {
    this.valNames = valNames;
    this.imports = imports;
  }

  /**
   * Reserve name to avoid name collision for name that not created with {@link
   * CodegenContext#newName(String)}.
   */
  public void reserveName(String name) {
    Preconditions.checkArgument(!valNames.contains(name));
    String s = newName(name);
    Preconditions.checkArgument(s.equals(name));
  }

  public boolean containName(String name) {
    return valNames.contains(name);
  }

  /**
   * If name is a java reserved word, return as if called with name "value".
   *
   * <p>Since we don't pass in TypeToken, no need to consider generics
   */
  public String newName(Class<?> clz) {
    return newName(namePrefix(clz));
  }

  public String newName(Class<?> clz, String suffix) {
    String name = newName(clz);
    return newName(name + suffix);
  }

  /** Returns a term name that is unique within this instance of a `CodegenContext`. */
  public String newName(String name) {
    newValNameIds.putIfAbsent(name, 0L);
    long id = newValNameIds.get(name);
    newValNameIds.put(name, id + 1);
    if (id == 0) {
      if (valNames.add(name)) {
        return name;
      }
    }
    String newName = String.format("%s%s", name, id);
    while (valNames.contains(newName)) {
      id++;
      newValNameIds.put(name, id);
      newName = String.format("%s%s", name, id);
    }
    valNames.add(newName);
    return newName;
  }

  /** Returns two term names that have same suffix to get more readability for generated code. */
  public String[] newNames(Class<?> clz1, String name2) {
    if (clz1.isArray()) {
      return newNames("arr", name2);
    } else {
      String type = type(clz1);
      int index = type.lastIndexOf(".");
      String name;
      if (index >= 0) {
        name = StringUtils.uncapitalize(type.substring(index + 1));
      } else {
        name = StringUtils.uncapitalize(type);
      }
      if (JAVA_RESERVED_WORDS.contains(name)) {
        return newNames("value", name2);
      } else {
        return newNames(name, name2);
      }
    }
  }

  /**
   * Try to return term names that have same suffixes to get more readability for generated code.
   */
  public String[] newNames(String... names) {
    long id = 0;
    for (String name : names) {
      id = Math.max(id, newValNameIds.getOrDefault(name, 0L));
    }
    for (String name : names) {
      newValNameIds.put(name, id + 1);
    }
    if (id == 0 && Sets.intersection(valNames, Sets.newHashSet(names)).isEmpty()) {
      valNames.addAll(Arrays.asList(names));
      return names;
    } else {
      String[] newNames = new String[names.length];
      for (int i = 0; i < names.length; i++) {
        newNames[i] = String.format("%s%s", names[i], id);
        while (valNames.contains(newNames[i])) {
          id++;
          newValNameIds.put(newNames[i], id);
          newNames[i] = String.format("%s%s", names[i], id);
        }
      }
      valNames.addAll(Arrays.asList(newNames));
      return newNames;
    }
  }

  public String namePrefix(Class<?> clz) {
    if (clz.isArray()) {
      return "arr";
    } else {
      String type = type(clz);
      int index = type.lastIndexOf(".");
      String name;
      if (index >= 0) {
        name = StringUtils.uncapitalize(type.substring(index + 1));
      } else {
        name = StringUtils.uncapitalize(type);
      }

      if (JAVA_RESERVED_WORDS.contains(name)) {
        return "value";
      } else {
        return name;
      }
    }
  }

  /**
   * Get type string.
   *
   * @param clz type
   * @return simple name for class if type's canonical name starts with java.lang or is imported,
   *     return canonical name otherwise.
   */
  public String type(Class<?> clz) {
    if (clz.isArray()) {
      return getArrayType(clz);
    }
    String type = clz.getCanonicalName();
    if (type.startsWith("java.lang")) {
      if (!type.substring("java.lang.".length()).contains(".")) {
        return clz.getSimpleName();
      }
    }
    if (imports.contains(type)) {
      return clz.getSimpleName();
    } else {
      int index = type.lastIndexOf(".");
      if (index > 0) {
        // This might be package name or qualified name of outer class
        String pkgOrClassName = type.substring(0, index);
        if (imports.contains(pkgOrClassName + ".*")) {
          return clz.getSimpleName();
        }
      }
      return type;
    }
  }

  /** return type name. since janino doesn't generics, we ignore type parameters in typeToken. */
  public String type(TypeToken<?> typeToken) {
    return type(getRawType(typeToken));
  }

  /**
   * Set the generated class's package.
   *
   * @param pkg java package
   */
  public void setPackage(String pkg) {
    this.pkg = pkg;
  }

  public Set<String> getValNames() {
    return valNames;
  }

  public LinkedHashSet<String> getImports() {
    return imports;
  }

  /**
   * Import classes.
   *
   * @param classes classes to be imported
   */
  public void addImports(Class<?>... classes) {
    for (Class<?> clz : classes) {
      imports.add(clz.getCanonicalName());
    }
  }

  /**
   * Add imports.
   *
   * @param imports import statements
   */
  public void addImports(String... imports) {
    this.imports.addAll(Arrays.asList(imports));
  }

  /**
   * Import class.
   *
   * <p>Import class carefully, otherwise class will conflict. Only java.lang.Class is unique and
   * won't conflict.
   *
   * @param cls class to be imported
   */
  public void addImport(Class<?> cls) {
    this.imports.add(cls.getCanonicalName());
  }

  /**
   * Add import.
   *
   * <p>Import class carefully, otherwise class will conflict. Only java.lang.Class is unique and
   * won't conflict.
   *
   * @param im import statement
   */
  public void addImport(String im) {
    this.imports.add(im);
  }

  /**
   * Set class name of class to be generated.
   *
   * @param className class name of class to be generated
   */
  public void setClassName(String className) {
    this.className = className;
  }

  /**
   * Set super classes.
   *
   * @param superClasses super classes
   */
  public void extendsClasses(String... superClasses) {
    this.superClasses = superClasses;
  }

  /**
   * Set implemented interfaces.
   *
   * @param interfaces implemented interfaces
   */
  public void implementsInterfaces(String... interfaces) {
    this.interfaces = interfaces;
  }

  public void addConstructor(String codeBody, Object... params) {
    List<Tuple2<String, String>> parameters = getParameters(params);
    String paramsStr =
        parameters.stream().map(t -> t.f0 + " " + t.f1).collect(Collectors.joining(", "));

    StringBuilder codeBuilder = new StringBuilder(alignIndent(codeBody)).append("\n");
    for (String init : initCodes) {
      codeBuilder.append(indent(init, 4)).append('\n');
    }
    String constructor =
        StringUtils.format(
            "" + "public ${className}(${paramsStr}) {\n" + "    ${codeBody}" + "}",
            "className",
            className,
            "paramsStr",
            paramsStr,
            "codeBody",
            codeBuilder);
    constructors.add(constructor);
  }

  public void addInitCode(String code) {
    initCodes.add(code);
  }

  public void addStaticMethod(
      String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("public static", methodName, codeBody, returnType, params);
  }

  public void addMethod(String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("public", methodName, codeBody, returnType, params);
  }

  public void addMethod(
      String modifier, String methodName, String codeBody, Class<?> returnType, Object... params) {
    List<Tuple2<String, String>> parameters = getParameters(params);
    String paramsStr =
        parameters.stream().map(t -> t.f0 + " " + t.f1).collect(Collectors.joining(", "));
    String method =
        StringUtils.format(
            ""
                + "${modifier} ${returnType} ${methodName}(${paramsStr}) {\n"
                + "    ${codeBody}\n"
                + "}\n",
            "modifier",
            modifier,
            "returnType",
            type(returnType),
            "methodName",
            methodName,
            "paramsStr",
            paramsStr,
            "codeBody",
            alignIndent(codeBody));
    String signature = String.format("%s(%s)", methodName, paramsStr);
    if (methods.containsKey(signature)) {
      throw new IllegalStateException(String.format("Duplicated method signature: %s", signature));
    }
    methods.put(signature, method);
  }

  public void overrideMethod(
      String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("@Override public final", methodName, codeBody, returnType, params);
  }

  /**
   * Get parameters.
   *
   * @param args type, value; type, value; type, value; ......
   */
  private List<Tuple2<String, String>> getParameters(Object... args) {
    Preconditions.checkArgument(args.length % 2 == 0);
    List<Tuple2<String, String>> params = new ArrayList<>(0);
    for (int i = 0; i < args.length; i += 2) {
      String type;
      if (args[i] instanceof Class) {
        type = type(((Class<?>) args[i]));
      } else {
        type = args[i].toString();
      }
      params.add(Tuple2.of(type, args[i + 1].toString()));
    }
    return params;
  }

  /** Returns true if class has field with name {@code fieldName}. */
  public boolean hasField(String fieldName) {
    for (Tuple3<Boolean, String, String> field : fields) {
      if (fieldName.equals(field.f2)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Add a field to class.
   *
   * @param type type
   * @param fieldName field name
   * @param initExpr field init expression
   */
  public void addField(Class<?> type, String fieldName, Expression initExpr) {
    addField(type(type), fieldName, initExpr);
  }

  public void addField(String type, String fieldName, Expression initExpr) {
    addField(type, fieldName, initExpr, true);
  }

  /**
   * Add a field to class.
   *
   * @param type type
   * @param fieldName field name
   * @param initExpr field init expression
   */
  public void addField(String type, String fieldName, Expression initExpr, boolean isFinalField) {
    if (instanceInitCtx == null) {
      instanceInitCtx = new CodegenContext(valNames, imports);
    }
    fields.add(Tuple3.of(isFinalField, type, fieldName));
    ExprCode exprCode = initExpr.genCode(instanceInitCtx);
    if (StringUtils.isNotBlank(exprCode.code())) {
      initCodes.add(exprCode.code());
    }
    initCodes.add(String.format("%s = %s;", fieldName, exprCode.value()));
  }

  /**
   * Add a field to class.
   *
   * @param type type
   * @param fieldName field name
   * @param initCode field init code
   */
  public void addField(String type, String fieldName, String initCode) {
    fields.add(Tuple3.of(false, type, fieldName));
    if (StringUtils.isNotBlank(initCode)) {
      initCodes.add(initCode);
    }
  }

  /**
   * Add a field to class. The init code should be placed in constructor's code
   *
   * @param type type
   * @param fieldName field name
   */
  public void addField(Class<?> type, String fieldName) {
    addField(type(type), fieldName);
  }

  public void addField(String type, String fieldName) {
    fields.add(Tuple3.of(true, type, fieldName));
  }

  /** Generate code for class. */
  public String genCode() {
    StringBuilder codeBuilder = new StringBuilder();

    if (StringUtils.isNotBlank(pkg)) {
      codeBuilder.append("package ").append(pkg).append(";\n\n");
    }

    if (!imports.isEmpty()) {
      imports.forEach(clz -> codeBuilder.append("import ").append(clz).append(";\n"));
      codeBuilder.append('\n');
    }

    codeBuilder.append(String.format("public final class %s ", className));
    if (superClasses != null) {
      codeBuilder.append(String.format("extends %s ", String.join(", ", superClasses)));
    }
    if (interfaces != null) {
      codeBuilder.append(String.format("implements %s ", String.join(", ", interfaces)));
    }
    codeBuilder.append("{\n");

    // fields
    if (!fields.isEmpty()) {
      codeBuilder.append('\n');
      for (Tuple3<Boolean, String, String> field : fields) {
        String declare;
        if (field.f0) {
          declare = String.format("private final %s %s;\n", field.f1, field.f2);
        } else {
          declare = String.format("private %s %s;\n", field.f1, field.f2);
        }
        codeBuilder.append(indent(declare));
      }
    }

    // constructors
    if (!constructors.isEmpty()) {
      codeBuilder.append('\n');
      constructors.forEach(constructor -> codeBuilder.append(indent(constructor)).append('\n'));
    }

    // methods
    codeBuilder.append('\n');
    methods.values().forEach(method -> codeBuilder.append(indent(method)).append('\n'));

    codeBuilder.append('}');
    return codeBuilder.toString();
  }

  public void clearExprState() {
    exprState.clear();
  }

  /** Optimize method code based current compiled expressions. */
  public String optimizeMethodCode(String code) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<Expression, ExprState> entry : exprState.entrySet()) {
      Expression expression = entry.getKey();
      ExprState value = entry.getValue();
      if (expression instanceof Reference) {
        Reference reference = (Reference) expression;
        if (reference.isFieldRef() && value.getAccessCount() > 1) {
          // access only once are not necessary to load into local variable table,
          // which bloat local variable table.
          String type = type(reference.type());
          String cacheVariable =
              StringUtils.format(
                  "${type} ${name} = this.${name};", "type", type, "name", reference.name());
          builder.append(cacheVariable).append('\n');
        }
      }
    }
    if (builder.length() > 0) {
      return builder.append(code).toString();
    } else {
      return code;
    }
  }
}
