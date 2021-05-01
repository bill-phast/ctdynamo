package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.DynamoCodec;
import ai.phast.ctdynamo.DynamoIndex;
import ai.phast.ctdynamo.DynamoTable;
import ai.phast.ctdynamo.annotations.DefaultCodec;
import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoIgnore;
import ai.phast.ctdynamo.annotations.DynamoItem;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSecondaryPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSecondarySortKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

public class TableWriter {

    private static final ClassName HASH_MAP_CLASS_NAME = ClassName.get(HashMap.class);
    private static final ClassName STRING_CLASS_NAME = ClassName.get(String.class);
    private static final ClassName ATTRIBUTE_VALUE_CLASS_NAME = ClassName.get(AttributeValue.class);
    private static final TypeName MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME = ParameterizedTypeName.get(HASH_MAP_CLASS_NAME, STRING_CLASS_NAME, ATTRIBUTE_VALUE_CLASS_NAME);

    /**
     * The element declaring the type of our table entry
     */
    private final TypeElement entryType;

    /** Our element utilities */
    private final Elements elements;

    /** Our type utilities */
    private final Types types;

    private String partitionKeyAttribute;

    private String sortKeyAttribute;

    /** Mirror type of Map<String, AttributeValue>. Used frequently, so we build it once at constructor time */
    private final TypeMirror dynamoMapMirror;

    private final TypeMirror defaultCodecMirror;

    private final TypeMirror stringMirror;

    private final TypeMirror listMirror;

    private final TypeMirror setMirror;

    private final TypeMirror enumMirror;

    private final List<TypeMirror> boxedPrimitiveMirrors;

    private int paramNumber = 0;

    private final Map<String, AttributeMetadata> attributes = new HashMap<>();

    private final Set<String> ignoredAttributes = new HashSet<>();

    private final Map<TypeName, String> codecClassToCodecVar = new HashMap<>();

    private final Map<String, IndexMetadata> indexes = new HashMap<>();

    private final boolean ignoreNulls;

    public TableWriter(TypeElement entryType, Elements elements, Types types, boolean ignoreNulls) throws TableException {
        this.entryType = entryType;
        this.elements = elements;
        this.types = types;
        this.ignoreNulls = ignoreNulls;
        dynamoMapMirror = types.getDeclaredType(elements.getTypeElement(Map.class.getCanonicalName()),
            types.getDeclaredType(elements.getTypeElement(String.class.getCanonicalName())),
            types.getDeclaredType(elements.getTypeElement(AttributeValue.class.getCanonicalName())));
        defaultCodecMirror = types.getDeclaredType(elements.getTypeElement(DefaultCodec.class.getCanonicalName()));
        stringMirror = types.getDeclaredType(elements.getTypeElement(String.class.getCanonicalName()));
        listMirror = types.getDeclaredType(elements.getTypeElement(List.class.getCanonicalName()),
            types.getWildcardType(null, null));
        setMirror = types.getDeclaredType(elements.getTypeElement(Set.class.getCanonicalName()),
            types.getWildcardType(null, null));
        enumMirror = types.getDeclaredType(elements.getTypeElement(Enum.class.getCanonicalName()),
            types.getWildcardType(null, null));
        boxedPrimitiveMirrors = List.of(TypeKind.INT, TypeKind.BYTE, TypeKind.LONG, TypeKind.FLOAT, TypeKind.DOUBLE, TypeKind.SHORT, TypeKind.BOOLEAN)
            .stream().map(kind -> types.boxedClass(types.getPrimitiveType(kind)).asType())
            .collect(Collectors.toList());

        for (var element : entryType.getEnclosedElements()) {
            if (element.getKind() == ElementKind.METHOD) {
                var exec = (ExecutableElement)element;
                var name = exec.getSimpleName().toString();
                if (name.startsWith("get") && name.length() >= 4) {
                    processGetter(exec);
                }
            } else if (element.getKind() == ElementKind.FIELD) {
                processField((VariableElement)element);
            }
        }
    }

    public JavaFile buildTableClass() throws TableException {
        if (partitionKeyAttribute == null) {
            throw new TableException("Tables must have a getter with @DynamoPartitionKey annotation");
        }
        var tableType = types.getDeclaredType(elements.getTypeElement(DynamoTable.class.getCanonicalName()),
            types.getDeclaredType(entryType), attributes.get(partitionKeyAttribute).returnType,
            sortKeyAttribute == null
            ? types.getDeclaredType(elements.getTypeElement(Void.class.getCanonicalName()))
            : attributes.get(sortKeyAttribute).returnType);
        var classBuilder = TypeSpec.classBuilder(entryType.getSimpleName() + "DynamoTable")
                               .addModifiers(Modifier.PUBLIC)
                               .superclass(ParameterizedTypeName.get(tableType));
        classBuilder.addMethod(buildTableConstructor(true, true))
            .addMethod(buildTableConstructor(true, false))
            .addMethod(buildTableConstructor(false, true))
            .addMethod(buildGetKey("getPartitionKey", partitionKeyAttribute, true))
            .addMethod(buildGetKey("getSortKey", sortKeyAttribute, true))
            .addMethod(buildGetKey("getPartitionKey", partitionKeyAttribute, false))
            .addMethod(buildGetKey("getSortKey", sortKeyAttribute, false))
            .addMethod(buildKeyToAttributeValue("partitionValueToAttributeValue", partitionKeyAttribute))
            .addMethod(buildKeyToAttributeValue("sortValueToAttributeValue", sortKeyAttribute))
            .addMethod(buildEncoder(false))
            .addMethod(buildDecoder(false))
            .addMethod(buildGetExclusiveStart())
            .addMethod(buildGetIndex());

        var qualifiedName = entryType.getQualifiedName().toString();
        var packageSplit = qualifiedName.lastIndexOf('.');
        var packageName = (packageSplit > 0 ? qualifiedName.substring(0, packageSplit) : "");
        // Add our indexes
        for (var indexName : indexes.keySet()) {
            TypeName name = ClassName.get(packageName, entryType.getSimpleName() + "DynamoTable",
                indexNameToClassName(indexName));
            classBuilder.addType(buildIndexInnerClass(indexName));
            classBuilder.addMethod(MethodSpec.methodBuilder("get" + indexNameToClassName(indexName))
                                       .addModifiers(Modifier.PUBLIC)
                                       .returns(name)
                                       .addStatement("return new $T(getClient(), getAsyncClient(), getTableName())", name)
                                       .build());
        }

        // Add the member variables for the codecs we need. This must be done after all methods are built, because building
        // methods may find more codecs we need.
        for (var codecEntry : codecClassToCodecVar.entrySet()) {
            var field = FieldSpec.builder(codecEntry.getKey(), codecEntry.getValue(),
                Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                            .initializer(CodeBlock.builder().add("new $T()", codecEntry.getKey()).build());
            classBuilder.addField(field.build());
        }

        return JavaFile.builder(packageName, classBuilder.build()).build();
    }

    private TypeSpec buildIndexInnerClass(String indexName) throws TableException {
        var metadata = indexes.get(indexName);
        if (metadata.getPartitonAttribute() == null) {
            throw new TableException("Index " + indexName + " has no partition key");
        }
        if (metadata.getSortAttribute() == null) {
            throw new TableException("Index " + indexName + " has no sort key");
        }
        var indexType = types.getDeclaredType(elements.getTypeElement(DynamoIndex.class.getCanonicalName()),
            types.getDeclaredType(entryType), attributes.get(metadata.partitonAttribute).returnType,
            attributes.get(metadata.sortAttribute).returnType);
        var constructor = MethodSpec.constructorBuilder()
                              .addParameter(DynamoDbClient.class, "client")
                              .addParameter(DynamoDbAsyncClient.class, "asyncClient")
                              .addParameter(String.class, "tableName")
                              .addStatement("super(client, asyncClient, tableName, $S, $S, $S)",
                                  indexName, metadata.partitonAttribute, metadata.sortAttribute)
                              .build();
        var classBuilder = TypeSpec.classBuilder(indexNameToClassName(indexName))
                               .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                               .superclass(ParameterizedTypeName.get(indexType))
                               .addMethod(constructor)
                               .addMethod(buildKeyToAttributeValue("partitionValueToAttributeValue", metadata.partitonAttribute))
                               .addMethod(buildKeyToAttributeValue("sortValueToAttributeValue", metadata.sortAttribute))
                               .addMethod(buildDecoder(false))
                               .addMethod(buildGetExclusiveStart(metadata.partitonAttribute, metadata.getSortAttribute()));
        return classBuilder.build();
    }

    public JavaFile buildCodecClass() throws TableException {
        var codecType = types.getDeclaredType(elements.getTypeElement(DynamoCodec.class.getCanonicalName()),
            types.getDeclaredType(entryType));
        var classBuilder = TypeSpec.classBuilder(entryType.getSimpleName() + "DynamoCodec")
                               .addModifiers(Modifier.PUBLIC)
                               .superclass(ParameterizedTypeName.get(codecType));
        for (var codecEntry: codecClassToCodecVar.entrySet()) {
            var field = FieldSpec.builder(codecEntry.getKey(), codecEntry.getValue(),
                Modifier.PRIVATE, Modifier.FINAL)
                            .initializer(CodeBlock.builder().add("new $T()", codecEntry.getKey()).build());
            classBuilder.addField(field.build());
        }
        classBuilder.addMethod(buildEncoder(true))
            .addMethod(buildDecoder(true));
        var qualifiedName = entryType.getQualifiedName().toString();
        var packageSplit = qualifiedName.lastIndexOf('.');
        return JavaFile.builder(packageSplit > 0 ? qualifiedName.substring(0, packageSplit) : "", classBuilder.build()).build();
    }

    private void processField(VariableElement element) throws TableException {
        var attributeName = element.getSimpleName().toString();
        if (ignoredAttributes.contains(attributeName)) {
            // We already got an ignore for this.
            return;
        }
        if (element.getAnnotation(DynamoIgnore.class) != null) {
            ignoredAttributes.add(attributeName);
            return;
        }
        if (element.getAnnotation(DynamoAttribute.class) != null
                || element.getAnnotation(DynamoPartitionKey.class) != null
                || element.getAnnotation(DynamoSortKey.class) != null
                || element.getAnnotation(DynamoSecondaryPartitionKey.class) != null
                || element.getAnnotation(DynamoSecondarySortKey.class) != null) {
            processAttribute(element, "get" + upcaseFirst(attributeName), attributeName, element.asType());
        }
    }

    private void processGetter(ExecutableElement getter) throws TableException {
        var getterName = getter.getSimpleName().toString();
        var attributeName = getAttributeName(null, getterName);
        if (ignoredAttributes.contains(attributeName)) {
            // We already got an ignore for this attribute.
            return;
        }
        if (getter.getAnnotation(DynamoIgnore.class) != null) {
            ignoredAttributes.add(attributeName);
            return; // Ignoring this field.
        }
        processAttribute(getter, getterName, attributeName, getter.getReturnType());
    }

    private void processAttribute(Element declaringElement, String getterName, String attributeName, TypeMirror attributeType) throws TableException {
        TypeMirror codecType = null;
        var attributeAnnotation = declaringElement.getAnnotation(DynamoAttribute.class);
        if (attributeAnnotation != null) {
            attributeName = getAttributeName(attributeAnnotation.value(), getterName);
            codecType = getCodecClass(attributeAnnotation::codec);
        }
        var partitionKeyAnnotation = declaringElement.getAnnotation(DynamoPartitionKey.class);
        if (partitionKeyAnnotation != null) {
            if (attributeAnnotation != null) {
                throw new TableException("At most one of " + DynamoPartitionKey.class.getSimpleName()
                                             + ", " + DynamoSortKey.class.getSimpleName()
                                             + ", or " + DynamoAttribute.class.getSimpleName()
                                             + " may be provided for each method", declaringElement);
            }
            if (partitionKeyAttribute != null) {
                throw new TableException("Cannot have multiple partition keys", declaringElement);
            }
            partitionKeyAttribute = getAttributeName(partitionKeyAnnotation.value(), getterName);
            codecType = getCodecClass(partitionKeyAnnotation::codec);
        }
        var sortKeyAnnotation = declaringElement.getAnnotation(DynamoSortKey.class);
        if (sortKeyAnnotation != null) {
            if (partitionKeyAnnotation != null || attributeAnnotation != null) {
                throw new TableException("At most one of " + DynamoPartitionKey.class.getSimpleName()
                                             + ", " + DynamoSortKey.class.getSimpleName()
                                             + ", or " + DynamoAttribute.class.getSimpleName()
                                             + " may be provided for each method", declaringElement);
            }
            if (sortKeyAttribute != null) {
                throw new TableException("Cannot have multiple sort keys", declaringElement);
            }
            sortKeyAttribute = getAttributeName(sortKeyAnnotation.value(), getterName);
            codecType = getCodecClass(sortKeyAnnotation::codec);
        }
        var secondaryPartitionKeyAnnotation = declaringElement.getAnnotation(DynamoSecondaryPartitionKey.class);
        if (secondaryPartitionKeyAnnotation != null) {
            for (var indexName : secondaryPartitionKeyAnnotation.value()) {
                indexes.computeIfAbsent(indexName, index -> new IndexMetadata()).setPartitonAttribute(attributeName);
            }
        }
        var secondarySortKeyAnnotation = declaringElement.getAnnotation(DynamoSecondarySortKey.class);
        if (secondarySortKeyAnnotation != null) {
            for (var indexName: secondarySortKeyAnnotation.value()) {
                indexes.computeIfAbsent(indexName, index -> new IndexMetadata()).setSortAttribute(attributeName);
            }
        }
        var codecName = (codecType == null || defaultCodecMirror.equals(codecType) ? null : TypeName.get(codecType));
        if (codecName == null) {
            codecName = findCodecClass(attributeType);
        } else {
            addCodec(codecName);
        }
        var prevMetadata = attributes.put(attributeName, new AttributeMetadata(getterName, attributeType, codecName));
        if (prevMetadata != null) {
            throw new TableException("Two getters return attribute " + attributeName, declaringElement);
        }
    }

    private String getAttributeName(String annotationValue, String getterName) {
        return (annotationValue == null || annotationValue.isEmpty()
                ? Character.toLowerCase(getterName.charAt(3)) + getterName.substring(4)
                : annotationValue);
    }

    private MethodSpec buildTableConstructor(boolean withSyncClient, boolean withAsyncClient) {
        var builder = MethodSpec.constructorBuilder()
                          .addModifiers(Modifier.PUBLIC);
        if (withSyncClient) {
            builder.addParameter(DynamoDbClient.class, "client");
        }
        if (withAsyncClient) {
            builder.addParameter(DynamoDbAsyncClient.class, "asyncClient");
        }
        builder.addParameter(String.class, "tableName");
        if (withSyncClient && withAsyncClient) {
            builder.addStatement("super(client, asyncClient, tableName, $S, $S)", partitionKeyAttribute, sortKeyAttribute);
        } else if (withSyncClient) {
            builder.addStatement("super(client, null, tableName, $S, $S)", partitionKeyAttribute, sortKeyAttribute);
        } else {
            builder.addStatement("super(null, asyncClient, tableName, $S, $S)", partitionKeyAttribute, sortKeyAttribute);
        }
        return builder.build();
    }

    private MethodSpec buildGetKey(String getKeyName, String attributeName, boolean fromItem) throws TableException {
        var methodBuilder = MethodSpec.methodBuilder(getKeyName)
                                .addAnnotation(Override.class)
                                .addModifiers(fromItem ? Modifier.PUBLIC : Modifier.PROTECTED, Modifier.FINAL);
        if (fromItem) {
            methodBuilder.addParameter(TypeName.get(types.getDeclaredType(entryType)), "value");
        } else {
            methodBuilder.addParameter(AttributeValue.class, "value");
        }
        if (attributeName == null) {
            // A nonexistant sort key. Return a Void that is null.
            methodBuilder.returns(Void.class)
                .addStatement("return null");
        } else {
            var parameterMetadata = attributes.get(attributeName);
            methodBuilder.returns(TypeName.get(parameterMetadata.returnType));
            if (fromItem) {
                if (parameterMetadata.returnType.getKind().isPrimitive()) {
                    // Cannot be null
                    methodBuilder.addStatement("return value." + parameterMetadata.getterName + "()");
                } else {
                    // Check for null
                    methodBuilder.addStatement("$T key = value." + parameterMetadata.getterName + "()", parameterMetadata.returnType)
                        .beginControlFlow("if (key == null)")
                        .addStatement("throw new $T($S)", NullPointerException.class,
                            "Null "
                                + (attributeName.equals(partitionKeyAttribute) ? "partition" : "sort")
                                + " key attribute \"" + attributeName + "\"")
                        .endControlFlow()
                        .addStatement("return key");
                }
            } else {
                methodBuilder.beginControlFlow("if ((value == null) || (value.nul() == $T.TRUE))", Boolean.class)
                    .addStatement("return null")
                    .nextControlFlow("else");
                var formatParams = new HashMap<String, Object>();
                methodBuilder.addCode("return " + buildAttributeDecodeExpression("value", parameterMetadata.codecClass, parameterMetadata.returnType, formatParams) + ";\n", formatParams);
                methodBuilder.endControlFlow();
            }
        }
        return methodBuilder.build();
    }

    private MethodSpec buildKeyToAttributeValue(String methodName, String attribute) throws TableException {
        var metadata = (attribute == null ? null : attributes.get(attribute));
        var methodBuilder = MethodSpec.methodBuilder(methodName)
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
                                .returns(AttributeValue.class);
        if (metadata == null) {
            methodBuilder.addParameter(Void.class, "value");
            methodBuilder.addStatement("throw new $T($S)", UnsupportedOperationException.class,
                "This table has no sort key");
        } else {
            methodBuilder.addParameter(TypeName.get(metadata.returnType), "value");
            var formatParams = new HashMap<String, Object>();
            methodBuilder.addNamedCode("return " + buildAttributeEncodeExpression(attribute, "value", formatParams) + ";\n", formatParams);
        }
        return methodBuilder.build();
    }

    private MethodSpec buildEncoder(boolean toAttributeValue) throws TableException {
        var builder = MethodSpec.methodBuilder("encode")
                          .addAnnotation(Override.class)
                          .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                          .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
                          .returns(toAttributeValue ? TypeName.get(AttributeValue.class) : TypeName.get(dynamoMapMirror))
                          .addStatement("$T map = new $T($L)", MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, (attributes.size() * 4 + 2) / 3);
        var formatParams = new HashMap<String, Object>();
        for (var entry : attributes.entrySet()) {
            var attributeName = entry.getKey();
            var kind = entry.getValue().returnType.getKind();
            formatParams.clear();
            var attrNameParam = "s" + ++paramNumber;
            formatParams.put(attrNameParam, attributeName);
            if (kind.isPrimitive()) {
                builder.addNamedCode("map.put($" + attrNameParam + ":S, " + buildAttributeEncodeExpression(attributeName, null, formatParams) + ");\n", formatParams);
            } else {
                // Non-primitives. May be null
                var varName = "v" + upcaseFirst(attributeName);  // Prepend a "v" and upcase to make sure it is not a reserved word, or the name "map" or "value"
                builder.addStatement("$T " + varName + " = value." + entry.getValue().getterName + "()", TypeName.get(entry.getValue().returnType));
                if (attributeName.equals(partitionKeyAttribute) || attributeName.equals(sortKeyAttribute)) {
                    builder.beginControlFlow("if (" + varName + " == null)")
                        .addStatement("throw new $T($S)", NullPointerException.class,
                            "Null primary "
                                + (attributeName.equals(partitionKeyAttribute) ? "partition" : "sort")
                                + " key attribute \"" + attributeName + "\"")
                        .endControlFlow()
                        .addNamedCode("map.put($" + attrNameParam + ":S, " + buildAttributeEncodeExpression(attributeName, varName, formatParams) + ");\n", formatParams);
                } else if (ignoreNulls) {
                    builder.beginControlFlow("if (" + varName + " != null)")
                        .addNamedCode("map.put($" + attrNameParam + ":S, " + buildAttributeEncodeExpression(attributeName, varName, formatParams) + ");\n", formatParams)
                        .endControlFlow();
                } else {
                    var codecClassParam = "t" + ++paramNumber;
                    formatParams.put(codecClassParam, DynamoCodec.class);
                    builder.addNamedCode("map.put($" + attrNameParam + ":S, " + varName + " == null ? $" + codecClassParam + ":T.NULL_ATTRIBUTE_VALUE : "
                                             + buildAttributeEncodeExpression(attributeName, varName, formatParams) + ");\n", formatParams);
                }
            }
        }
        if (toAttributeValue) {
            builder.addStatement("return $T.builder().m(map).build()", AttributeValue.class);
        } else {
            builder.addStatement("return map");
        }
        return builder.build();
    }

    private MethodSpec buildDecoder(boolean fromAttributeValue) throws TableException {
        var entryTypeName = TypeName.get(types.getDeclaredType(entryType));
        var builder = MethodSpec.methodBuilder("decode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .returns(entryTypeName)
            .addStatement("$T result = new $T()", entryTypeName, entryTypeName);
        if (fromAttributeValue) {
            builder.addParameter(TypeName.get(AttributeValue.class), "value")
                .addStatement("$T map = value.m()", dynamoMapMirror);
        } else {
            builder.addParameter(TypeName.get(dynamoMapMirror), "map");
        }
        builder.addStatement("$T attribute", AttributeValue.class);
        var formatParams = new HashMap<String, Object>();
        for (var entry: attributes.entrySet()) {
            builder.addStatement("attribute = map.get($S)", entry.getKey());
            builder.beginControlFlow("if (attribute != null && attribute.nul() != $T.TRUE)", Boolean.class);
            formatParams.clear();
            builder.addNamedCode("result." + entry.getValue().setterName + "("
                + buildAttributeDecodeExpression("attribute", entry.getValue().codecClass, entry.getValue().returnType, formatParams)
                + ");\n", formatParams);
            builder.endControlFlow();
        }
        return builder.addStatement("return result").build();
    }

    private MethodSpec buildGetExclusiveStart(String... secondaryKeys) throws TableException {
        var builder = MethodSpec.methodBuilder("getExclusiveStart")
                          .addAnnotation(Override.class)
                          .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                          .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
                          .returns(TypeName.get(dynamoMapMirror));
        var formatParams = new HashMap<String, Object>();
        if ((sortKeyAttribute) == null && (secondaryKeys.length == 0)) {
            formatParams.put("collections", Collections.class);
            formatParams.put("param", partitionKeyAttribute);
            builder.addNamedCode("return $collections:T.singletonMap($param:S, "
                                     + buildAttributeEncodeExpression(partitionKeyAttribute, null, formatParams)
                                     + ");\n", formatParams);
        } else {
            var keys = new HashSet<String>();
            keys.add(partitionKeyAttribute);
            if (sortKeyAttribute != null) {
                keys.add(sortKeyAttribute);
            }
            Collections.addAll(keys, secondaryKeys);
            formatParams.put("map", Map.class);
            formatParams.put("param", partitionKeyAttribute);
            formatParams.put("sort", sortKeyAttribute);
            var template = new StringBuilder("return $map:T.of(");
            var first = true;
            for (var key: keys) {
                var param = "v" + ++paramNumber;
                if (first) {
                    first = false;
                } else {
                    template.append(", ");
                }
                formatParams.put(param, key);
                template.append("$").append(param).append(":S, ")
                    .append(buildAttributeEncodeExpression(key, null, formatParams));
            }
            builder.addNamedCode(template.append(");\n").toString(), formatParams);
        }
        return builder.build();
    }

    private MethodSpec buildGetIndex() {
        var partitionT = TypeVariableName.get("IndexPartitionT");
        var sortT = TypeVariableName.get("IndexSortT");
        var builder = MethodSpec.methodBuilder("getIndex")
                          .addModifiers(Modifier.PUBLIC)
                          .addAnnotation(Override.class)
                          .addTypeVariable(partitionT)
                          .addTypeVariable(sortT)
                          .returns(ParameterizedTypeName.get(ClassName.get(DynamoIndex.class), TypeName.get(entryType.asType()), partitionT, sortT))
                          .addParameter(String.class, "name")
                          .addParameter(ParameterizedTypeName.get(ClassName.get(Class.class), partitionT), "partitionClass")
                          .addParameter(ParameterizedTypeName.get(ClassName.get(Class.class), sortT), "sortClass");
        switch (indexes.size()) {
            case 0:
                return builder.addStatement("throw new $T($S)", UnsupportedOperationException.class,
                    "Class " + entryType.getSimpleName() + " has no secondary indexes. Maybe you are missing @"
                        + DynamoSecondaryPartitionKey.class.getSimpleName() + " annotations").build();
            case 1:
                var entry = indexes.entrySet().iterator().next();
                return builder.beginControlFlow("if (name.equals($S))", entry.getKey())
                           .addStatement("if ((partitionClass == $T.class) && (sortClass == $T.class))",
                               attributes.get(entry.getValue().getPartitonAttribute()).returnType,
                               attributes.get(entry.getValue().getSortAttribute()).returnType)
                           .addStatement("return (DynamoIndex<$T, IndexPartitionT, IndexSortT>)" + "get" + upcaseFirst(entry.getKey()) + "Index()", entryType)
                           .nextControlFlow("else")
                           .addStatement("throw new $T($S + name)", IllegalArgumentException.class, "Unknown index: ")
                           .endControlFlow()
                           .build();
            default:
                builder.addStatement("$T expectedPartitionClass", ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)))
                    .addStatement("$T expectedSortClass", ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)))
                    .addStatement("$T index", ParameterizedTypeName.get(ClassName.get(DynamoIndex.class),
                        WildcardTypeName.subtypeOf(Object.class), WildcardTypeName.subtypeOf(Object.class)))
                    .beginControlFlow("switch(name)");
                for (var indexName : indexes.keySet()) {
                    var metadata = indexes.get(indexName);
                    builder.addCode("case($S):\n", indexName)
                        .addStatement("$>expectedPartitionClass = $T.class", indexName, attributes.get(metadata.partitonAttribute).returnType)
                        .addStatement("expectedSortClass = $T.class", attributes.get(metadata.sortAttribute).returnType)
                        .addStatement("index = " + "get" + upcaseFirst(indexName) + "()")
                        .addStatement("break");
                }
                builder.addCode("default:\n")
                    .addStatement("throw new $T($S + name)", IllegalArgumentException.class,
                        "Unknown index name: ");
                builder.endControlFlow();
                builder.beginControlFlow("if ((expectedPartitionClass != partitionClass) || (expectedSortClass != sortClass))")
                    .addStatement("throw new $T($S + name + $S + expectedPartitionClass.getSimpleName() + $S + expectedSortClass.getSimpleName() + $S + partitionClass.getSimpleName() + $S + sortClass.getSimpleName())",
                        IllegalArgumentException.class, "Incorrect key types for index ", ", expected: ", " and ", ", got: ", " and ")
                    .endControlFlow();
                builder.addStatement("return null");
                return builder.build();
        }
    }

    /**
     * Turn an index name into the name of a class. Replace illegal java class name characters with underscores, upcase
     * the first character, and append the word "Index" at the end.
     * @param indexName The name of the index
     * @return An inner class name.
     */
    private String indexNameToClassName(String indexName) {
        if (Character.isDigit(indexName.charAt(0))) {
            indexName = "n" + indexName;  // Prepend a "n" so we don't start with a digit.
        }
        return upcaseFirst(indexName.replace('.', '_').replace('-', '_')) + "Index";
    }

    private String upcaseFirst(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private String buildAttributeEncodeExpression(String attributeName, String valueVar, Map<String, Object> formatData) throws TableException {
        var metadata = Objects.requireNonNull(attributes.get(Objects.requireNonNull(attributeName, "Null attribute name")), "No metadata for " + attributeName);
        if (valueVar == null) {
            valueVar = "value." + metadata.getterName + "()";
        }
        return buildAttributeEncodeExpression(valueVar, metadata.codecClass, metadata.returnType, formatData);
    }

    private String buildAttributeEncodeExpression(String valueVar, TypeName codecClass, TypeMirror returnType, Map<String, Object> formatData)
    throws TableException {
        if (codecClass == null) {
            var avId = "t" + ++paramNumber;
            var typeId = "t" + ++paramNumber;
            formatData.put(avId, AttributeValue.class);
            switch (returnType.getKind()) {
                case INT:
                    formatData.put(typeId, Integer.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case LONG:
                    formatData.put(typeId, Long.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case BYTE:
                    formatData.put(typeId, Byte.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case FLOAT:
                    formatData.put(typeId, Float.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case DOUBLE:
                    formatData.put(typeId, Double.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case SHORT:
                    formatData.put(typeId, Short.class);
                    return "$" + avId + ":T.builder().n($" + typeId + ":T.toString(" + valueVar + ")).build()";
                case BOOLEAN:
                    return "$" + avId + ":T.builder().bool(" + valueVar + ").build()";
                case DECLARED:
                    break;
                default:
                    throw new TableException("Unknown typeKind " + returnType.getKind());
            }
            if (types.isSameType(returnType, stringMirror)) {
                return "$" + avId + ":T.builder().s(" + valueVar + ").build()";
            } else if (types.isSubtype(returnType, listMirror) || types.isSubtype(returnType, setMirror)) {
                var innerType = ((DeclaredType)returnType).getTypeArguments().get(0);
                var tmpVar = "t" + ++paramNumber;
                var collectors = "t" + ++paramNumber;
                formatData.put(collectors, Collectors.class);
                return "$" + avId + ":T.builder().l(" + valueVar + ".stream()"
                           + ".map(" + tmpVar + " -> " + buildAttributeEncodeExpression(tmpVar, null, innerType, formatData) + ")"
                           + ".collect($" + collectors + ":T.toList())).build()";
            } else if (types.isSubtype(returnType, enumMirror)) {
                return "$" + avId + ":T.builder().s(" + valueVar + ".name()).build()";
            } else if (returnType.equals(types.boxedClass(types.getPrimitiveType(TypeKind.BOOLEAN)).asType())) {
                return "$" + avId + ":T.builder().bool(" + valueVar + ").build()";
            } else if (boxedPrimitiveMirrors.contains(returnType)) {
                // boolean is in here too, but we check that first
                return "$" + avId + ":T.builder().n(" + valueVar + ".toString()).build()";
            } else {
                // See if we can find a codec for this class. Otherwise we can't encode it.
                codecClass = findCodecClass(returnType);
                if (codecClass == null) {
                    throw new TableException("Don't know how to encode class " + returnType);
                } else {
                    return codecClassToCodecVar.get(codecClass) + ".encode(" + valueVar + ")";
                }
            }
        } else {
            // We have a codec for this class. Simply call it.
            return codecClassToCodecVar.get(codecClass) + ".encode(" + valueVar + ")";
        }
    }

    private String buildAttributeDecodeExpression(String valueVar, TypeName codecClass, TypeMirror returnType, Map<String, Object> formatData)
        throws TableException {
        if (codecClass == null) {
            var typeId = "t" + ++paramNumber;
            switch (returnType.getKind()) {
                case INT:
                    formatData.put(typeId, Integer.class);
                    return "$" + typeId + ":T.parseInt(" + valueVar + ".n())";
                case LONG:
                    formatData.put(typeId, Long.class);
                    return "$" + typeId + ":T.parseLong(" + valueVar + ".n())";
                case BYTE:
                    formatData.put(typeId, Byte.class);
                    return "$" + typeId + ":T.parseByte(" + valueVar + ".n())";
                case FLOAT:
                    formatData.put(typeId, Float.class);
                    return "$" + typeId + ":T.parseFloat(" + valueVar + ".n())";
                case DOUBLE:
                    formatData.put(typeId, Double.class);
                    return "$" + typeId + ":T.parseDouble(" + valueVar + ".n())";
                case SHORT:
                    formatData.put(typeId, Short.class);
                    return "$" + typeId + ":T.parseShort(" + valueVar + ".n())";
                case BOOLEAN:
                    return "valueVar.bool()";
                case DECLARED:
                    break;
                default:
                    throw new TableException("Unknown typeKind " + returnType.getKind());
            }
            if (types.isSameType(returnType, stringMirror)) {
                return valueVar + ".s()";
            } else if (types.isSubtype(returnType, listMirror) || types.isSubtype(returnType, setMirror)) {
                var collectorFunc = (types.isSubtype(returnType, listMirror) ? "toList" : "toSet");
                var innerType = ((DeclaredType)returnType).getTypeArguments().get(0);
                var tmpVar = "t" + ++paramNumber;
                var collectors = "t" + ++paramNumber;
                formatData.put(collectors, Collectors.class);
                return valueVar + ".l().stream()"
                           + ".map(" + tmpVar + " -> " + buildAttributeDecodeExpression(tmpVar, null, innerType, formatData) + ")"
                           + ".collect($" + collectors + ":T." + collectorFunc + "())";
            } else if (types.isSubtype(returnType, enumMirror) || boxedPrimitiveMirrors.contains(returnType)) {
                formatData.put(typeId, returnType);
                return "$" + typeId + ":T.valueOf(" + valueVar + ".s())";
            } else {
                // See if we can find a codec for this class. Otherwise we can't decode it.
                codecClass = findCodecClass(returnType);
                if (codecClass == null) {
                    throw new TableException("Don't know how to decode class " + returnType);
                } else {
                    return codecClassToCodecVar.get(codecClass) + ".decode(" + valueVar + ")";
                }
            }
        } else {
            // We have a codec for this class. Simply call it.
            return codecClassToCodecVar.get(codecClass) + ".decode(" + valueVar + ")";
        }
    }

    private TypeName findCodecClass(TypeMirror baseType) {
        if (baseType.getKind() == TypeKind.DECLARED) {
            // Check to see if this is based on a class that has a DynamoItem annotation
            var itemAnnotation = ((DeclaredType)baseType).asElement().getAnnotation(DynamoItem.class);
            if (itemAnnotation != null && Arrays.asList(itemAnnotation.value()).contains(DynamoItem.Output.CODEC)) {
                // This gets a little tricky. We can't just create a TypeMirror, because the codec class will be generated by
                // us, so it doesn't exist yet. I think be working with the "stage" system of annotation processing we can
                // delay until it is created, but it's easier to do these steps to create a ClassName object for a nonexistant
                // class.
                var baseClassName = (ClassName)TypeName.get(baseType);
                var codecName = ClassName.get(baseClassName.packageName(), baseClassName.simpleName() + "DynamoCodec");
                addCodec(codecName);
                return codecName;
            }
        }
        return null;
    }

    private void addCodec(TypeName codecName) {
        codecClassToCodecVar.computeIfAbsent(codecName, klass -> "CODEC_" + ++paramNumber);
    }

    private TypeMirror getCodecClass(Supplier<Class<?>> supplier) throws TableException {
        try {
            var klass = supplier.get();
            throw new TableException("Somehow managed to get class " + klass + " from annotation");
        } catch (MirroredTypeException e) {
            return e.getTypeMirror();
        }
    }

    private static class AttributeMetadata {
        public final String getterName;
        private final String setterName;
        public final TypeMirror returnType;
        public final TypeName codecClass;

        public AttributeMetadata(String getterName, TypeMirror returnType, TypeMirror codecClass) {
            this(getterName, returnType, codecClass == null ? null : TypeName.get(codecClass));
        }

        public AttributeMetadata(String getterName, TypeMirror returnType, TypeName codecClass) {
            this.getterName = getterName;
            setterName = "set" + getterName.substring(3);
            this.returnType = returnType;
            this.codecClass = codecClass;
        }
    }

    private static class IndexMetadata {
        private String partitonAttribute;
        private String sortAttribute;

        public void setPartitonAttribute(String value) throws TableException {
            if ((partitonAttribute != null) && !partitonAttribute.equals(value)) {
                throw new TableException("Secondary partition attribute set twice: " + partitonAttribute + " and " + value);
            }
            partitonAttribute = value;
        }

        public String getPartitonAttribute() {
            return partitonAttribute;
        }

        public void setSortAttribute(String value) throws TableException {
            if ((sortAttribute != null) && !sortAttribute.equals(value)) {
                throw new TableException(("Secondary sort attribute set twice: " + partitonAttribute + " and " + value));
            }
            sortAttribute = value;
        }

        public String getSortAttribute() {
            return sortAttribute;
        }
    }
}
