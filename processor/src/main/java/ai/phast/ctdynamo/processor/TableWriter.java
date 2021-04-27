package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.DynamoCodec;
import ai.phast.ctdynamo.DynamoTable;
import ai.phast.ctdynamo.annotations.DefaultCodec;
import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoIgnore;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
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

    private int paramNumber = 0;

    private final Map<String, AttributeMetadata> attributes = new HashMap<>();

    private final Map<TypeMirror, String> codecClassToCodecVar = new HashMap<>();

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
        for (var element : entryType.getEnclosedElements()) {
            if (element.getKind() == ElementKind.METHOD) {
                var exec = (ExecutableElement)element;
                var name = exec.getSimpleName().toString();
                if (name.startsWith("get") && name.length() >= 4) {
                    processGetter(exec);
                }
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
        for (var codecEntry: codecClassToCodecVar.entrySet()) {
            var field = FieldSpec.builder(TypeName.get(codecEntry.getKey()), codecEntry.getValue(),
                Modifier.PRIVATE, Modifier.FINAL)
                .initializer(CodeBlock.builder().add("new $T()", codecEntry.getKey()).build());
            classBuilder.addField(field.build());
        }
        classBuilder.addMethod(buildTableConstructor(true, true))
                            .addMethod(buildTableConstructor(true, false))
                            .addMethod(buildTableConstructor(false, true))
                            .addMethod(buildGetKey("getPartitionKey", partitionKeyAttribute))
                            .addMethod(buildGetKey("getSortKey", sortKeyAttribute))
                            .addMethod(buildKeyToAttributeValue("partitionValueToAttributeValue", partitionKeyAttribute))
                            .addMethod(buildKeyToAttributeValue("sortValueToAttributeValue", sortKeyAttribute))
                            .addMethod(buildEncoder(false))
                            .addMethod(buildDecoder(false))
                            .addMethod(buildGetExclusiveStart());
        var qualifiedName = entryType.getQualifiedName().toString();
        var packageSplit = qualifiedName.lastIndexOf('.');
        return JavaFile.builder(packageSplit > 0 ? qualifiedName.substring(0, packageSplit) : "", classBuilder.build()).build();
    }

    public JavaFile buildCodecClass() throws TableException {
        var codecType = types.getDeclaredType(elements.getTypeElement(DynamoCodec.class.getCanonicalName()),
            types.getDeclaredType(entryType));
        var classBuilder = TypeSpec.classBuilder(entryType.getSimpleName() + "DynamoCodec")
                               .addModifiers(Modifier.PUBLIC)
                               .superclass(ParameterizedTypeName.get(codecType));
        for (var codecEntry: codecClassToCodecVar.entrySet()) {
            var field = FieldSpec.builder(TypeName.get(codecEntry.getKey()), codecEntry.getValue(),
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

    private void processGetter(ExecutableElement getter) throws TableException {
        if (getter.getAnnotation(DynamoIgnore.class) != null) {
            return; // Ignoring this field.
        }
        TypeMirror codecClass = null;
        var getterName = getter.getSimpleName().toString();
        var getterReturnType = getter.getReturnType();
        var attributeName = getAttributeName(null, getterName);
        var attributeAnnotation = getter.getAnnotation(DynamoAttribute.class);
        if (attributeAnnotation != null) {
            attributeName = getAttributeName(attributeAnnotation.value(), getterName);
            codecClass = getCodecClass(attributeAnnotation::codec);
        }
        var partitionKeyAnnotation = getter.getAnnotation(DynamoPartitionKey.class);
        if (partitionKeyAnnotation != null) {
            if (attributeAnnotation != null) {
                throw new TableException("At most one of " + DynamoPartitionKey.class.getSimpleName()
                                             + ", " + DynamoSortKey.class.getSimpleName()
                                             + ", or " + DynamoAttribute.class.getSimpleName()
                                             + " may be provided for each method", getter);
            }
            if (partitionKeyAttribute != null) {
                throw new TableException("Cannot have multiple partition keys", getter);
            }
            partitionKeyAttribute = getAttributeName(partitionKeyAnnotation.value(), getterName);
            codecClass = getCodecClass(partitionKeyAnnotation::codec);
        }
        var sortKeyAnnotation = getter.getAnnotation(DynamoSortKey.class);
        if (sortKeyAnnotation != null) {
            if (partitionKeyAnnotation != null || attributeAnnotation != null) {
                throw new TableException("At most one of " + DynamoPartitionKey.class.getSimpleName()
                                             + ", " + DynamoSortKey.class.getSimpleName()
                                             + ", or " + DynamoAttribute.class.getSimpleName()
                                             + " may be provided for each method", getter);
            }
            if (sortKeyAttribute != null) {
                throw new TableException("Cannot have multiple sort keys", getter);
            }
            sortKeyAttribute = getAttributeName(sortKeyAnnotation.value(), getterName);
            codecClass = getCodecClass(sortKeyAnnotation::codec);
        }
        if (defaultCodecMirror.equals(codecClass)) {
            codecClass = null;
        }
        if (codecClass != null) {
            codecClassToCodecVar.computeIfAbsent(codecClass, klass -> "codec" + ++paramNumber);
        }
        var prevMetadata = attributes.put(attributeName, new AttributeMetadata(getterName, getterReturnType, codecClass));
        if (prevMetadata != null) {
            throw new TableException("Two getters return attribute " + attributeName, getter);
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
        builder.addParameter(String.class, "tableName")
            .addParameter(String.class, "partitionKeyAttribute")
            .addParameter(String.class, "sortKeyAttribute");
        if (withSyncClient && withAsyncClient) {
            builder.addStatement("super(client, asyncClient, tableName, partitionKeyAttribute, sortKeyAttribute)");
        } else if (withSyncClient) {
            builder.addStatement("super(client, null, tableName, partitionKeyAttribute, sortKeyAttribute)");
        } else {
            builder.addStatement("super(null, asyncClient, tableName, partitionKeyAttribute, sortKeyAttribute)");
        }
        return builder.build();
    }

    private MethodSpec buildGetKey(String getKeyName, String attributeName) {
        var methodBuilder = MethodSpec.methodBuilder(getKeyName)
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                                .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value");
        if (attributeName == null) {
            // A nonexistant sort key. Return a Void that is null.
            methodBuilder.returns(Void.class)
                .addStatement("return null");
        } else {
            var parameterMetadata = attributes.get(attributeName);
            methodBuilder.returns(TypeName.get(parameterMetadata.returnType))
                .addStatement("return value." + parameterMetadata.getterName + "()");
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
        var builder = MethodSpec.methodBuilder(toAttributeValue ? "encode" : "encodeToMap")
                          .addAnnotation(Override.class)
                          .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                          .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
                          .returns(toAttributeValue ? TypeName.get(AttributeValue.class) : TypeName.get(dynamoMapMirror))
                          .addStatement("$T map = new $T($L)", MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, (attributes.size() * 4 + 2) / 3);
        var formatParams = new HashMap<String, Object>();
        for (var entry : attributes.entrySet()) {
            var kind = entry.getValue().returnType.getKind();
            formatParams.clear();
            var attrNameParam = "s" + ++paramNumber;
            formatParams.put(attrNameParam, entry.getKey());
            if (kind.isPrimitive()) {
                builder.addNamedCode("map.put($" + attrNameParam + ":S, " + buildAttributeEncodeExpression(entry.getKey(), null, formatParams) + ");\n", formatParams);
            } else {
                // Non-primitives. May be null
                var varName = "v" + upcaseFirst(entry.getKey());  // Prepend a "v" and upcase to make sure it is not a reserved word, or the name "map" or "value"
                builder.addStatement("$T " + varName + " = value." + entry.getValue().getterName + "()", TypeName.get(entry.getValue().returnType));
                if (ignoreNulls) {
                    builder.beginControlFlow("if (" + varName + " != null)")
                        .addNamedCode("map.put($" + attrNameParam + ":S, " + buildAttributeEncodeExpression(entry.getKey(), varName, formatParams) + ");\n", formatParams)
                        .endControlFlow();
                } else {
                    builder.addNamedCode("map.put($" + attrNameParam + ":S, " + varName + " == null ? NULL_ATTRIBUTE_VALUE : "
                                             + buildAttributeEncodeExpression(entry.getKey(), varName, formatParams) + ");\n", formatParams);
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

    private MethodSpec buildGetExclusiveStart() throws TableException {
        var builder = MethodSpec.methodBuilder("getExclusiveStart")
                          .addAnnotation(Override.class)
                          .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                          .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
                          .returns(TypeName.get(dynamoMapMirror));
        var formatParams = new HashMap<String, Object>();
        if (sortKeyAttribute == null) {
            formatParams.put("collections", Collections.class);
            formatParams.put("param", partitionKeyAttribute);
            builder.addNamedCode("return $collections:T.singletonMap($param:S, "
                                     + buildAttributeEncodeExpression(partitionKeyAttribute, null, formatParams)
                                     + ");\n", formatParams);
        } else {
            formatParams.put("map", Map.class);
            formatParams.put("param", partitionKeyAttribute);
            formatParams.put("sort", sortKeyAttribute);
            builder.addNamedCode("return $map:T.of($param:S, "
                                     + buildAttributeEncodeExpression(partitionKeyAttribute, null, formatParams)
                                     + ", $sort:S, "
                                     + buildAttributeEncodeExpression(sortKeyAttribute, null, formatParams), formatParams);
        }
        return builder.build();
    }

    private String buildGetterName(String attributeName) {
        return "get" + Character.toUpperCase(attributeName.charAt(0)) + attributeName.substring(1);
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

    private String buildAttributeEncodeExpression(String valueVar, TypeMirror codecClass, TypeMirror returnType, Map<String, Object> formatData)
    throws TableException {
        if (codecClass == null) {
            var avId = "t" + ++paramNumber;
            formatData.put(avId, AttributeValue.class);
            switch (returnType.getKind()) {
                case INT:
                case LONG:
                case BYTE:
                case FLOAT:
                case DOUBLE:
                case SHORT:
                    var emptyStrId = "s" + ++paramNumber;
                    // Any numeric type will be catenated with a string and passed in as a number
                    formatData.put(emptyStrId, "");
                    return "$" + avId + ":T.builder().n($" + emptyStrId + ":S + " + valueVar + ").build()";
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
            } else {
                throw new TableException("Don't know how to encode class " + returnType);
            }
        } else {
            // We have a codec for this class. Simply call it.
            return codecClassToCodecVar.get(codecClass) + ".encode(" + valueVar + ")";
        }
    }

    private String buildAttributeDecodeExpression(String valueVar, TypeMirror codecClass, TypeMirror returnType, Map<String, Object> formatData)
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
            } else {
                throw new TableException("Don't know how to encode class " + returnType);
            }
        } else {
            // We have a codec for this class. Simply call it.
            return codecClassToCodecVar.get(codecClass) + ".decode(" + valueVar + ")";
        }
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
        public final TypeMirror codecClass;

        public AttributeMetadata(String getterName, TypeMirror returnType, TypeMirror codecClass) {
            this.getterName = getterName;
            setterName = "set" + getterName.substring(3);
            this.returnType = returnType;
            this.codecClass = codecClass;
        }
    }
}
