package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.DynamoTable;
import ai.phast.ctdynamo.annotations.DynamoAttribute;
import ai.phast.ctdynamo.annotations.DynamoIgnore;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
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

    private final Map<String, AttributeMetadata> attributes = new HashMap<>();

    public TableWriter(TypeElement entryType, Elements elements, Types types) {
        this.entryType = entryType;
        this.elements = elements;
        this.types = types;
        dynamoMapMirror = types.getDeclaredType(elements.getTypeElement(Map.class.getCanonicalName()),
            types.getDeclaredType(elements.getTypeElement(String.class.getCanonicalName())),
            types.getDeclaredType(elements.getTypeElement(AttributeValue.class.getCanonicalName())));
    }

    public JavaFile buildJavaFile() throws TableException {
        for (var element : entryType.getEnclosedElements()) {
            if (element.getKind() == ElementKind.METHOD) {
                var exec = (ExecutableElement)element;
                var name = exec.getSimpleName().toString();
                if (name.startsWith("get") && name.length() >= 4) {
                    processGetter(exec);
                }
            }
        }
        if (partitionKeyAttribute == null) {
            throw new TableException("Tables must have a getter with @DynamoPartitionKey annotation");
        }
        var className = entryType.getQualifiedName() + "DynamoTable";
        var mainFunc = MethodSpec.methodBuilder("main")
                           .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                           .returns(void.class)
                           .addParameter(String[].class, "args")
                           .addStatement("$T.out.println($S)", System.class, "Hello world!")
                           .build();
        var tableType = types.getDeclaredType(elements.getTypeElement(DynamoTable.class.getCanonicalName()),
            types.getDeclaredType(entryType), attributes.get(partitionKeyAttribute).returnType,
            sortKeyAttribute == null
            ? types.getDeclaredType(elements.getTypeElement(Void.class.getCanonicalName()))
            : attributes.get(sortKeyAttribute).returnType);
        var classSpec = TypeSpec.classBuilder(entryType.getSimpleName() + "DynamoTable")
                            .addModifiers(Modifier.PUBLIC)
                            .superclass(ParameterizedTypeName.get(tableType))
                            .addMethod(buildConstructor())
                            .addMethod(buildGetKey("getPartitionKey", partitionKeyAttribute))
                            .addMethod(buildGetKey("getSortKey", sortKeyAttribute))
                            .addMethod(buildKeysToMap())
                            .addMethod(buildEncoder())
                            .addMethod(buildDecoder())
                            .addMethod(buildGetExclusiveStart())
                            .build();
        return JavaFile.builder(className, classSpec).build();
    }

    private void processGetter(ExecutableElement getter) throws TableException {
        if (getter.getAnnotation(DynamoIgnore.class) != null) {
            return; // Ignoring this field.
        }
        var getterName = getter.getSimpleName().toString();
        var getterReturnType = getter.getReturnType();
        var attributeName = getAttributeName(null, getterName);
        var attributeAnnotation = getter.getAnnotation(DynamoAttribute.class);
        if (attributeAnnotation != null) {
            attributeName = getAttributeName(attributeAnnotation.value(), getterName);
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
        }
        var prevMetadata = attributes.put(attributeName, new AttributeMetadata(getterName, getterReturnType));
        if (prevMetadata != null) {
            throw new TableException("Two getters return attribute " + attributeName, getter);
        }
    }

    private String getAttributeName(String annotationValue, String getterName) {
        return (annotationValue == null || annotationValue.isEmpty()
                ? Character.toLowerCase(getterName.charAt(3)) + getterName.substring(4)
                : annotationValue);
    }

    private MethodSpec buildConstructor() {
        return MethodSpec.constructorBuilder()
                   .addModifiers(Modifier.PUBLIC)
                   .addParameter(DynamoDbClient.class, "client")
                   .addParameter(String.class, "tableName")
                   .addStatement("super(client, tableName)")
                   .build();
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

    private MethodSpec buildKeysToMap() {
        var partitionKeyMetadata = attributes.get(partitionKeyAttribute);
        var sortKeyMetadata = (sortKeyAttribute == null ? null : attributes.get(sortKeyAttribute));
        var methodBuilder = MethodSpec.methodBuilder("keysToMap")
                                .addAnnotation(Override.class)
                                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                                .addParameter(TypeName.get(partitionKeyMetadata.returnType), "partitionValue")
                                .returns(TypeName.get(dynamoMapMirror));
        if (sortKeyMetadata == null) {
            methodBuilder.addParameter(Void.class, "sortValue");
            methodBuilder.addStatement("return $T.singletonMap($S, $T.builder().s(partitionValue).build())",
                Collections.class, partitionKeyAttribute, AttributeValue.class);
        } else {
            methodBuilder.addParameter(TypeName.get(sortKeyMetadata.returnType), "sortValue");
            methodBuilder.addStatement("return $T.of($S, $T.builder().s(partitionValue).build(), $S, $T.builder().s(sortValue).build())",
                Map.class, partitionKeyAttribute, Collections.class, AttributeValue.class, sortKeyAttribute, Collections.class, AttributeValue.class);
        }
        return methodBuilder.build();
    }

    private MethodSpec buildEncoder() throws TableException {
        var builder = MethodSpec.methodBuilder("encode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
            .returns(TypeName.get(dynamoMapMirror))
            .addStatement("$T map = new $T($L)", MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, MAP_OF_ATTRIBUTE_VALUES_CLASS_NAME, (attributes.size() * 4 + 2) / 3);
        new HashMap<String, String>(5);
        for (var entry: attributes.entrySet()) {
            var kind = entry.getValue().returnType.getKind();
            switch(kind) {
                case INT:
                case LONG:
                case BYTE:
                case FLOAT:
                case DOUBLE:
                case SHORT:
                    // Any numeric type will be catenated with a string and passed in as a number
                    builder.addStatement("map.put($S, $T.builder().n($S + value." + entry.getValue().getterName + "()).build())",
                        entry.getKey(), AttributeValue.class, "");
                    break;
                case DECLARED:
                    // Classes. May be null
                    var varName = "v" + upcaseFirst(entry.getKey());  // Prepend a "v" and upcase to make sure it is not a reserved word, or the name "map" or "value"
                    builder.addStatement("$T " + varName + " = value." + entry.getValue().getterName + "()", TypeName.get(entry.getValue().returnType));
                    builder.beginControlFlow("if (" + varName + " != null)");
                    builder.addStatement("map.put($S, $T.builder().s(" + varName + ").build())", entry.getKey(), AttributeValue.class);
                    builder.endControlFlow();
                    break;
                default:
                    throw new TableException("Unknown type kind " + kind);
            }
        }
        return builder.addStatement("return map").build();
    }

    private MethodSpec buildDecoder() throws TableException {
        var entryTypeName = TypeName.get(types.getDeclaredType(entryType));
        var builder = MethodSpec.methodBuilder("decode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(TypeName.get(dynamoMapMirror), "map")
            .returns(entryTypeName)
            .addStatement("$T result = new $T()", entryTypeName, entryTypeName);
        builder.addStatement("$T attribute", AttributeValue.class);
        for (var entry: attributes.entrySet()) {
            builder.addStatement("attribute = map.get($S)", entry.getKey());
            builder.beginControlFlow("if (attribute != null)");
            switch (entry.getValue().returnType.getKind()) {
                case INT:
                    builder.addStatement("result." + entry.getValue().setterName + "($T.parseInt(attribute.n()))", Integer.class);
                    break;
                case DECLARED:
                    builder.addStatement("result." + entry.getValue().setterName + "(attribute.s())");
                    break;
                default:
                    throw new TableException("Unknown type kind " + entry.getValue().returnType.getKind());
            }
            builder.endControlFlow();
        }
        return builder.addStatement("return result").build();
    }

    private MethodSpec buildGetExclusiveStart() {
        return MethodSpec.methodBuilder("getExclusiveStart")
                   .addAnnotation(Override.class)
                   .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                   .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
                   .returns(TypeName.get(dynamoMapMirror))
                   .addStatement("return null")
                   .build();
    }

    private String buildGetterName(String attributeName) {
        return "get" + Character.toUpperCase(attributeName.charAt(0)) + attributeName.substring(1);
    }

    private String upcaseFirst(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private static class AttributeMetadata {
        public final String getterName;
        private final String setterName;
        public final TypeMirror returnType;

        public AttributeMetadata(String getterName, TypeMirror returnType) {
            this.getterName = getterName;
            setterName = "set" + getterName.substring(3);
            this.returnType = returnType;
        }
    }
}
