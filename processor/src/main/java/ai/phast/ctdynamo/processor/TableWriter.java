package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.DynamoTable;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

public class TableWriter {

    /**
     * The element declaring the type of our table entry
     */
    private final TypeElement entryType;

    /** Our element utilities */
    private final Elements elements;

    /** Our type utilities */
    private final Types types;

    private TypeMirror partitionKeyType = null;
    private String partitionKeyAttributeName = null;

    private TypeMirror sortKeyType = null;
    private String sortKeyAttributeName = null;

    /** Mirror type of Void. Used frequencly, so we build it once at constructor time */
    private final TypeMirror voidTypeMirror;

    /** Mirror type of Map<String, AttributeValue>. Used frequently, so we build it once at constructor time */
    private final TypeMirror dynamoMapMirror;

    public TableWriter(TypeElement entryType, Elements elements, Types types) {
        this.entryType = entryType;
        this.elements = elements;
        this.types = types;
        voidTypeMirror = types.getDeclaredType(elements.getTypeElement(Void.class.getCanonicalName()));
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
        if (partitionKeyType == null) {
            throw new TableException("Tables must have a getter with @DynamoPartitionKey annotation");
        }
        if (sortKeyType == null) {
            sortKeyType = voidTypeMirror;
        }
        var className = entryType.getQualifiedName() + "DynamoTable";
        var mainFunc = MethodSpec.methodBuilder("main")
                           .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                           .returns(void.class)
                           .addParameter(String[].class, "args")
                           .addStatement("$T.out.println($S)", System.class, "Hello world!")
                           .build();
        var tableType = types.getDeclaredType(elements.getTypeElement(DynamoTable.class.getCanonicalName()),
            types.getDeclaredType(entryType), partitionKeyType, sortKeyType);
        var classSpec = TypeSpec.classBuilder(entryType.getSimpleName() + "DynamoTable")
                            .addModifiers(Modifier.PUBLIC)
                            .superclass(ParameterizedTypeName.get(tableType))
                            .addMethod(buildGetKey(true, partitionKeyType, partitionKeyAttributeName))
                            .addMethod(buildGetKey(false, sortKeyType, sortKeyAttributeName))
                            .addMethod(buildEncoder())
                            .addMethod(buildDecoder())
                            .addMethod(buildGetExclusiveStart())
                            .build();
        return JavaFile.builder(className, classSpec).build();
    }

    private void processGetter(ExecutableElement getter) throws TableException {
        var name = getter.getSimpleName().toString();
        var partitionKeyAnnotation = getter.getAnnotation(DynamoPartitionKey.class);
        if (partitionKeyAnnotation != null) {
            if (partitionKeyType != null) {
                throw new TableException("Cannot have multiple partition keys", getter);
            }
            partitionKeyType = getter.getReturnType();
            partitionKeyAttributeName = getAttributeName(partitionKeyAnnotation.value(), name);
        }
        var sortKeyAnnotation = getter.getAnnotation(DynamoSortKey.class);
        if (sortKeyAnnotation != null) {
            if (sortKeyType != null) {
                throw new TableException("Cannot have multiple sort keys", getter);
            }
            sortKeyType = getter.getReturnType();
            sortKeyAttributeName = getAttributeName(sortKeyAnnotation.value(), name);
        }
    }

    private String getAttributeName(String annotationValue, String getterName) {
        return (annotationValue.isEmpty()
                ? Character.toLowerCase(getterName.charAt(3)) + getterName.substring(4)
                : annotationValue);
    }

    private MethodSpec buildGetKey(boolean isPartition, TypeMirror keyType, String attributeName) {
        return MethodSpec.methodBuilder(isPartition ? "getPartitionKey" : "getSortKey")
                   .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
            .returns(TypeName.get(keyType))
            .addStatement(keyType.equals(voidTypeMirror) ? "return null" : "return value." + buildGetterName(attributeName) + "()")
            .build();
    }

    private MethodSpec buildEncoder() {
        return MethodSpec.methodBuilder("encode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(TypeName.get(types.getDeclaredType(entryType)), "value")
            .returns(TypeName.get(dynamoMapMirror))
            .addStatement("return null")
            .build();
    }

    private MethodSpec buildDecoder() {
        return MethodSpec.methodBuilder("decode")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
            .addParameter(TypeName.get(dynamoMapMirror), "map")
            .returns(TypeName.get(types.getDeclaredType(entryType)))
            .addStatement("return null")
            .build();
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
}
