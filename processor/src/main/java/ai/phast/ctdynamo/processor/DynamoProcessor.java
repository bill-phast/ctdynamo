package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.annotations.DynamoDoc;
import com.google.auto.service.AutoService;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import java.io.IOException;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
public class DynamoProcessor extends AbstractProcessor {

    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        messager = processingEnv.getMessager();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(DynamoDoc.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
        for (var element: roundEnvironment.getElementsAnnotatedWith(DynamoDoc.class)) {
            if (element.getKind() != ElementKind.CLASS) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Only classes can have annotation " + DynamoDoc.class.getSimpleName(), element);
                return true;
            }
            var type = (TypeElement)element;
            messager.printMessage(Diagnostic.Kind.NOTE, "This is my note on processing " + element);
            var className = type.getQualifiedName() + "DynamoTable";
            var mainFunc = MethodSpec.methodBuilder("main")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(void.class)
                .addParameter(String[].class, "args")
                .addStatement("$T.out.println($S)", System.class, "Hello world!")
                .build();
            var classSpec = TypeSpec.classBuilder(type.getSimpleName() + "DynamoTable")
                .addModifiers(Modifier.PUBLIC)
                .addMethod(mainFunc)
                .build();
            var fileBuilder = JavaFile.builder(className, classSpec).build();
            try {
                fileBuilder.writeTo(processingEnv.getFiler());
            } catch (IOException excep) {
                throw new RuntimeException(excep);
            }
        }
        return true;
    }
}
