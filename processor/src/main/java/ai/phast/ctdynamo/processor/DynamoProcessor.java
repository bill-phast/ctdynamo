package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.DynamoTable;
import ai.phast.ctdynamo.annotations.DynamoDoc;
import ai.phast.ctdynamo.annotations.DynamoPartitionKey;
import ai.phast.ctdynamo.annotations.DynamoSortKey;
import com.google.auto.service.AutoService;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
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
        for (var element : roundEnvironment.getElementsAnnotatedWith(DynamoDoc.class)) {
            if (element.getKind() != ElementKind.CLASS) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Only classes can have annotation " + DynamoDoc.class.getSimpleName(), element);
                return true;
            }
            try {
                var writer = new TableWriter((TypeElement)element, processingEnv.getElementUtils(), processingEnv.getTypeUtils());
                writer.buildJavaFile(false).writeTo(processingEnv.getFiler());
                writer.buildJavaFile(true).writeTo(processingEnv.getFiler());
            } catch (TableException e) {
                messager.printMessage(Diagnostic.Kind.ERROR, e.getMessage(), e.getElement() == null ? element : e.getElement());
            } catch (IOException e) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Error writing table: " + e.toString(), element);
            } catch (Exception e) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Failure to process: " + e.toString() + Arrays.asList(e.getStackTrace()), element);
            }
        }
        return true;
    }
}
