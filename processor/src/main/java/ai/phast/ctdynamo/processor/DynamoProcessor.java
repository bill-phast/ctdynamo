package ai.phast.ctdynamo.processor;

import ai.phast.ctdynamo.annotations.DynamoItem;
import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
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
        return Set.of(DynamoItem.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
        for (var element : roundEnvironment.getElementsAnnotatedWith(DynamoItem.class)) {
            if (element.getKind() != ElementKind.CLASS) {
                messager.printMessage(Diagnostic.Kind.ERROR, "Only classes can have annotation " + DynamoItem.class.getSimpleName(), element);
                return true;
            }
            var annotation = element.getAnnotation(DynamoItem.class);
            try {
                var writer = new TableWriter((TypeElement)element, processingEnv.getElementUtils(), processingEnv.getTypeUtils(),
                    annotation.ignoreNulls());
                if (Arrays.asList(annotation.value()).contains(DynamoItem.Output.TABLE)) {
                    writer.buildTableClass().writeTo(processingEnv.getFiler());
                }
                if (Arrays.asList(annotation.value()).contains(DynamoItem.Output.CODEC)) {
                    writer.buildCodecClass().writeTo(processingEnv.getFiler());
                }
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
