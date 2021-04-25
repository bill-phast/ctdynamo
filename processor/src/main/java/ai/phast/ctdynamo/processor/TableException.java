package ai.phast.ctdynamo.processor;

import javax.lang.model.element.Element;

public class TableException extends Exception {

    private final Element element;

    public TableException(String message) {
        this(message, null);
    }

    public TableException(String message, Element sourceElement) {
        super(message);
        element = sourceElement;
    }

    public Element getElement() {
        return element;
    }
}
