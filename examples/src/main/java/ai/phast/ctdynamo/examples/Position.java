package ai.phast.ctdynamo.examples;

import ai.phast.ctdynamo.annotations.DynamoItem;

@DynamoItem(DynamoItem.Output.CODEC)
public class Position {

    private int x;

    private int y;

    public int getX() {
        return x;
    }

    public void setX(int value) {
        x = value;
    }

    public int getY() {
        return y;
    }

    public void setY(int value) {
        y = value;
    }
}
