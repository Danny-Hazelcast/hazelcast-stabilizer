package com.hazelcast.simulator.tests.yard;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SampleValue implements Externalizable {
    private int id;

    public SampleValue(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
    }

    @Override public String toString() {
        return "Value [id=" + id + ']';
    }
}