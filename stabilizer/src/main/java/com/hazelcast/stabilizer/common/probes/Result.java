package com.hazelcast.stabilizer.common.probes;

import javax.xml.stream.XMLStreamWriter;
import java.io.Serializable;

public interface Result<R extends Result> extends Serializable {

    R combine(R other);

    String toHumanString();

    void writeTo(XMLStreamWriter writer);
}
