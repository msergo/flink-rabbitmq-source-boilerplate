package org.msergo.models;

import lombok.Data;

import java.io.Serializable;

@Data
public class CoreApiMessage implements Serializable {
    private String id;
    private Order result;
}
