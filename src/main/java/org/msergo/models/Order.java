package org.msergo.models;

import lombok.Data;
import java.util.Date;

@Data
public class Order {
    private String id;
    private String customerId;
    private long total;
    private String status;
    private Date createdAt;
    private Date updatedAt;
}
