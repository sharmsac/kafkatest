package com.platform.logging.cache;

import lombok.*;

@Data
@AllArgsConstructor
public class OrgConfigChangedEvent {
    private Long   orgId;
    private String oldName;
    private String newName;
}
