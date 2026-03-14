package com.platform.logging.cache;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class OrgConfigChangedEventTest {

    @Test
    @DisplayName("constructor sets all fields")
    void constructorSetsFields() {
        OrgConfigChangedEvent event = new OrgConfigChangedEvent(42L, "OldName", "NewName");

        assertEquals(42L, event.getOrgId());
        assertEquals("OldName", event.getOldName());
        assertEquals("NewName", event.getNewName());
    }

    @Test
    @DisplayName("constructor allows null oldName for new orgs")
    void constructorNullOldName() {
        OrgConfigChangedEvent event = new OrgConfigChangedEvent(1L, null, "NewOrg");

        assertNull(event.getOldName());
        assertEquals("NewOrg", event.getNewName());
    }

    @Test
    @DisplayName("equals works correctly")
    void equalsWorks() {
        OrgConfigChangedEvent e1 = new OrgConfigChangedEvent(1L, "A", "B");
        OrgConfigChangedEvent e2 = new OrgConfigChangedEvent(1L, "A", "B");
        assertEquals(e1, e2);
    }

    @Test
    @DisplayName("setters work correctly")
    void settersWork() {
        OrgConfigChangedEvent event = new OrgConfigChangedEvent(1L, "A", "B");
        event.setOrgId(2L);
        event.setNewName("C");
        assertEquals(2L, event.getOrgId());
        assertEquals("C", event.getNewName());
    }
}
