package com.bardsoftware.papeeria.backend.cosmas

import org.junit.Test

import org.junit.Assert.*

class ProtoTest {
    @Test
    fun cosmasProtoHasBeenCompiled() {
        assertNotNull(CosmasProto.GetVersionResponse.getDefaultInstance())
        assertNotNull(CosmasProto.GetVersionRequest.getDefaultInstance())
    }
}
