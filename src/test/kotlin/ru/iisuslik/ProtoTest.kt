package ru.iisuslik

import org.junit.Test

import org.junit.Assert.*

class ProtoTest {
    @Test
    fun cosmasOuterClassHasBeenCompiled() {
        assertEquals("CosmasOuterClass${'$'}VersionResponse",
                CosmasOuterClass.VersionResponse.getDefaultInstance().javaClass.name)
    }
}
