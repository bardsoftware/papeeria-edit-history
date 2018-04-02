/**
Copyright 2018 BarD Software s.r.o

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.bardsoftware.papeeria.backend.cosmas

import com.google.protobuf.ByteString
import org.junit.Assert.*
import org.junit.Test
import io.grpc.internal.testing.StreamRecorder
import org.junit.Before
/**
 * This is some tests for CosmasService class
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasInMemoryServiceTest {

    private var service = CosmasInMemoryService()

    @Before
    fun testInitialization() {
        service = CosmasInMemoryService()
        println()
    }

    @Test
    fun addOneVersion() {
        addFileToService("Here comes the sun")
        val file = getFileFromService(0)
        assertFalse(file.isEmpty)
        assertTrue(file.isValidUtf8)
        assertEquals("Here comes the sun", file.toStringUtf8())
    }

    @Test
    fun addSecondVersion() {
        addFileToService("Here comes the sun")
        addFileToService("Little darling, it's been a long cold lonely winter")
        val file0 = getFileFromService(0)
        val file1 = getFileFromService(1)
        assertFalse(file0.isEmpty)
        assertFalse(file1.isEmpty)
        assertTrue(file0.isValidUtf8 && file1.isValidUtf8)
        assertEquals("Here comes the sun", file0.toStringUtf8())
        assertEquals("Little darling, it's been a long cold lonely winter", file1.toStringUtf8())
    }

    @Test
    fun addSecondFile() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        assertFalse(file1.isEmpty)
        assertFalse(file2.isEmpty)
        assertTrue(file1.isValidUtf8 && file2.isValidUtf8)
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
    }

    @Test
    fun tryToGetFileWithWrongId() {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = CosmasProto.GetVersionRequest
                .newBuilder()
                .setVersion(0)
                .setFileId("1")
                .setProjectId("1")
                .build()
        service.getVersion(getVersionRequest, getVersionRecorder)
        val file = getVersionRecorder.values[0].file
        assertTrue(file.isEmpty)
        assertNotNull(getVersionRecorder.error)
        assertEquals("INVALID_ARGUMENT: There is no file in storage with file id 1",
                getVersionRecorder.error!!.message)
    }

    @Test
    fun tryToGetFileWithWrongVersion() {
        addFileToService("file")
        getFileFromService(1, "0")
        getFileFromService(-1, "0")
    }

    @Test
    fun addManyFilesAndManyVersions() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        addFileToService("file3", "3")
        addFileToService("file4", "4")
        addFileToService("file2ver1", "2")
        addFileToService("file4ver1", "4")
        addFileToService("file4ver2", "4")
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        val file3 = getFileFromService(0, "3")
        val file4 = getFileFromService(0, "4")
        val file2_1 = getFileFromService(1, "2")
        val file4_1 = getFileFromService(1, "4")
        val file4_2 = getFileFromService(2, "4")
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
        assertEquals("file3", file3.toStringUtf8())
        assertEquals("file4", file4.toStringUtf8())
        assertEquals("file2ver1", file2_1.toStringUtf8())
        assertEquals("file4ver1", file4_1.toStringUtf8())
        assertEquals("file4ver2", file4_2.toStringUtf8())
    }

    private fun getFileFromService(version: Int, fileId: String = "0", projectId: String = "0"): ByteString {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = CosmasProto.GetVersionRequest
                .newBuilder()
                .setVersion(version)
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder.values[0].file
    }

    private fun addFileToService(text: String, fileId: String = "0", projectId: String = "0") {
        val createVersionRecorder: StreamRecorder<CosmasProto.CreateVersionResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.CreateVersionRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .setFile(ByteString.copyFromUtf8(text))
                .build()
        service.createVersion(newVersionRequest, createVersionRecorder)
    }
}