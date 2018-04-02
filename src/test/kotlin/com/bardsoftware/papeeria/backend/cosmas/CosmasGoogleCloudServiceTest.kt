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
import io.grpc.internal.testing.StreamRecorder
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

/**
 * Some tests for CosmasGoogleCloudService
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudServiceTest {

    private var service = CosmasGoogleCloudService("papeeria-interns-cosmas")

    @Before
    fun testInitialization() {
        println()
    }

    @Test
    fun addFileAndGetFile() {
        addFileToService("file", "43")
        var file = getFileFromService(0, "43")
        assertFalse(file.isEmpty)
        assertEquals("file", file.toStringUtf8())
        service.deleteFile("43")
        file = getFileFromService(0, "43")
        assertTrue(file.isEmpty)
    }

    @Test
    fun getFileThatNotExists() {
        val file = getFileFromService(0, "43")
        assertTrue(file.isEmpty)
    }

    @Test
    fun addTwoFiles() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        var file1 = getFileFromService(0, "1")
        var file2 = getFileFromService(0, "2")
        assertFalse(file1.isEmpty)
        assertFalse(file2.isEmpty)
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
        service.deleteFile("1")
        service.deleteFile("2")
        file1 = getFileFromService(0, "1")
        file2 = getFileFromService(0, "2")
        assertTrue(file1.isEmpty)
        assertTrue(file2.isEmpty)
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