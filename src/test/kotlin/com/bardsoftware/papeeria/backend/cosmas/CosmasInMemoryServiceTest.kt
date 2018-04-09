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

    private lateinit var service: CosmasInMemoryService

    @Before
    fun testInitialization() {
        this.service = CosmasInMemoryService()
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
        val getVersionRecorder = getStreamRecorderWithResult(0, "1")
        assertEquals(0, getVersionRecorder.values.size)
        assertNotNull(getVersionRecorder.error)
        assertEquals("INVALID_ARGUMENT: There is no file in storage with file id 1",
                getVersionRecorder.error!!.message)
    }

    @Test
    fun tryToGetFileWithWrongVersion() {
        addFileToService("file")
        val getVersionRecorder1 = getStreamRecorderWithResult(1, "0")
        val getVersionRecorder2 = getStreamRecorderWithResult(-1, "0")
        assertEquals(0, getVersionRecorder1.values.size)
        assertEquals(0, getVersionRecorder2.values.size)
        assertNotNull(getVersionRecorder1.error)
        assertNotNull(getVersionRecorder2.error)
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
        val file2ver1 = getFileFromService(1, "2")
        val file4ver1 = getFileFromService(1, "4")
        val file4ver2 = getFileFromService(2, "4")
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
        assertEquals("file3", file3.toStringUtf8())
        assertEquals("file4", file4.toStringUtf8())
        assertEquals("file2ver1", file2ver1.toStringUtf8())
        assertEquals("file4ver1", file4ver1.toStringUtf8())
        assertEquals("file4ver2", file4ver2.toStringUtf8())
    }

    @Test
    fun checkListOfVersions() {
        addFileToService("ver1", "43")
        addFileToService("ver2", "43")
        addFileToService("ver3", "43")
        assertArrayEquals(listOf(0L, 1L, 2L).toLongArray(), getVersionsList("43").toLongArray())
    }

    private fun getFileFromService(version: Long, fileId: String = "0", projectId: String = "0"): ByteString {
        return getStreamRecorderWithResult(version, fileId, projectId).values[0].file
    }

    private fun getStreamRecorderWithResult(version: Long, fileId: String = "0", projectId: String = "0"):
            StreamRecorder<CosmasProto.GetVersionResponse> {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = CosmasProto.GetVersionRequest
                .newBuilder()
                .setVersion(version)
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder
    }

    private fun addFileToService(text: String, fileId: String = "0", projectId: String = "0") {
        val createVersionRecorder: StreamRecorder<CosmasProto.CreateVersionResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.CreateVersionRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .setFile(ByteString.copyFromUtf8(text))
                .build()
        this.service.createVersion(newVersionRequest, createVersionRecorder)
    }

    private fun getStreamRecorderForVersionList(fileId: String = "0", projectId: String = "0"):
            StreamRecorder<CosmasProto.FileVersionListResponse> {
        val listOfFileVersionsRecorder: StreamRecorder<CosmasProto.FileVersionListResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.FileVersionListRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        this.service.fileVersionList(newVersionRequest, listOfFileVersionsRecorder)
        return listOfFileVersionsRecorder
    }

    private fun getVersionsList(fileId: String = "0", projectId: String = "0"): List<Long> {
        return getStreamRecorderForVersionList(fileId, projectId).values[0].versionsList
    }
}