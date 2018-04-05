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

import com.google.api.gax.paging.Page
import com.google.cloud.storage.*
import com.google.cloud.storage.testing.RemoteStorageHelper
import com.google.protobuf.ByteString
import io.grpc.internal.testing.StreamRecorder
import org.junit.After
import org.junit.AfterClass
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.mockito.Matchers
import org.mockito.Matchers.eq
import org.mockito.MockSettings
import org.mockito.Mockito
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import java.io.FileInputStream


/**
 * Some tests for CosmasGoogleCloudService
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudServiceTest {

    private val bucketName = "papeeria-interns-cosmas"
    private var service = getServiceForTests()

    @Before
    fun testInitialization() {
        println()
    }

    @Test
    fun addFileAndGetFile() {
        addFileToService("file", "43")
        val file = getFileFromService(0, "43")
        assertFalse(file.isEmpty)
        assertEquals("file", file.toStringUtf8())
        this.service.deleteFile("43")
        val stream = getStreamRecorderWithResult(0, "43")
        assertEquals(0, stream.values.size)
        assertNotNull(stream.error)
    }

    @Test
    fun getFileThatNotExists() {
        val stream = getStreamRecorderWithResult(0, "43")
        assertEquals(0, stream.values.size)
        assertNotNull(stream.error)
    }

    @Test
    fun addTwoFiles() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        assertFalse(file1.isEmpty)
        assertFalse(file2.isEmpty)
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
        this.service.deleteFile("1")
        this.service.deleteFile("2")
        val stream1 = getStreamRecorderWithResult(0, "1")
        assertEquals(0, stream1.values.size)
        assertNotNull(stream1.error)
        val stream2 = getStreamRecorderWithResult(0, "2")
        assertEquals(0, stream2.values.size)
        assertNotNull(stream2.error)
    }

    @Test
    fun addSecondVersionAndCheckListVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1".toByteArray(), 1)
        val blob2 = getMockedBlob("ver2".toByteArray(), 2)
        val fakePage: Page<Blob> = mock(Page::class.java) as Page<Blob>

        Mockito.`when`(fakeStorage.create(Matchers.any(BlobInfo::class.java),
                Matchers.any(ByteArray::class.java))).thenReturn(blob1).thenReturn(blob2)

        Mockito.`when`(fakeStorage.list(eq(this.bucketName),
                Matchers.any(Storage.BlobListOption::class.java),
                Matchers.any(Storage.BlobListOption::class.java))).thenReturn(fakePage)

        Mockito.`when`(fakePage.iterateAll()).thenReturn(listOf(blob1, blob2))
        service = CosmasGoogleCloudService(this.bucketName, fakeStorage)
        addFileToService("ver1", "43")
        addFileToService("ver2", "43")
        assertArrayEquals(listOf(1L, 2L).toLongArray(), getVersionsList("43").toLongArray())
        service = getServiceForTests()
    }

    @Test
    fun getBothVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1".toByteArray(), 1444)
        val blob2 = getMockedBlob("ver2".toByteArray(), 822)
        Mockito.`when`(fakeStorage.create(Matchers.any(BlobInfo::class.java),
                Matchers.any(ByteArray::class.java))).thenReturn(blob1).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(Matchers.any(BlobId::class.java),
                Matchers.any(Storage.BlobGetOption::class.java))).
                thenReturn(blob1).thenReturn(blob2)
        service = CosmasGoogleCloudService(this.bucketName, fakeStorage)
        addFileToService("ver1", "43")
        addFileToService("ver2", "43")
        assertEquals("ver1", getFileFromService(1444, "43").toStringUtf8())
        assertEquals("ver2", getFileFromService(822, "43").toStringUtf8())
        service = getServiceForTests()
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
        service.getVersion(getVersionRequest, getVersionRecorder)
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

    private fun getServiceForTests(): CosmasGoogleCloudService {
        return CosmasGoogleCloudService(this.bucketName, LocalStorageHelper.getOptions().service)
        //return CosmasGoogleCloudService(this.bucketName)
    }

    private fun getStreamRecorderForVersionList(fileId: String = "0", projectId: String = "0"):
            StreamRecorder<CosmasProto.ListOfFileVersionsResponse> {
        val listOfFileVersionsRecorder: StreamRecorder<CosmasProto.ListOfFileVersionsResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.ListOfFileVersionsRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        this.service.listOfFileVersions(newVersionRequest, listOfFileVersionsRecorder)
        return listOfFileVersionsRecorder
    }

    private fun getVersionsList(fileId: String = "0", projectId: String = "0"): List<Long> {
        return getStreamRecorderForVersionList(fileId, projectId).values[0].versionsList
    }


    private fun getMockedBlob(file: ByteArray, generation: Long = 0): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(file)
        Mockito.`when`(blob.generation).thenReturn(generation)
        return blob
    }
}