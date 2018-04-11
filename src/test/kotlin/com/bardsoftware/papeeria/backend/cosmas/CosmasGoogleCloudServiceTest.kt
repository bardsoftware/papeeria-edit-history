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
import com.google.protobuf.ByteString
import io.grpc.internal.testing.StreamRecorder
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.mockito.Matchers.any
import org.mockito.Matchers.eq
import org.mockito.Mockito
import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import org.mockito.Mockito.*


/**
 * Some tests for CosmasGoogleCloudService
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudServiceTest {

    private val BUCKET_NAME = "papeeria-interns-cosmas"
    private var service = getServiceForTests()

    @Before
    fun testInitialization() {
        this.service = getServiceForTests()
        println()
    }

    @Test
    fun addFileAndGetFile() {
        createVersion("file", "43")
        commit()
        val file = getFileFromService(0, "43")
        assertEquals("file", file)
        this.service.deleteFile("43")
        val (stream, request) = getStreamRecorderAndRequestForGettingVersion(0, "43")
        this.service.getVersion(request, stream)
        assertEquals(0, stream.values.size)
        assertNotNull(stream.error)
    }

    @Test
    fun getFileThatNotExists() {
        val (stream, request) = getStreamRecorderAndRequestForGettingVersion(0, "43")
        this.service.getVersion(request, stream)
        assertEquals(0, stream.values.size)
        assertNotNull(stream.error)
    }

    @Test
    fun addTwoFiles() {
        createVersion("file1", "1")
        createVersion("file2", "2")
        commit()
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        assertEquals("file1", file1)
        assertEquals("file2", file2)
        this.service.deleteFile("1")
        this.service.deleteFile("2")
        val (stream1, request1) = getStreamRecorderAndRequestForGettingVersion(0, "1")
        this.service.getVersion(request1, stream1)
        assertEquals(0, stream1.values.size)
        assertNotNull(stream1.error)
        val (stream2, request2) = getStreamRecorderAndRequestForGettingVersion(0, "2")
        this.service.getVersion(request2, stream2)
        assertEquals(0, stream2.values.size)
        assertNotNull(stream2.error)
    }

    @Test
    fun addSecondVersionAndCheckListVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1)
        val blob2 = getMockedBlob("ver2", 2)
        val fakePage: Page<Blob> = mock(Page::class.java) as Page<Blob>

        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), any(ByteArray::class.java)))
                .thenReturn(blob1).thenReturn(blob2)

        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage)

        Mockito.`when`(fakePage.iterateAll())
                .thenReturn(listOf(blob1, blob2))
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1", "43")
        createVersion("ver2", "43")
        commit()
        assertEquals(listOf(1L, 2L), getVersionsList("43"))
    }

    @Test
    fun getBothVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1444)
        val blob2 = getMockedBlob("ver2", 822)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), any(ByteArray::class.java)))
                .thenReturn(blob1).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(
                any(BlobId::class.java), any(Storage.BlobGetOption::class.java)))
                .thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1", "43")
        commit()
        createVersion("ver2", "43")
        commit()
        assertEquals("ver1", getFileFromService(1444, "43"))
        assertEquals("ver2", getFileFromService(822, "43"))
    }

    @Test
    fun handleStorageException() {
        val fakeStorage: Storage = mock(Storage::class.java)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), any(ByteArray::class.java)))
                .thenThrow(StorageException(1, "test"))
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("file")
        val (commitRecorder, commitRequest) = getStreamRecorderAndRequestForCommitVersions()
        this.service.commitVersion(commitRequest, commitRecorder)
        assertNotNull(commitRecorder.error)
        assertEquals("test", commitRecorder.error!!.message)
    }

    @Test
    fun addTwoVersionsAndOneCommit() {
        createVersion("ver0")
        createVersion("ver1")
        commit()
        assertEquals("ver1", getFileFromService(0))
    }

    @Test
    fun makeTwoCommits() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver2", 0)
        val blob2 = getMockedBlob("ver4", 1)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), eq("ver2".toByteArray())))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), eq("ver4".toByteArray())))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(
                any(BlobId::class.java), any(Storage.BlobGetOption::class.java)))
                .thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1")
        createVersion("ver2")
        commit()
        createVersion("ver3")
        createVersion("ver4")
        commit()
        assertEquals("ver2", getFileFromService(0))
        assertEquals("ver4", getFileFromService(1))
        verify(fakeStorage, never()).create(any(BlobInfo::class.java), eq("ver1".toByteArray()))
        verify(fakeStorage, never()).create(any(BlobInfo::class.java), eq("ver3".toByteArray()))
    }

    @Test
    fun addTwoFilesFromOneProject() {
        createVersion("file0", "0")
        createVersion("file1", "1")
        commit()
        assertEquals("file0", getFileFromService(0, "0"))
        assertEquals("file1", getFileFromService(0, "1"))
    }

    @Test
    fun addTwoFilesFromDifferentProjects() {
        createVersion("file0", "0", "0")
        createVersion("file1", "1", "1")
        commit("0")
        assertEquals("file0", getFileFromService(0, "0"))
        val (stream, request) = getStreamRecorderAndRequestForGettingVersion(0, "1", "1")
        this.service.getVersion(request, stream)
        assertNotNull(stream.error)
    }

    private fun getFileFromService(version: Long, fileId: String = "0", projectId: String = "0"): String {
        val (getVersionRecorder, getVersionRequest) = getStreamRecorderAndRequestForGettingVersion(version, fileId, projectId)
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder.values[0].file.toStringUtf8()
    }

    private fun getStreamRecorderAndRequestForGettingVersion(version: Long, fileId: String = "0", projectId: String = "0"):
            Pair<StreamRecorder<GetVersionResponse>, GetVersionRequest> {
        val getVersionRecorder: StreamRecorder<GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = GetVersionRequest
                .newBuilder()
                .setVersion(version)
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        return Pair(getVersionRecorder, getVersionRequest)
    }

    private fun createVersion(text: String, fileId: String = "0", projectId: String = "0") {
        val createVersionRecorder: StreamRecorder<CreateVersionResponse> = StreamRecorder.create()
        val newVersionRequest = CreateVersionRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .setFile(ByteString.copyFromUtf8(text))
                .build()
        this.service.createVersion(newVersionRequest, createVersionRecorder)
    }

    private fun getServiceForTests(): CosmasGoogleCloudService {
        return CosmasGoogleCloudService(this.BUCKET_NAME, LocalStorageHelper.getOptions().service)
    }

    private fun getStreamRecorderAndRequestForVersionList(fileId: String = "0", projectId: String = "0"):
            Pair<StreamRecorder<FileVersionListResponse>, FileVersionListRequest> {
        val listVersionsRecorder: StreamRecorder<FileVersionListResponse> = StreamRecorder.create()
        val listVersionsRequest = FileVersionListRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        return Pair(listVersionsRecorder, listVersionsRequest)
    }

    private fun getVersionsList(fileId: String = "0", projectId: String = "0"): List<Long> {
        val (listVersionsRecorder, listVersionsRequest) = getStreamRecorderAndRequestForVersionList(fileId, projectId)
        this.service.fileVersionList(listVersionsRequest, listVersionsRecorder)
        return listVersionsRecorder.values[0].versionsList
    }


    private fun getMockedBlob(fileContent: String, generation: Long = 0): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(fileContent.toByteArray())
        Mockito.`when`(blob.generation).thenReturn(generation)
        return blob
    }

    private fun getStreamRecorderAndRequestForCommitVersions(projectId: String = "0"):
            Pair<StreamRecorder<CommitVersionResponse>, CommitVersionRequest> {
        val commitRecorder: StreamRecorder<CommitVersionResponse> = StreamRecorder.create()
        val commitRequest = CommitVersionRequest
                .newBuilder()
                .setProjectId(projectId)
                .build()

        return Pair(commitRecorder, commitRequest)
    }

    private fun commit(projectId: String = "0") {
        val (commitRecorder, commitRequest) = getStreamRecorderAndRequestForCommitVersions(projectId)
        this.service.commitVersion(commitRequest, commitRecorder)
    }
}