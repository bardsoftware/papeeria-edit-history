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

import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.COSMAS_ID
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.md5Hash
import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.google.api.gax.paging.Page
import com.google.cloud.storage.*
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.protobuf.ByteString
import io.grpc.internal.testing.StreamRecorder
import name.fraser.neil.plaintext.diff_match_patch
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.mockito.Matchers.any
import org.mockito.Matchers.eq
import org.mockito.Mockito
import org.mockito.Mockito.*


/**
 * Some tests for CosmasGoogleCloudService
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudServiceTest {

    private val BUCKET_NAME = "papeeria-interns-cosmas"
    private var service = getServiceForTests()
    private val dmp = diff_match_patch()
    private val USER_ID = "1"
    private val FILE_ID = "1"
    private val PROJECT_ID = "1"

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

        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage)

        Mockito.`when`(fakePage.iterateAll())
                .thenReturn(listOf(blob1, blob2))
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1", "43")
        commit()
        createVersion("ver2", "43")
        commit()
        assertEquals(listOf(1L, 2L), getVersionsList("43"))
        verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, "43").build()),
                eq(createFileVersion("ver1").toByteArray()))
        verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, "43").build()),
                eq(createFileVersion("ver2").toByteArray()))
    }

    @Test
    fun getBothVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1444)
        val blob2 = getMockedBlob("ver2", 822)
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1", "43")
        commit()
        createVersion("ver2", "43")
        commit()
        assertEquals("ver1", getFileFromService(1444, "43"))
        assertEquals("ver2", getFileFromService(822, "43"))
        verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, "43").build()),
                eq(createFileVersion("ver1").toByteArray()))
        verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, "43").build()),
                eq(createFileVersion("ver2").toByteArray()))
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
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        createVersion("ver1")
        createVersion("ver2")
        commit()
        createVersion("ver3")
        createVersion("ver4")
        commit()
        assertEquals("ver2", getFileFromService(0))
        assertEquals("ver4", getFileFromService(1))
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(createFileVersion("ver2").toByteArray()))
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(createFileVersion("ver4").toByteArray()))
        verify(fakeStorage, never()).create(any(BlobInfo::class.java),
                eq(createFileVersion("ver1").toByteArray()))
        verify(fakeStorage, never()).create(any(BlobInfo::class.java),
                eq(createFileVersion("ver3").toByteArray()))
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


    @Test
    fun addPatch() {
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "kek", "kek lol", 2)
        addPatchToService(patch1)
        addPatchToService(patch2)
        val patchList = this.service.getPatchList(PROJECT_ID, FILE_ID)
        assertEquals(listOf(patch1, patch2), patchList)
        commit(PROJECT_ID)
        val file = getFileFromService(0, FILE_ID, PROJECT_ID)
        assertEquals("kek lol", file)
    }

    @Test
    fun addManyPatcherAtSameTime() {
        val patch2 = diffPatch(USER_ID, "", "kek", 1)
        val patch1 = diffPatch(USER_ID, "kek", "kek lol", 2)
        addPatchesToService(listOf(patch1, patch2))
        commit()
        val file = getFileFromService(0, FILE_ID, PROJECT_ID)
        assertEquals("kek lol", file)
    }

    @Test
    fun checkEmptyList() {
        val patch = diffPatch(USER_ID, "", "kek", 1)
        addPatchToService(patch)
        commit()
        assertEquals(0, this.service.getPatchList(PROJECT_ID, FILE_ID)?.size)
    }

    @Test
    fun addManyPatches() {
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "kek", "kek lol", 2)
        val patch3 = diffPatch(USER_ID, "kek lol", "kekes", 3)
        addPatchToService(patch1)
        assertEquals(listOf(patch1), this.service.getPatchList(PROJECT_ID, FILE_ID))
        addPatchToService(patch2)
        assertEquals(listOf(patch1, patch2), this.service.getPatchList(PROJECT_ID, FILE_ID))
        addPatchToService(patch3)
        assertEquals(listOf(patch1, patch2, patch3), this.service.getPatchList(PROJECT_ID, FILE_ID))
        commit()
        assertEquals("kekes", getFileFromService(0, FILE_ID, PROJECT_ID))
    }

    @Test
    fun twoCommitsWithPatches() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver2", 0)
        val blob2 = getMockedBlob("ver4", 1)
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        val patch3 = diffPatch(USER_ID, "ver2", "ver3", 3)
        val patch4 = diffPatch(USER_ID, "ver3", "ver4", 4)
        addPatchToService(patch1)
        addPatchToService(patch2)
        commit()
        addPatchToService(patch3)
        addPatchToService(patch4)
        commit()
        assertEquals("ver2", getFileFromService(0))
        assertEquals("ver4", getFileFromService(1))
        val ver1 = createFileVersion("ver2").toBuilder().addAllPatches(listOf(patch1, patch2)).build()
        val ver2 = createFileVersion("ver4").toBuilder().addAllPatches(listOf(patch3, patch4)).build()
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(ver1.toByteArray()))
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(ver2.toByteArray()))
    }

    @Test
    fun twoFilesInProjectWithPatches() {
        val patch1 = diffPatch(USER_ID, "", "file1", 1)
        val patch2 = diffPatch(USER_ID, "", "file2", 2)
        addPatchToService(patch1, "1", PROJECT_ID)
        addPatchToService(patch2, "2", PROJECT_ID)
        commit(PROJECT_ID)
        assertEquals("file1", getFileFromService(0, "1"))
        assertEquals("file2", getFileFromService(0, "2"))
    }

    @Test
    fun getLastVersionWithOneVersion() {
        val patch = diffPatch(USER_ID, "", "kek", 1)
        addPatchToService(patch)
        commit()
        assertEquals("kek", getFileFromService(-1))
    }

    @Test
    fun getLastVersionWithTwoVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 0)
        val blob2 = getMockedBlob("ver2", 1)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, FILE_ID, 0)))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, FILE_ID, 1)))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, FILE_ID, null)))).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        addPatchToService(patch2)
        commit()
        assertEquals("ver2", getFileFromService(-1))
    }


    @Test
    fun simpleForcedCommit() {
        val patch = diffPatch(USER_ID, "", "never applies", 1)
        addPatchToService(patch)
        forcedCommit("data", 2)
        assertEquals("data", getFileFromService(0))
    }

    @Test
    fun forcedCommitEmptyFile() {
        forcedCommit("data", 1)
        assertEquals("data", getFileFromService(0))
    }

    @Test
    fun forcedCommitBetweenCommits() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 0)
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch = diffPatch(USER_ID, "", "ver1", 1)
        addPatchToService(patch)
        val badFiles = commit()
        assertTrue(badFiles.isEmpty())
        val uselessPatch = diffPatch(USER_ID, "lol", "kek", 2)
        addPatchToService(uselessPatch)
        forcedCommit("ver2", 3)
        val diffPatch = diffPatch(COSMAS_ID, "ver1", "ver2", 3)
        val ver1 = createFileVersion("ver1").toBuilder().addAllPatches(listOf(patch)).build()
        val ver2 = createFileVersion("ver2").toBuilder().addAllPatches(listOf(diffPatch)).build()
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(ver1.toByteArray()))
        verify(fakeStorage).create(any(BlobInfo::class.java),
                eq(ver2.toByteArray()))
        verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, FILE_ID)))
    }

    @Test
    fun failedCommitBecauseOfBadPatch() {
        val patch = newPatch(USER_ID, "kek", 1)
        addPatchToService(patch)
        val badFiles = commit()
        val expected = FileInfo.newBuilder()
                .setProjectId(PROJECT_ID)
                .setFileId(FILE_ID)
                .build()
        assertEquals(mutableListOf(expected), badFiles)
    }

    @Test
    fun failedCommitBecauseOfWrongHash() {
        val patch = diffPatch(USER_ID, "", "lol", 1, "it's not a hash")
        addPatchToService(patch)
        val badFiles = commit()
        val expected = FileInfo.newBuilder()
                .setProjectId(PROJECT_ID)
                .setFileId(FILE_ID)
                .build()
        assertEquals(mutableListOf(expected), badFiles)
    }

    @Test
    fun getPatchSimple() {
        val listPatch1 = mutableListOf<Patch>()
        val patch1 = newPatch("-1", "patch1", 1)
        val patch2 = newPatch("-2", "patch2", 2)
        listPatch1.add(patch1)
        listPatch1.add(patch2)
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlobWithPatch("ver1", 1444, listPatch1)
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val list = this.service.getPatchListFromStorage("1", 0)
        assertEquals(2, list!!.size)
        assertEquals(newPatch("-1", "patch1", 1), list[0])
        assertEquals(newPatch("-2", "patch2", 2), list[1])
    }


    @Test
    fun getTextWithoutPatchSimple() {
        val text1 = "Hello"
        val text2 = "Hello world"
        val text3 = "Hello beautiful world"
        val text4 = "Hello beautiful life"
        val patch1 = newPatch("-", dmp.patch_toText(dmp.patch_make(text1, text2)), 1)
        val patch2 = newPatch("-", dmp.patch_toText(dmp.patch_make(text2, text3)), 2)
        val patch3 = newPatch("-", dmp.patch_toText(dmp.patch_make(text3, text4)), 3)
        val listPatch = mutableListOf(patch1, patch2, patch3)
        val blob0 = getMockedBlobWithPatch(text1, 900, mutableListOf())
        val blob1 = getMockedBlobWithPatch(text4, 1444, listPatch)
        val fakeStorage: Storage = mock(Storage::class.java)
        val fakePage: Page<Blob> = mock(Page::class.java) as Page<Blob>
        Mockito.`when`(fakePage.iterateAll())
                .thenReturn(listOf(blob1, blob0))
        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage)
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId, generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val resultText = deletePatch(fileId, "1", generation, 2)
        assertEquals("Hello life", resultText)
    }

    @Test
    fun deletePatchLongList() {
        val text1 = """Mr and Mrs Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal,
              | thank you very much. They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense.""".trimMargin().replace("\n", "")
        val text2 = """Mr and Mrs Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal,
              | thank you very much. They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr. Dursley was the director of a firm called Grunnings,
              | which made drills.""".trimMargin().replace("\n", "")
        val text3 = """Mr and Mrs Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr. Dursley was the director of a firm called Grunnings,
              | which made drills.""".trimMargin().replace("\n", "")
        val text4 = """Mr Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr. Dursley was the director of a firm called Grunnings,
              | which made drills.""".trimMargin().replace("\n", "")
        val text5 = """Mr Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Grunnings,
              | which made drills.""".trimMargin().replace("\n", "")
        val text6 = """Mr Dursley, of number four, Privet Drive, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Grunnings,
              | which made furniture.""".trimMargin().replace("\n", "")
        val text7 = """Mr Dursley, of number six, Privet Drive, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Grunnings,
              | which made furniture.""".trimMargin().replace("\n", "")
        val text8 = """Mr Dursley, of number six, Wall Street, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Grunnings,
              | which made furniture.""".trimMargin().replace("\n", "")
        val text9 = """Mr Dursley, of number six, Wall Street, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange or mysterious,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Happy,
              | which made furniture.""".trimMargin().replace("\n", "")
        val text10 = """Mr Dursley, of number six, Wall Street, were proud to say that they were perfectly normal.
              | They were the last people you'd expect to be involved in anything strange,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Happy,
              | which made furniture.""".trimMargin().replace("\n", "")
        val text11 = """Mr Dursley, of number six, Wall Street, were proud to say that they were perfectly strange.
              | They were the last people you'd expect to be involved in anything normal,
              | because they just didn't hold with such nonsense. Mr Dursley was the director of a firm called Happy,
              | which made furniture.""".trimMargin().replace("\n", "")
        val patch1 = newPatch("-", dmp.patch_toText(dmp.patch_make(text1, text2)), 1)
        val patch2 = newPatch("-", dmp.patch_toText(dmp.patch_make(text2, text3)), 2)
        val patch3 = newPatch("-", dmp.patch_toText(dmp.patch_make(text3, text4)), 3)
        val patch4 = newPatch("-", dmp.patch_toText(dmp.patch_make(text4, text5)), 4)
        val patch5 = newPatch("-", dmp.patch_toText(dmp.patch_make(text5, text6)), 5)
        val patch6 = newPatch("-", dmp.patch_toText(dmp.patch_make(text6, text7)), 6)
        val patch7 = newPatch("-", dmp.patch_toText(dmp.patch_make(text7, text8)), 7)
        val patch8 = newPatch("-", dmp.patch_toText(dmp.patch_make(text8, text9)), 8)
        val patch9 = newPatch("-", dmp.patch_toText(dmp.patch_make(text9, text10)), 9)
        val patch10 = newPatch("-", dmp.patch_toText(dmp.patch_make(text10, text11)), 10)
        val listPatch = mutableListOf<Patch>()
        listPatch.add(patch1)
        listPatch.add(patch2)
        listPatch.add(patch3)
        listPatch.add(patch4)
        listPatch.add(patch5)
        listPatch.add(patch6)
        listPatch.add(patch7)
        listPatch.add(patch8)
        listPatch.add(patch9)
        listPatch.add(patch10)
        val blob0 = getMockedBlobWithPatch(text1, 900, mutableListOf())
        val blob1 = getMockedBlobWithPatch(text11, 1444, listPatch)
        val fakeStorage: Storage = mock(Storage::class.java)
        val fakePage: Page<Blob> = mock(Page::class.java) as Page<Blob>
        Mockito.`when`(fakePage.iterateAll())
                .thenReturn(listOf(blob1, blob0))
        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage)
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId, generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val resultText = deletePatch(fileId, "1", generation, 4)
        assertEquals("""Mr Dursley, of number six, Wall Street, were proud to say that they were perfectly strange.
              | They were the last people you'd expect to be involved in anything normal,
              | because they just didn't hold with such nonsense. Mr. Dursley was the director of a firm called Happy,
              | which made furniture.""".trimMargin().replace("\n", ""), resultText)
    }

    @Test
    fun getTextWithoutPatchFileVersions() {
        val text1 = """Not for the first time, an argument had broken out over breakfast at number four,
            | Privet Drive. Mr. Vernon Dursley had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew Harry's room.""".trimMargin().replace("\n", "")
        val text2 = """Not for the first time, an argument had broken out over breakfast at number four,
            | Wall Street. Mr. Vernon Dursley had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew Harry's room.""".trimMargin().replace("\n", "")
        val text3 = """Not for the first time, an argument had broken out over breakfast at number four,
            | Wall Street. Mr. Dursley had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew Harry's room.""".trimMargin().replace("\n", "")
        val text4 = """Not for the first time, an argument had broken out over breakfast at number four,
            | Wall Street. Mr. Dursley had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew's room.""".trimMargin().replace("\n", "")
        val text5 = """Not for the first time, an argument had broken out over breakfast at number four,
            | Wall Street. Mr. Braun had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew's room.""".trimMargin().replace("\n", "")
        val patch1 = newPatch("-", dmp.patch_toText(dmp.patch_make(text1, text2)), 1)
        val patch2 = newPatch("-", dmp.patch_toText(dmp.patch_make(text2, text3)), 2)
        val patch3 = newPatch("-", dmp.patch_toText(dmp.patch_make(text3, text4)), 3)
        val patch4 = newPatch("-", dmp.patch_toText(dmp.patch_make(text4, text5)), 4)
        val blob0 = getMockedBlobWithPatch(text1, 200, mutableListOf())
        val blob1 = getMockedBlobWithPatch(text2, 300, mutableListOf(patch1))
        val blob2 = getMockedBlobWithPatch(text3, 400, mutableListOf(patch2))
        val blob3 = getMockedBlobWithPatch(text4, 500, mutableListOf(patch3))
        val blob4 = getMockedBlobWithPatch(text5, 600, mutableListOf(patch4))
        val fakeStorage: Storage = mock(Storage::class.java)
        val fakePage: Page<Blob> = mock(Page::class.java) as Page<Blob>
        Mockito.`when`(fakePage.iterateAll())
                .thenReturn(listOf(blob1, blob0, blob2, blob3, blob4))
        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId, generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(BlobId.of(this.BUCKET_NAME, fileId))).thenReturn(blob4)
        val resultText = deletePatch(fileId, "1", generation, 1)
        assertEquals("""Not for the first time, an argument had broken out over breakfast at number four,
            | Privet Drive. Mr. Braun had been woken in the early hours of the morning by a loud,
            | hooting noise from his nephew's room.""".trimMargin().replace("\n", ""), resultText)
    }

    @Test
    fun getPatchManyVersions() {
        val listPatch1 = mutableListOf<Patch>()
        val listPatch2 = mutableListOf<Patch>()
        val patch1 = newPatch("-1", "patch1", 1)
        val patch2 = newPatch("-2", "patch2", 2)
        val patch3 = newPatch("-3", "patch3", 3)
        val patch4 = newPatch("-4", "patch4", 4)
        listPatch1.add(patch1)
        listPatch1.add(patch2)
        listPatch2.add(patch3)
        listPatch2.add(patch4)
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlobWithPatch("ver1", 1444, listPatch1)
        val blob2 = getMockedBlobWithPatch("ver2", 822, listPatch2)
        Mockito.`when`(fakeStorage.get(any(BlobId::class.java))).thenReturn(blob1).thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val list1 = this.service.getPatchListFromStorage("1", 0)
        val list2 = this.service.getPatchListFromStorage("1", 1)
        assertEquals(2, list1!!.size)
        assertEquals(newPatch("-1", "patch1", 1), list1[0])
        assertEquals(newPatch("-2", "patch2", 2), list1[1])
        assertEquals(newPatch("-3", "patch3", 3), list2!![0])
        assertEquals(newPatch("-4", "patch4", 4), list2[1])
    }

    @Test
    fun deleteFileWithEmptyCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val projectId = "1"
        val fileId = "1"
        val time = 1L
        val cemeteryName = "$projectId-cemetery"
        val cemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deleteFile(projectId, fileId, "file", time)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))
        val newTomb = CosmasProto.FileTomb.newBuilder().setFileId(fileId).setFileName("file").setRemovalTimestamp(time).build()
        Mockito.verify(fakeStorage).create(eq(BlobInfo.newBuilder(this.BUCKET_NAME, cemeteryName).build()),
                eq(cemetery.toBuilder().addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deleteFileWithNullCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val projectId = "1"
        val fileId = "1"
        val time = 1L
        val cemeteryName = "$projectId-cemetery"
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))).thenReturn(null)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deleteFile(projectId, fileId, "file", time)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))
        val newTomb = CosmasProto.FileTomb.newBuilder().setFileId(fileId).setFileName("file").setRemovalTimestamp(time).build()
        Mockito.verify(fakeStorage).create(eq(BlobInfo.newBuilder(this.BUCKET_NAME, cemeteryName).build()),
                eq(FileCemetery.newBuilder().addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deletedFileList() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = "$PROJECT_ID-cemetery"
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deletedFileList(PROJECT_ID)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))
        assertEquals(DeletedFileListResponse.newBuilder().addFiles(tomb).build(), streamRecorder.values[0])
    }

    @Test
    fun restoreDeletedFileTest() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = "$PROJECT_ID-cemetery"
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val emptyCemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        restoreDeletedFile()
        Mockito.verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))
        Mockito.verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, cemeteryName).build()),
                eq(emptyCemetery.toByteArray()))
    }

    @Test
    fun restoreDeletedFileTestWithBigCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val cemeteryName = "$PROJECT_ID-cemetery"
        val tomb1 = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file1")
                .setRemovalTimestamp(1L)
                .build()

        val tomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("another file")
                .setFileName("file2")
                .setRemovalTimestamp(2L)
                .build()
        val cemetery = FileCemetery.newBuilder()
                .addCemetery(tomb1)
                .addCemetery(tomb2)
                .build()
        val expectedCemetery = FileCemetery.newBuilder().addCemetery(tomb2).build()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        restoreDeletedFile()
        Mockito.verify(fakeStorage).get(eq(BlobId.of(this.BUCKET_NAME, cemeteryName)))
        Mockito.verify(fakeStorage).create(eq(BlobInfo.newBuilder(BUCKET_NAME, cemeteryName).build()),
                eq(expectedCemetery.toByteArray()))
    }

    private fun getFileFromService(version: Long, fileId: String = FILE_ID, projectId: String = PROJECT_ID): String {
        val (getVersionRecorder, getVersionRequest) = getStreamRecorderAndRequestForGettingVersion(version, fileId, projectId)
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder.values[0].file.content.toStringUtf8()
    }

    @Test
    fun changeFileIdSimple() {
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        addPatchToService(patch1)
        commit()
        changeFileId("2")
        val patch2 = diffPatch(USER_ID, "kek", "lol", 1)
        addPatchToService(patch2, "2")
        commit()
        assertEquals("kek", getFileFromService(1))
        assertEquals("lol", getFileFromService(1, "2"))
    }

    @Test
    fun changeFileIdWithPatch() {
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "kek", "ch", 1)
        addPatchToService(patch1)
        commit()
        addPatchToService(patch2)
        changeFileId("2")
        val patch3 = diffPatch(USER_ID, "ch", "lol", 1)
        addPatchToService(patch3, "2")
        commit()
        assertEquals("kek", getFileFromService(1))
        assertEquals("lol", getFileFromService(1, "2"))
    }

    @Test
    fun changeFileIdWithVersions() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1)
        val blob2 = getMockedBlob("ver2", 2)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, FILE_ID, 1)))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, "2", 2)))).thenReturn(blob2)

        val fakePage1: Page<Blob> = mock(Page::class.java) as Page<Blob>
        val fakePage2: Page<Blob> = mock(Page::class.java) as Page<Blob>

        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage1).thenReturn(fakePage2)
        Mockito.`when`(fakePage1.iterateAll())
                .thenReturn(listOf(blob1))
        Mockito.`when`(fakePage2.iterateAll())
                .thenReturn(listOf(blob2))
        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, PROJECT_ID + "-fileIdMap"))))
                .thenReturn(null)
                .thenReturn(getMockedBlobWithFileIdMap(map))

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        changeFileId("2")
        addPatchToService(patch2, "2")
        commit()
        assertEquals(listOf(1L, 2L), getVersionsList("2"))
    }

    @Test
    fun changeFileIdTwoChanges() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1)
        val blob2 = getMockedBlob("ver2", 2)
        val blob3 = getMockedBlob("ver3", 3)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, FILE_ID, 1)))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, "2", 2)))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, "3", 3)))).thenReturn(blob3)

        val fakePage1: Page<Blob> = mock(Page::class.java) as Page<Blob>
        val fakePage2: Page<Blob> = mock(Page::class.java) as Page<Blob>
        val fakePage3: Page<Blob> = mock(Page::class.java) as Page<Blob>

        Mockito.`when`(fakeStorage.list(eq(this.BUCKET_NAME),
                any(Storage.BlobListOption::class.java), any(Storage.BlobListOption::class.java)))
                .thenReturn(fakePage1).thenReturn(fakePage2).thenReturn(fakePage3)
        Mockito.`when`(fakePage1.iterateAll())
                .thenReturn(listOf(blob1))
        Mockito.`when`(fakePage2.iterateAll())
                .thenReturn(listOf(blob2))
        Mockito.`when`(fakePage3.iterateAll())
                .thenReturn(listOf(blob3))
        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1", "3" to "2"))
                .build()
        Mockito.`when`(fakeStorage.get(eq(BlobId.of(BUCKET_NAME, PROJECT_ID + "-fileIdMap"))))
                .thenReturn(null)
                .thenReturn(getMockedBlobWithFileIdMap(map))

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        val patch3 = diffPatch(USER_ID, "ver2", "ver3", 3)
        addPatchToService(patch1)
        commit()
        changeFileId("2")
        addPatchToService(patch2, "2")
        commit()
        changeFileId("3")
        addPatchToService(patch3, "3")
        commit()
        assertEquals(listOf(1L, 2L, 3L), getVersionsList("3"))
    }

    private fun getStreamRecorderAndRequestForGettingVersion(version: Long, fileId: String = FILE_ID, projectId: String = PROJECT_ID):
            Pair<StreamRecorder<GetVersionResponse>, GetVersionRequest> {
        val getVersionRecorder: StreamRecorder<GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = GetVersionRequest
                .newBuilder()
                .setGeneration(version)
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        return Pair(getVersionRecorder, getVersionRequest)
    }

    private fun createVersion(text: String, fileId: String = FILE_ID, projectId: String = PROJECT_ID) {
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

    private fun getStreamRecorderAndRequestForVersionList(fileId: String = FILE_ID, projectId: String = PROJECT_ID):
            Pair<StreamRecorder<FileVersionListResponse>, FileVersionListRequest> {
        val listVersionsRecorder: StreamRecorder<FileVersionListResponse> = StreamRecorder.create()
        val listVersionsRequest = FileVersionListRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        return Pair(listVersionsRecorder, listVersionsRequest)
    }

    private fun getVersionsList(fileId: String = FILE_ID, projectId: String = PROJECT_ID): List<Long> {
        val (listVersionsRecorder, listVersionsRequest) = getStreamRecorderAndRequestForVersionList(fileId, projectId)
        this.service.fileVersionList(listVersionsRequest, listVersionsRecorder)
        return listVersionsRecorder.values[0].versionsList.map { e -> e.generation }
    }


    private fun getMockedBlob(fileContent: String, generation: Long = 0): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(
                FileVersion.newBuilder().setContent(
                        ByteString.copyFrom(fileContent.toByteArray())).build().toByteArray())
        Mockito.`when`(blob.generation).thenReturn(generation)
        return blob
    }

    private fun createFileVersion(fileContent: String): FileVersion {
        return FileVersion.newBuilder().setContent(
                ByteString.copyFrom(fileContent.toByteArray())).build()
    }

    private fun getMockedBlobWithPatch(fileContent: String, createTime: Long = 0, patchList: MutableList<CosmasProto.Patch>): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(FileVersion.newBuilder().addAllPatches(patchList)
                .setContent(ByteString.copyFrom(fileContent.toByteArray())).build().toByteArray())
        Mockito.`when`(blob.createTime).thenReturn(createTime)
        return blob
    }

    private fun getStreamRecorderAndRequestForCommitVersions(projectId: String = PROJECT_ID):
            Pair<StreamRecorder<CommitVersionResponse>, CommitVersionRequest> {
        val commitRecorder: StreamRecorder<CommitVersionResponse> = StreamRecorder.create()
        val commitRequest = CommitVersionRequest
                .newBuilder()
                .setProjectId(projectId)
                .build()
        return Pair(commitRecorder, commitRequest)
    }

    private fun commit(projectId: String = PROJECT_ID): MutableList<FileInfo> {
        val (commitRecorder, commitRequest) = getStreamRecorderAndRequestForCommitVersions(projectId)
        this.service.commitVersion(commitRequest, commitRecorder)
        return commitRecorder.values[0].badFilesList
    }

    private fun addPatchToService(patch: Patch, fileId: String = FILE_ID, projectId: String = PROJECT_ID) {
        addPatchesToService(listOf(patch), fileId, projectId)
    }

    private fun addPatchesToService(patches: List<Patch>, fileId: String = FILE_ID, projectId: String = PROJECT_ID) {
        val createPatchRecorder: StreamRecorder<CosmasProto.CreatePatchResponse> = StreamRecorder.create()
        val newPatchRequest = CosmasProto.CreatePatchRequest.newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .addAllPatches(patches)
                .build()
        this.service.createPatch(newPatchRequest, createPatchRecorder)
    }

    private fun deletePatch(fileId: String, projectId: String, generation: Long, patchTimestamp: Long): String {
        val deletePatchRecorder: StreamRecorder<DeletePatchResponse> = StreamRecorder.create()
        val deletePatchRequest = DeletePatchRequest
                .newBuilder()
                .setGeneration(generation)
                .setFileId(fileId)
                .setProjectId(projectId)
                .setPatchTimestamp(patchTimestamp)
                .build()
        this.service.deletePatch(deletePatchRequest, deletePatchRecorder)
        return deletePatchRecorder.values[0].content.toStringUtf8()
    }

    private fun newPatch(userId: String, text: String, timeStamp: Long): CosmasProto.Patch {
        return CosmasProto.Patch.newBuilder().setText(text).setUserId(userId).setTimestamp(timeStamp).build()
    }

    private fun deleteFile(projectId: String, fileId: String, fileName: String, time: Long): StreamRecorder<DeleteFileResponse> {
        val deleteFileRecorder: StreamRecorder<DeleteFileResponse> = StreamRecorder.create()
        val deleteFileRequest = DeleteFileRequest.newBuilder().setProjectId(projectId).setFileId(fileId).setFileName(fileName).setRemovalTimestamp(time).build()
        this.service.deleteFile(deleteFileRequest, deleteFileRecorder)
        return deleteFileRecorder
    }

    private fun deletedFileList(projectId: String): StreamRecorder<DeletedFileListResponse> {
        val deletedFileListRecorder: StreamRecorder<DeletedFileListResponse> = StreamRecorder.create()
        val deletedFileListRequest = DeletedFileListRequest.newBuilder().setProjectId(projectId).build()
        this.service.deletedFileList(deletedFileListRequest, deletedFileListRecorder)
        return deletedFileListRecorder
    }

    private fun restoreDeletedFile(fileId: String = FILE_ID, projectId: String = PROJECT_ID): StreamRecorder<RestoreDeletedFileResponse> {
        val recorder: StreamRecorder<RestoreDeletedFileResponse> = StreamRecorder.create()
        val request = RestoreDeletedFileRequest
                .newBuilder()
                .setProjectId(projectId)
                .setFileId(fileId)
                .build()
        this.service.restoreDeletedFile(request, recorder)
        return recorder
    }

    private fun changeFileId(newFileId: String, oldFileId: String = FILE_ID, projectId: String = PROJECT_ID): StreamRecorder<ChangeFileIdResponse> {
        val recorder: StreamRecorder<ChangeFileIdResponse> = StreamRecorder.create()
        val change = ChangeId.newBuilder()
                .setNewFileId(newFileId)
                .setOldFileId(oldFileId)
                .build()
        val request = ChangeFileIdRequest.newBuilder()
                .setProjectId(projectId)
                .addChanges(change)
                .build()
        this.service.changeFileId(request, recorder)
        return recorder
    }

    private fun getMockedBlobWithCemetery(cemetery: FileCemetery): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(cemetery.toByteArray())
        return blob
    }

    private fun getMockedBlobWithFileIdMap(fileIdMap: FileIdMap): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(fileIdMap.toByteArray())
        return blob
    }

    private fun diffPatch(userId: String, text1: String, text2: String, timeStamp: Long): Patch {
        val text = dmp.patch_toText(dmp.patch_make(text1, text2))
        return CosmasProto.Patch
                .newBuilder()
                .setText(text)
                .setUserId(userId)
                .setTimestamp(timeStamp)
                .setActualHash(md5Hash(text2))
                .build()
    }

    private fun diffPatch(userId: String, text1: String, text2: String, timeStamp: Long, hash: String): Patch {
        val text = dmp.patch_toText(dmp.patch_make(text1, text2))
        return CosmasProto.Patch
                .newBuilder()
                .setText(text)
                .setUserId(userId)
                .setTimestamp(timeStamp)
                .setActualHash(hash)
                .build()
    }

    private fun forcedCommit(content: String, timestamp: Long,
                             fileId: String = FILE_ID, projectId: String = PROJECT_ID) {
        val recorder: StreamRecorder<ForcedFileCommitResponse> = StreamRecorder.create()
        val request = ForcedFileCommitRequest.newBuilder()
                .setProjectId(projectId)
                .setFileId(fileId)
                .setTimestamp(timestamp)
                .setActualContent(ByteString.copyFrom(content.toByteArray()))
                .build()
        this.service.forcedFileCommit(request, recorder)
    }
}