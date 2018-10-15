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
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.internal.testing.StreamRecorder
import name.fraser.neil.plaintext.diff_match_patch
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Matchers.eq
import org.mockito.Mockito
import org.mockito.Mockito.*
import java.time.Clock


/**
 * Some tests for CosmasGoogleCloudService
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudServiceTest {

    private val BUCKET_NAME = "papeeria-interns-cosmas"
    private val FREE_BUCKET_NAME = "papeeria-free"
    private val PAID_BUCKET_NAME = "papeeria-paid"
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
    fun getFileThatNotExists() {
        val (stream, request) = getStreamRecorderAndRequestForGettingVersion(0, "43")
        this.service.getVersion(request, stream)
        assertEquals(0, stream.values.size)
        assertNotNull(stream.error)
    }


    @Test
    fun handleStorageException() {
        val fakeStorage: Storage = mock(Storage::class.java)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), any(ByteArray::class.java)))
                .thenThrow(StorageException(1, "test"))
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch = diffPatch(USER_ID, "", "lol", 1L)
        addPatchToService(patch)
        val (commitRecorder, commitRequest) = getStreamRecorderAndRequestForCommitVersions()
        this.service.commitVersion(commitRequest, commitRecorder)
        assertNotNull(commitRecorder.error)
        assertEquals("test", commitRecorder.error!!.message)
    }


    @Test
    fun uselessCommit() {
        val blob = getMockedBlob("lol", 1L)
        val fakeStorage: Storage = mock(Storage::class.java)
        Mockito.`when`(fakeStorage.create(
                any(BlobInfo::class.java), any(ByteArray::class.java)))
                .thenReturn(blob)
                .thenReturn(null)
                .thenThrow(StorageException(1, "test failed!!!"))

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch = diffPatch(USER_ID, "", "lol", 1L)
        addPatchToService(patch)
        commit()
        commit()
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
    fun addPatchAndCommitPaidPlan() {
        service = getServiceForTestsWithPlans()
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "kek", "kek lol", 2)
        addPatchToService(patch1, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        addPatchToService(patch2, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        commit(projectInfo(PROJECT_ID, USER_ID, false))
        val file = getFileFromService(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        val (getVersionRecorder, getVersionRequest) =
                getStreamRecorderAndRequestForGettingVersion(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, true))
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        assertNotNull(getVersionRecorder.error)
        assertEquals("kek lol", file)
    }

    @Test
    fun twoFilesWithDifferentPlan() {
        service = getServiceForTestsWithPlans()
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "", "lol", 2)
        addPatchToService(patch1, "1", projectInfo(PROJECT_ID, USER_ID, false))
        addPatchToService(patch2, "2", projectInfo(PROJECT_ID, USER_ID, true))
        commit(projectInfo(PROJECT_ID, USER_ID, false))
        commit(projectInfo(PROJECT_ID, USER_ID, true))
        val file1 = getFileFromService(0, "1", projectInfo(PROJECT_ID, USER_ID, false))
        val file2 = getFileFromService(0, "2", projectInfo(PROJECT_ID, USER_ID, false))
        assertEquals("kek", file1)
        assertEquals("lol", file2)
    }

    @Test
    fun addManyPatchesAtSameTime() {
        val patch2 = diffPatch(USER_ID, "", "kek", 1)
        val patch1 = diffPatch(USER_ID, "kek", "kek lol", 2)
        addPatchesToService(listOf(patch1, patch2))
        commit()
        val file = getFileFromService(0, FILE_ID, PROJECT_ID)
        assertEquals("kek lol", file)
    }

    @Test
    fun addPatchesDifferentProjects() {
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "", "lol", 2)
        addPatchToService(patch1, "1", "1")
        addPatchToService(patch2, "2", "2")
        commit("1")
        commit("2")
        val file1 = getFileFromService(0, "1", "1")
        val file2 = getFileFromService(0, "2", "2")
        assertEquals("kek", file1)
        assertEquals("lol", file2)
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 0L))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1L))).thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1).thenReturn(blob2)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
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
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver1.toByteArray()))
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0)))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1)))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), null)))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1).thenReturn(blob2)
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
    fun forcedCommitPaid() {
        service = getServiceForTestsWithPlans()
        val patch = diffPatch(USER_ID, "", "never applies", 1)
        addPatchToService(patch, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        forcedCommit("data", 2, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        getFileAssertError(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, true))
        assertEquals("data", getFileFromService(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false)))
    }

    private fun getFileAssertError(version: Long, fileId: String = FILE_ID, info: ProjectInfo = projectInfo()) {
        val (getVersionRecorder, getVersionRequest) =
                getStreamRecorderAndRequestForGettingVersion(version, fileId, info)
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        assertNotNull(getVersionRecorder.error)
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo())))).thenReturn(blob1)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
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
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver1.toByteArray()))
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver2.toByteArray()))
        verify(fakeStorage).get(eq(service.getBlobId(FILE_ID, projectInfo())))
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0L)))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val list = this.service.getPatchListFromStorage(FILE_ID, 0L, projectInfo())
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
                eq(Storage.BlobListOption.versions(true)),
                eq(Storage.BlobListOption.prefix(service.fileStorageName(FILE_ID, projectInfo())))))
                .thenReturn(fakePage)
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo()))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val resultText = deletePatch(fileId, generation, 2)
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
                eq(Storage.BlobListOption.versions(true)),
                eq(Storage.BlobListOption.prefix(service.fileStorageName(FILE_ID, projectInfo())))))
                .thenReturn(fakePage)
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo()))).thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val resultText = deletePatch(fileId, generation, 4)
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
                eq(Storage.BlobListOption.versions(true)),
                eq(Storage.BlobListOption.prefix(service.fileStorageName(FILE_ID, projectInfo())))))
                .thenReturn(fakePage)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val generation = 43L
        val fileId = "1"
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo()))).thenReturn(blob4)
        val resultText = deletePatch(fileId, generation, 1)
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0L))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1L))))
                .thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val list1 = this.service.getPatchListFromStorage("1", 0, projectInfo())
        val list2 = this.service.getPatchListFromStorage("1", 1, projectInfo())
        assertEquals(2, list1!!.size)
        assertEquals(newPatch("-1", "patch1", 1), list1[0])
        assertEquals(newPatch("-2", "patch2", 2), list1[1])
        assertEquals(newPatch("-3", "patch3", 3), list2!![0])
        assertEquals(newPatch("-4", "patch4", 4), list2[1])
    }

    @Test
    fun deleteFileWithEmptyCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = "$PROJECT_ID-cemetery"
        val cemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo())))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deleteFile(FILE_ID, "file", time)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        val newTomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(FileCemetery.newBuilder().addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deleteFileWithNullCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = "$PROJECT_ID-cemetery"
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo())))).thenReturn(null)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deleteFile(FILE_ID, "file", time)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        val newTomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(FileCemetery.newBuilder().addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deleteFilePlan() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = "$PROJECT_ID-cemetery"
        this.service = CosmasGoogleCloudService(this.FREE_BUCKET_NAME, this.PAID_BUCKET_NAME, fakeStorage)
        val streamRecorder = deleteFile(FILE_ID, "file", time, projectInfo(PROJECT_ID, USER_ID, false))
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo(PROJECT_ID, USER_ID, false))))
        val newTomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo(PROJECT_ID, USER_ID, false))),
                eq(FileCemetery.newBuilder().addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deletedFileListPlan() {
        val tomb1 = CosmasProto.FileTomb.newBuilder()
                .setFileId("1")
                .setFileName("file1")
                .setRemovalTimestamp(1)
                .build()

        val tomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("2")
                .setFileName("file2")
                .setRemovalTimestamp(2)
                .build()
        service = getServiceForTestsWithPlans()
        deleteFile("1", "file1", 1, projectInfo(PROJECT_ID, USER_ID, true))
        deleteFile("2", "file2", 2, projectInfo(PROJECT_ID, USER_ID, false))
        assertEquals(DeletedFileListResponse.newBuilder().addFiles(tomb1).build(),
                deletedFileList(projectInfo(PROJECT_ID, USER_ID, true)).values[0])
        assertEquals(DeletedFileListResponse.newBuilder().addFiles(tomb2).build(),
                deletedFileList(projectInfo(PROJECT_ID, USER_ID, false)).values[0])
    }

    @Test
    fun deletedFileListTest() {
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo())))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deletedFileList()
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo())))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        restoreDeletedFile()
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(emptyCemetery.toByteArray()))
    }

    @Test
    fun restoreDeletedFilePlan() {
        val tomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("2")
                .setFileName("file2")
                .setRemovalTimestamp(2)
                .build()
        service = getServiceForTestsWithPlans()
        deleteFile("1", "file1", 1, projectInfo(PROJECT_ID, USER_ID, true))
        deleteFile("2", "file2", 2, projectInfo(PROJECT_ID, USER_ID, false))
        restoreDeletedFile("1", projectInfo(PROJECT_ID, USER_ID, true))

        assertEquals(DeletedFileListResponse.getDefaultInstance(),
                deletedFileList(projectInfo(PROJECT_ID, USER_ID, true)).values[0])
        assertEquals(DeletedFileListResponse.newBuilder().addFiles(tomb2).build(),
                deletedFileList(projectInfo(PROJECT_ID, USER_ID, false)).values[0])
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo())))).thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        restoreDeletedFile()
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(expectedCemetery.toByteArray()))
    }

    private fun getFileFromService(version: Long, fileId: String = FILE_ID, projectId: String = PROJECT_ID): String {
        val (getVersionRecorder, getVersionRequest) =
                getStreamRecorderAndRequestForGettingVersion(version, fileId, projectInfo(projectId))
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder.values[0].file.content.toStringUtf8()
    }

    private fun getFileFromService(version: Long, fileId: String = FILE_ID, info: ProjectInfo): String {
        val (getVersionRecorder, getVersionRequest) =
                getStreamRecorderAndRequestForGettingVersion(version, fileId, info)
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
        val blob1 = getMockedBlob("ver1", 1, 1)
        val blob2 = getMockedBlob("ver2", 2, 2)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId("2", projectInfo(), 2))).thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob2)


        val fileIdVersionsInfo1 = getMockedBlobWithFileIdVersionsInfo(listOf(FileVersionInfo.newBuilder()
                .setFileId(FILE_ID)
                .setGeneration(1)
                .setTimestamp(0)
                .build()))

        val fileIdVersionsInfo2 = getMockedBlobWithFileIdVersionsInfo(listOf(FileVersionInfo.newBuilder()
                .setFileId("2")
                .setGeneration(2)
                .setTimestamp(0)
                .build()))

        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-fileIdMap", projectInfo()))))
                .thenReturn(null)
                .thenReturn(getMockedBlobWithFileIdMap(map))


        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(fileIdVersionsInfo1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-2-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(fileIdVersionsInfo2)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        changeFileId("2")
        addPatchToService(patch2, "2")
        commit()
        assertEquals(listOf(2L, 1L), getVersionsList("2"))
    }

    @Test
    fun changeFileIdTwoChanges() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1, 1)
        val blob2 = getMockedBlob("ver2", 2, 2)
        val blob3 = getMockedBlob("ver3", 3, 3)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1)))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("2", projectInfo(), 2)))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("3", projectInfo(), 3)))).thenReturn(blob3)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob2)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("3", projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob3)

        val fileIdVersionsInfo1 = getMockedBlobWithFileIdVersionsInfo(listOf(FileVersionInfo.newBuilder()
                .setFileId(FILE_ID)
                .setGeneration(1)
                .setTimestamp(0)
                .build()))

        val fileIdVersionsInfo2 = getMockedBlobWithFileIdVersionsInfo(listOf(FileVersionInfo.newBuilder()
                .setFileId("2")
                .setGeneration(2)
                .setTimestamp(0)
                .build()))

        val fileIdVersionsInfo3 = getMockedBlobWithFileIdVersionsInfo(listOf(FileVersionInfo.newBuilder()
                .setFileId("3")
                .setGeneration(3)
                .setTimestamp(0)
                .build()))

        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1", "3" to "2"))
                .build()
        val mapBlob = getMockedBlobWithFileIdMap(map)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(PROJECT_ID + "-fileIdMap", projectInfo()))))
                .thenReturn(null)
                .thenReturn(mapBlob)

        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(fileIdVersionsInfo1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-2-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(fileIdVersionsInfo2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("$PROJECT_ID-3-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(fileIdVersionsInfo3)

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
        assertEquals(listOf(3L, 2L, 1L), getVersionsList("3"))
    }

    @Test
    fun commitSavesVersionList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1, 0)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1)))
                .thenReturn(blob1)

        val patch1 = diffPatch(USER_ID, "", "ver1", 0)
        val newVersion = FileVersion.newBuilder()
                .setContent(ByteString.copyFrom("ver1".toByteArray()))
                .setTimestamp(0)
                .addPatches(patch1)
                .build()
        Mockito.`when`(fakeStorage.create(service.getBlobInfo(FILE_ID, projectInfo()), newVersion.toByteArray()))
                .thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        addPatchToService(patch1)
        commit()
        val versionInfo = FileVersionInfo.newBuilder()
                .setFileId(FILE_ID)
                .setGeneration(1L)
                .setTimestamp(0L)
                .build()
        verify(fakeStorage).create(
                eq(service.getBlobInfo("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())),
                eq(FileIdVersionsInfo.newBuilder()
                        .addVersionInfo(versionInfo)
                        .build()
                        .toByteArray()))
    }

    @Test
    fun correctVersionList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1)
        val blob2 = getMockedBlob("ver2", 2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 2))))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))).thenReturn(blob1).thenReturn(blob2)

        val versionInfo1 = FileVersionInfo.newBuilder()
                .setFileId(FILE_ID)
                .setGeneration(1L)
                .setTimestamp(0L)
                .build()
        val versionInfo2 = FileVersionInfo.newBuilder()
                .setFileId(FILE_ID)
                .setGeneration(2L)
                .setTimestamp(0L)
                .build()

        val versionsInfo1 = FileIdVersionsInfo.newBuilder()
                .addAllVersionInfo(listOf(versionInfo1))
                .build()

        val versionsInfo2 = FileIdVersionsInfo.newBuilder()
                .addAllVersionInfo(listOf(versionInfo1, versionInfo2))
                .build()

        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent())
                .thenReturn(versionsInfo1.toByteArray())
                .thenReturn(versionsInfo2.toByteArray())

        `when`(fakeStorage.get(
                eq(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo()))))
                .thenReturn(null).thenReturn(blob)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        addPatchToService(patch2)
        commit()
        assertEquals(listOf(2L, 1L), getVersionsList())
        verify(fakeStorage, times(2)).create(
                eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))
        verify(fakeStorage).create(
                eq(service.getBlobInfo("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())),
                eq(versionsInfo1.toByteArray()))
        verify(fakeStorage).create(
                eq(service.getBlobInfo("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())),
                eq(versionsInfo2.toByteArray()))

    }

    @Test
    fun requestForThreeVersionsOutOfThree() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob = getMockedBlobWithFileVersions(3)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob)
        assertEquals(listOf(3L, 2L, 1L), getVersionsList(FILE_ID, PROJECT_ID, 3))
    }

    @Test
    fun requestForMoreThanContains() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob = getMockedBlobWithFileVersions(1)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob)
        assertEquals(listOf(1L), getVersionsList(FILE_ID, PROJECT_ID, 2))
    }

    @Test
    fun requestForLessThanContains() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob = getMockedBlobWithFileVersions(3)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob)
        assertEquals(listOf(3L, 2L), getVersionsList(FILE_ID, PROJECT_ID, 2))
    }

    @Test
    fun twoDifferentFileIdsTakeOnlyLatest() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob2 = getMockedBlobWithFileVersions(2, 3, 4)
        val blob1 = getMockedBlobWithFileVersions(1)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob1)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-2-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob2)
        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val mapBlob = getMockedBlobWithFileIdMap(map)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(PROJECT_ID + "-fileIdMap", projectInfo()))))
                .thenReturn(mapBlob)
        assertEquals(listOf(4L, 3L, 2L), getVersionsList("2", PROJECT_ID, 3))
    }

    @Test
    fun takeFromBoth() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob2 = getMockedBlobWithFileVersions(3, 4)
        val blob1 = getMockedBlobWithFileVersions(1, 2)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob1)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-2-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob2)
        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val mapBlob = getMockedBlobWithFileIdMap(map)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(PROJECT_ID + "-fileIdMap", projectInfo()))))
                .thenReturn(mapBlob)
        assertEquals(listOf(4L, 3L, 2L), getVersionsList("2", PROJECT_ID, 3))
    }

    @Test
    fun takeFromBothAll() {
        val fakeStorage: Storage = mock(Storage::class.java)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())


        val blob2 = getMockedBlobWithFileVersions(4)
        val blob1 = getMockedBlobWithFileVersions(1, 2, 3)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-$FILE_ID-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob1)
        `when`(fakeStorage.get(service.getBlobId("$PROJECT_ID-2-fileIdVersionsInfo", projectInfo())))
                .thenReturn(blob2)
        val map = FileIdMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val mapBlob = getMockedBlobWithFileIdMap(map)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(PROJECT_ID + "-fileIdMap", projectInfo()))))
                .thenReturn(mapBlob)
        assertEquals(listOf(4L, 3L, 2L, 1L), getVersionsList("2", PROJECT_ID, 4))
    }

    @Test
    fun changeUserPlanTest() {
        service = getServiceForTestsWithPlans()
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        val patch2 = diffPatch(USER_ID, "kek", "lol", 2)
        addPatchToService(patch1, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        commit(projectInfo(PROJECT_ID, USER_ID, false))
        changePlan(true)
        addPatchToService(patch2, FILE_ID, projectInfo(PROJECT_ID, USER_ID, true))
        commit(projectInfo(PROJECT_ID, USER_ID, true))
        val file1 = getFileFromService(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        val file2 = getFileFromService(0, FILE_ID, projectInfo(PROJECT_ID, USER_ID, true))
        assertEquals("kek", file1)
        assertEquals("lol", file2)
    }

    @Test
    fun changeUserPlanTestDelete() {
        service = getServiceForTestsWithPlans()
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        addPatchToService(patch1, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        commit(projectInfo(PROJECT_ID, USER_ID, false))
        deleteFile(FILE_ID, "file", 3, projectInfo(PROJECT_ID, USER_ID, false))
        val recorder = changePlan(true)
        assertNull(recorder.error)
    }

    @Test
    fun changeUserPlanTestFailed() {
        val runtime = mock(Runtime::class.java)
        val process = mock(Process::class.java)
        Mockito.`when`(runtime.exec(Matchers.anyString())).thenReturn(process)
        Mockito.`when`(process.waitFor()).thenReturn(-1)
        service = getServiceForTestsWithPlans()
        val patch1 = diffPatch(USER_ID, "", "kek", 1)
        addPatchToService(patch1, FILE_ID, projectInfo(PROJECT_ID, USER_ID, false))
        commit(projectInfo(PROJECT_ID, USER_ID, false))
        val recorder = changePlan(true, runtime)
        assertNotNull(recorder.error)
        assertEquals(Status.INTERNAL.code, (recorder.error as StatusException).status.code)
    }

    private fun getStreamRecorderAndRequestForGettingVersion(version: Long, fileId: String = FILE_ID,
                                                             info: ProjectInfo = projectInfo()):
            Pair<StreamRecorder<GetVersionResponse>, GetVersionRequest> {
        val getVersionRecorder: StreamRecorder<GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = GetVersionRequest
                .newBuilder()
                .setGeneration(version)
                .setFileId(fileId)
                .setInfo(info)
                .build()
        return Pair(getVersionRecorder, getVersionRequest)
    }


    private fun getServiceForTests(): CosmasGoogleCloudService {
        return CosmasGoogleCloudService(this.BUCKET_NAME, LocalStorageHelper.getOptions().service, getMockedClock())
    }

    private fun getServiceForTestsWithPlans(): CosmasGoogleCloudService {
        return CosmasGoogleCloudService(FREE_BUCKET_NAME, PAID_BUCKET_NAME, LocalStorageHelper.getOptions().service)
    }

    private fun getStreamRecorderAndRequestForVersionList(fileId: String = FILE_ID, info: ProjectInfo = projectInfo(),
                                                          count: Int = 0):
            Pair<StreamRecorder<FileVersionListResponse>, FileVersionListRequest> {
        val listVersionsRecorder: StreamRecorder<FileVersionListResponse> = StreamRecorder.create()
        val listVersionsRequest = FileVersionListRequest
                .newBuilder()
                .setFileId(fileId)
                .setInfo(info)
                .setCount(count)
                .build()
        return Pair(listVersionsRecorder, listVersionsRequest)
    }

    private fun getVersionsList(fileId: String = FILE_ID, projectId: String = PROJECT_ID, count: Int = 0): List<Long> {
        val (listVersionsRecorder, listVersionsRequest) =
                getStreamRecorderAndRequestForVersionList(fileId, projectInfo(projectId = projectId), count)
        this.service.fileVersionList(listVersionsRequest, listVersionsRecorder)
        return listVersionsRecorder.values[0].versionsList.map { e -> e.generation }
    }


    private fun getMockedBlob(fileContent: String, generation: Long = 0, timestamp: Long = 0): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(
                FileVersion.newBuilder().setContent(
                        ByteString.copyFrom(fileContent.toByteArray()))
                        .setTimestamp(timestamp).build().toByteArray())
        Mockito.`when`(blob.generation).thenReturn(generation)
        return blob
    }

    private fun createFileVersion(fileContent: String): FileVersion {
        return FileVersion.newBuilder()
                .setContent(ByteString.copyFrom(fileContent.toByteArray()))
                .setTimestamp(0L)
                .build()
    }

    private fun getMockedBlobWithFileVersions(count: Int): Blob {
        val versions = mutableListOf<FileVersionInfo>()
        for (i in 1..count) {
            versions.add(FileVersionInfo.newBuilder()
                    .setFileId(FILE_ID)
                    .setGeneration(i.toLong())
                    .setTimestamp(0L)
                    .build())
        }
        val blob = mock(Blob::class.java)
        val versionsInfo = FileIdVersionsInfo.newBuilder()
                .addAllVersionInfo(versions)
                .build()
        Mockito.`when`(blob.getContent())
                .thenReturn(versionsInfo.toByteArray())
        return blob
    }

    private fun getMockedBlobWithFileVersions(vararg generation: Long): Blob {
        val versions = mutableListOf<FileVersionInfo>()
        for (gen in generation) {
            versions.add(FileVersionInfo.newBuilder()
                    .setFileId(FILE_ID)
                    .setGeneration(gen)
                    .setTimestamp(0L)
                    .build())
        }
        val blob = mock(Blob::class.java)
        val versionsInfo = FileIdVersionsInfo.newBuilder()
                .addAllVersionInfo(versions)
                .build()
        Mockito.`when`(blob.getContent())
                .thenReturn(versionsInfo.toByteArray())
        return blob
    }

    private fun getMockedBlobWithPatch(fileContent: String, createTime: Long = 0, patchList: MutableList<CosmasProto.Patch>): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(FileVersion.newBuilder().addAllPatches(patchList)
                .setContent(ByteString.copyFrom(fileContent.toByteArray()))
                .setTimestamp(createTime)
                .build().toByteArray())
        Mockito.`when`(blob.createTime).thenReturn(createTime)
        return blob
    }

    private fun getStreamRecorderAndRequestForCommitVersions(info: ProjectInfo = projectInfo()):
            Pair<StreamRecorder<CommitVersionResponse>, CommitVersionRequest> {
        val commitRecorder: StreamRecorder<CommitVersionResponse> = StreamRecorder.create()
        val commitRequest = CommitVersionRequest
                .newBuilder()
                .setInfo(info)
                .build()
        return Pair(commitRecorder, commitRequest)
    }

    private fun commit(projectId: String = PROJECT_ID): MutableList<FileInfo> {
        val (commitRecorder, commitRequest) =
                getStreamRecorderAndRequestForCommitVersions(projectInfo(projectId))
        this.service.commitVersion(commitRequest, commitRecorder)
        return commitRecorder.values[0].badFilesList
    }

    private fun commit(info: ProjectInfo): MutableList<FileInfo> {
        val (commitRecorder, commitRequest) =
                getStreamRecorderAndRequestForCommitVersions(info)
        this.service.commitVersion(commitRequest, commitRecorder)
        return commitRecorder.values[0].badFilesList
    }

    private fun addPatchToService(patch: Patch, fileId: String = FILE_ID, projectId: String = PROJECT_ID) {
        addPatchesToService(listOf(patch), fileId, projectInfo(projectId))
    }

    private fun addPatchToService(patch: Patch, fileId: String = FILE_ID, info: ProjectInfo) {
        addPatchesToService(listOf(patch), fileId, info)
    }

    private fun addPatchesToService(patches: List<Patch>, fileId: String = FILE_ID, info: ProjectInfo = projectInfo()) {
        val createPatchRecorder: StreamRecorder<CosmasProto.CreatePatchResponse> = StreamRecorder.create()
        val newPatchRequest = CosmasProto.CreatePatchRequest.newBuilder()
                .setFileId(fileId)
                .addAllPatches(patches)
                .setInfo(info)
                .build()
        this.service.createPatch(newPatchRequest, createPatchRecorder)
    }

    private fun deletePatch(fileId: String, generation: Long, patchTimestamp: Long,
                            info: ProjectInfo = projectInfo()): String {
        val deletePatchRecorder: StreamRecorder<DeletePatchResponse> = StreamRecorder.create()
        val deletePatchRequest = DeletePatchRequest
                .newBuilder()
                .setGeneration(generation)
                .setFileId(fileId)
                .setInfo(info)
                .setPatchTimestamp(patchTimestamp)
                .build()
        this.service.deletePatch(deletePatchRequest, deletePatchRecorder)
        return deletePatchRecorder.values[0].content.toStringUtf8()
    }

    private fun newPatch(userId: String, text: String, timeStamp: Long): CosmasProto.Patch {
        return CosmasProto.Patch.newBuilder().setText(text).setUserId(userId).setTimestamp(timeStamp).build()
    }

    private fun deleteFile(fileId: String, fileName: String, time: Long,
                           info: ProjectInfo = projectInfo()): StreamRecorder<DeleteFileResponse> {
        val deleteFileRecorder: StreamRecorder<DeleteFileResponse> = StreamRecorder.create()
        val deleteFileRequest = DeleteFileRequest.newBuilder()
                .setInfo(info)
                .setFileId(fileId)
                .setFileName(fileName)
                .setRemovalTimestamp(time)
                .build()
        this.service.deleteFile(deleteFileRequest, deleteFileRecorder)
        return deleteFileRecorder
    }

    private fun deletedFileList(info: ProjectInfo = projectInfo()): StreamRecorder<DeletedFileListResponse> {
        val deletedFileListRecorder: StreamRecorder<DeletedFileListResponse> = StreamRecorder.create()
        val deletedFileListRequest = DeletedFileListRequest.newBuilder().setInfo(info).build()
        this.service.deletedFileList(deletedFileListRequest, deletedFileListRecorder)
        return deletedFileListRecorder
    }

    private fun restoreDeletedFile(fileId: String = FILE_ID,
                                   info: ProjectInfo = projectInfo()): StreamRecorder<RestoreDeletedFileResponse> {
        val recorder: StreamRecorder<RestoreDeletedFileResponse> = StreamRecorder.create()
        val request = RestoreDeletedFileRequest
                .newBuilder()
                .setFileId(fileId)
                .setInfo(info)
                .build()
        this.service.restoreDeletedFile(request, recorder)
        return recorder
    }

    private fun changeFileId(newFileId: String, oldFileId: String = FILE_ID,
                             info: ProjectInfo = projectInfo()): StreamRecorder<ChangeFileIdResponse> {
        val recorder: StreamRecorder<ChangeFileIdResponse> = StreamRecorder.create()
        val change = ChangeId.newBuilder()
                .setNewFileId(newFileId)
                .setOldFileId(oldFileId)
                .build()
        val request = ChangeFileIdRequest.newBuilder()
                .addChanges(change)
                .setInfo(info)
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

    private fun getMockedBlobWithFileIdVersionsInfo(fileIdVersionsInfo: FileIdVersionsInfo): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(fileIdVersionsInfo.toByteArray())
        return blob
    }

    private fun getMockedBlobWithFileIdVersionsInfo(fileIdVersionsInfo: List<FileVersionInfo>): Blob {
        return getMockedBlobWithFileIdVersionsInfo(FileIdVersionsInfo.newBuilder()
                .addAllVersionInfo(fileIdVersionsInfo)
                .build())
    }

    private fun diffPatch(userId: String, text1: String, text2: String, timeStamp: Long): Patch {
        val text = dmp.patch_toText(dmp.patch_make(text1, text2))
        return CosmasProto.Patch
                .newBuilder()
                .setText(text)
                .setUserId(userId)
                .setTimestamp(timeStamp)
                .setActualHash(md5Hash(text2))
                .setUserName("Papeeria")
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
                             fileId: String = FILE_ID, info: ProjectInfo = projectInfo()) {
        val recorder: StreamRecorder<ForcedFileCommitResponse> = StreamRecorder.create()
        val request = ForcedFileCommitRequest.newBuilder()
                .setFileId(fileId)
                .setTimestamp(timestamp)
                .setActualContent(ByteString.copyFrom(content.toByteArray()))
                .setInfo(info)
                .build()
        this.service.forcedFileCommit(request, recorder)
    }

    fun projectInfo(projectId: String = PROJECT_ID, ownerId: String = USER_ID,
                    isFreePlan: Boolean = true): ProjectInfo {
        return ProjectInfo.newBuilder()
                .setProjectId(projectId)
                .setOwnerId(ownerId)
                .setIsFreePlan(isFreePlan)
                .build()
    }

    fun changePlan(toFree: Boolean, runtime: Runtime = getMockedRuntime(), userId: String = USER_ID):
            StreamRecorder<ChangeUserPlanResponse> {
        val recorder: StreamRecorder<ChangeUserPlanResponse> = StreamRecorder.create()
        val request = ChangeUserPlanRequest.newBuilder()
                .setIsFreePlanNow(toFree)
                .setUserId(userId)
                .build()
        this.service.changeUserPlan(request, recorder, runtime)
        return recorder
    }

    fun getMockedClock(): Clock {
        val clock = mock(Clock::class.java)
        Mockito.`when`(clock.millis()).thenReturn(0L)
        return clock
    }


    fun getMockedRuntime(): Runtime {
        val runtime = mock(Runtime::class.java)
        val process = mock(Process::class.java)
        Mockito.`when`(process.waitFor()).thenReturn(0)
        Mockito.`when`(runtime.exec(Matchers.anyString())).thenReturn(process)
        return runtime
    }
}