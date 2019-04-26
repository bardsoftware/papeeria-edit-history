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
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.COSMAS_NAME
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.MILLIS_IN_DAY
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.buildNewWindow
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.cemeteryName
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.fileIdChangeMapName
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.fileIdGenerationNameMapName
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
import java.io.IOException
import java.time.Clock
import java.time.Month


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
    fun restoreToBufferCreatePatch() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 0)
        val blob2 = getMockedBlob("ver2", 1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), null))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        val ver2 = createFileVersion("ver2").toBuilder()
                .addAllPatches(listOf(patch2))
                .addHistoryWindow(createFileVersionInfo(0L, 0, userName = COSMAS_NAME))
                .build()
        addPatchToService(patch2)
        commit()
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver2.toByteArray()))
    }

    @Test
    fun restoreToBufferVersionList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 0)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), null))))
                .thenReturn(blob1)

        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0))))
                .thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        assertEquals(listOf(0L), getVersionsList(FILE_ID, PROJECT_ID))
    }

    @Test
    fun restoreToBufferVersionBiggerList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob2 = getMockedBlob(createFileVersion("ver2")
                .toBuilder()
                .addHistoryWindow(createFileVersionInfo(0L))
                .build(), 1L)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), null))))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0))))
                .thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        assertEquals(listOf(1L, 0L), getVersionsList(FILE_ID, PROJECT_ID))
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 0L)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1L)))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
                .thenReturn(blob2)

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
        val ver2 = createFileVersion("ver4").toBuilder()
                .addAllPatches(listOf(patch3, patch4))
                .addHistoryWindow(createFileVersionInfo(0L, 0))
                .build()
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), null))))
                .thenReturn(null)
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
                .thenReturn(blob2)
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo()))))
                .thenReturn(null)
                .thenReturn(blob1)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val patch = diffPatch(USER_ID, "", "ver1", 1)
        addPatchToService(patch)
        val badFiles = commit()
        assertTrue(badFiles.isEmpty())
        val uselessPatch = diffPatch(USER_ID, "lol", "kek", 2)
        addPatchToService(uselessPatch)
        forcedCommit("ver2", 3)
        val diffPatch = diffPatch(COSMAS_ID, "ver1", "ver2", 3, userName = COSMAS_NAME)
        val ver1 = createFileVersion("ver1").toBuilder().addAllPatches(listOf(patch)).build()
        val ver1Info = createFileVersionInfo(0, userName = COSMAS_NAME)
        val ver2 = createFileVersion("ver2").toBuilder()
                .addPatches(diffPatch)
                .addHistoryWindow(ver1Info)
                .build()
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver1.toByteArray()))
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver2.toByteArray()))
        verify(fakeStorage, times(2)).get(eq(service.getBlobId(FILE_ID, projectInfo())))
    }

    @Test
    fun forcedCommitCheckUserNameCorrect() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 0)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo()))))
                .thenReturn(null)
                .thenReturn(blob1)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        forcedCommit("ver1", 1)
        val diffPatch = diffPatch(COSMAS_ID, "", "ver1", 1, userName = COSMAS_NAME)
        val ver1 = createFileVersion("ver1").toBuilder()
                .addPatches(diffPatch)
                .build()
        verify(fakeStorage).create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                eq(ver1.toByteArray()))
        verify(fakeStorage, times(1)).get(eq(service.getBlobId(FILE_ID, projectInfo())))
        assertEquals(listOf(createFileVersionInfo(0, userName = COSMAS_NAME)), getFullVersionsList())
    }

    @Test
    fun failedCommitBecauseOfBadPatch() {
        val patch = newPatch(USER_ID, "kek", 1)
        addPatchToService(patch)
        val badFiles = commit(checkBadFiles = false)
        val expected = FileInfo.newBuilder()
                .setProjectId(PROJECT_ID)
                .setFileId(FILE_ID)
                .build()
        assertEquals(mutableListOf(expected), badFiles)
    }

    @Test
    fun failedCommitBecauseOfWrongHash() {
        val patch = diffPatch(USER_ID, "", "lol", 1, hash = "it's not a hash")
        addPatchToService(patch)
        val badFiles = commit(checkBadFiles = false)
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 0L))))
                .thenReturn(blob1)
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo())))
                .thenReturn(blob1)
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo())))
                .thenReturn(blob1)
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), generation)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo())))
                .thenReturn(blob4)
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
        val cemeteryName = cemeteryName(projectInfo())
        val cemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
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
        val cemeteryName = cemeteryName(projectInfo())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(null)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
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
    fun deleteFileAndClearCemetery() {
        val day: Long = MILLIS_IN_DAY
        val fakeStorage = mock(Storage::class.java)
        val cemeteryName = cemeteryName(projectInfo())
        val tomb1 = CosmasProto.FileTomb.newBuilder()
                .setFileId("1")
                .setFileName("kek1")
                .setRemovalTimestamp(day) // should stay in cemetery
                .build()
        val tomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("2")
                .setFileName("kek2")
                .setRemovalTimestamp(2) // should be removed from cemetery
                .build()
        val cemetery = FileCemetery.newBuilder()
                .addCemetery(tomb1)
                .addCemetery(tomb2)
                .build()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)


        val clock = mock(Clock::class.java)

        Mockito.`when`(clock.millis()).thenReturn(day + 10)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, clock)
        val streamRecorder = deleteFile(FILE_ID, "file", day - 2)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        val newTomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(day - 2)
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(FileCemetery.newBuilder().addCemetery(tomb1).addCemetery(newTomb).build().toByteArray()))
    }

    @Test
    fun deleteTwoFiles() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val cemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val file1 = DeletedFileInfo.newBuilder()
                .setFileId("1")
                .setFileName("kek1")
                .build()
        val file2 = DeletedFileInfo.newBuilder()
                .setFileId("2")
                .setFileName("kek2")
                .build()
        val streamRecorder = deleteFiles(listOf(file1, file2), time)
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        val newTomb1 = CosmasProto.FileTomb.newBuilder()
                .setFileId("1")
                .setFileName("kek1")
                .setRemovalTimestamp(time)
                .build()
        val newTomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("2")
                .setFileName("kek2")
                .setRemovalTimestamp(time)
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(FileCemetery.newBuilder().addAllCemetery(listOf(newTomb1, newTomb2)).build().toByteArray()))
        Mockito.verifyNoMoreInteractions(fakeStorage)
    }

    @Test
    fun deletedFileListTest() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deletedFileList()
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        assertEquals(DeletedFileListResponse.newBuilder().addFiles(tomb).build(), streamRecorder.values[0])
    }

    @Test
    fun deletedFileListTwoFilesTest() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val tomb1 = CosmasProto.FileTomb.newBuilder()
                .setFileId("1")
                .setFileName("file1")
                .setRemovalTimestamp(time)
                .build()
        val tomb2 = CosmasProto.FileTomb.newBuilder()
                .setFileId("2")
                .setFileName("file2")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addAllCemetery(listOf(tomb1, tomb2)).build()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val streamRecorder = deletedFileList()
        assertNull(streamRecorder.error)
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        assertEquals(DeletedFileListResponse.newBuilder().addAllFiles(listOf(tomb1, tomb2)).build(),
                streamRecorder.values[0])
    }


    @Test
    fun restoreDeletedFileTest() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val emptyCemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        restoreDeletedFile()
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(emptyCemetery.toByteArray()))
    }


    @Test
    fun restoreDeletedFileAndChangingIdOldRequestFormat() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val emptyCemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)

        val blob1 = getMockedBlob("ver1", 1, 1)
        val blob2 = getMockedBlob("ver2", 2, 2)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId("2", projectInfo(), 2)))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        restoreDeletedFile()
        changeFileId("2")
        addPatchToService(patch2, "2")
        commit()
        assertEquals(listOf(2L, 1L), getVersionsList("2"))
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(emptyCemetery.toByteArray()))

    }

    @Test
    fun restoreDeletedFileAndChangingIdOneRequest() {
        val fakeStorage = mock(Storage::class.java)
        val time = 1L
        val cemeteryName = cemeteryName(projectInfo())
        val tomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(FILE_ID)
                .setFileName("file")
                .setRemovalTimestamp(time)
                .build()
        val cemetery = FileCemetery.newBuilder().addCemetery(tomb).build()
        val emptyCemetery = FileCemetery.getDefaultInstance()
        val cemeteryBlob = getMockedBlobWithCemetery(cemetery)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)

        val blob1 = getMockedBlob("ver1", 1, 1)
        val blob2 = getMockedBlob("ver2", 2, 2)
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId("2", projectInfo(), 2)))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        restoreDeletedFile(newFileId = "2")
        addPatchToService(patch2, "2")
        commit()
        assertEquals(listOf(2L, 1L), getVersionsList("2"))
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(cemeteryName, projectInfo())))
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(cemeteryName, projectInfo())),
                eq(emptyCemetery.toByteArray()))
    }

    @Test
    fun restoreDeletedFileTestWithBigCemetery() {
        val fakeStorage = mock(Storage::class.java)
        val cemeteryName = cemeteryName(projectInfo())
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
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(cemeteryName, projectInfo()))))
                .thenReturn(cemeteryBlob)
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
        Mockito.`when`(fakeStorage.get(service.getBlobId(FILE_ID, projectInfo(), 1)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(service.getBlobId("2", projectInfo(), 2)))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)

        val map = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()


        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        addPatchToService(patch1)
        commit()
        changeFileId("2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName(projectInfo()), projectInfo()),
                map.toByteArray())

        addPatchToService(patch2, "2")
        commit()
        assertEquals(listOf(2L, 1L), getVersionsList("2"))
    }

    @Test
    fun changeFileIdTwoChanges() {
        val fileIdChangeMapName = fileIdChangeMapName(projectInfo())
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1, 1, fileId = "1")
        val blob2 = getMockedBlob("ver2", 2, 2, fileId = "2")
        val blob3 = getMockedBlob("ver3", 3, 3, fileId = "3")
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("2", projectInfo(), 2))))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("3", projectInfo(), 3))))
                .thenReturn(blob3)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("3", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob3)
        val map1 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val map1Blob = getMockedBlobWithFileIdChangeMap(map1)
        val map2 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("3" to "2"))
                .build()
        val map2Blob = getMockedBlobWithFileIdChangeMap(map2)
        val emptyBlob = getMockedBlobWithFileIdChangeMap(FileIdChangeMap.getDefaultInstance())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(null)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch2 = diffPatch(USER_ID, "ver1", "ver2", 2)
        val patch3 = diffPatch(USER_ID, "ver2", "ver3", 3)
        addPatchToService(patch1)
        commit()
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(emptyBlob)
        changeFileId("2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map1.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map1Blob)

        addPatchToService(patch2, "2")
        commit()
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(emptyBlob)
        changeFileId("3", "2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map2.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map2Blob)

        addPatchToService(patch3, "3")
        commit()
        Mockito.verify(fakeStorage, times(3)).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                FileIdChangeMap.getDefaultInstance().toByteArray())
        assertEquals(listOf(3L, 2L, 1L), getVersionsList("3"))
    }

    @Test
    fun changeFileIdTwoChangesTwoCommits() {
        val fileIdChangeMapName = fileIdChangeMapName(projectInfo())
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1, 1, fileId = "1")
        val blob2 = getMockedBlob("ver2", 2, 2, fileId = "2")
        val blob3 = getMockedBlob("ver3", 3, 3, fileId = "3")
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("2", projectInfo(), 2))))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId("3", projectInfo(), 3))))
                .thenReturn(blob3)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("2", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob2)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo("3", projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob3)
        val map1 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val map1Blob = getMockedBlobWithFileIdChangeMap(map1)
        val map2 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1", "3" to "2"))
                .build()
        val map2Blob = getMockedBlobWithFileIdChangeMap(map2)
        val emptyBlob = getMockedBlobWithFileIdChangeMap(FileIdChangeMap.getDefaultInstance())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(null)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        val patch3 = diffPatch(USER_ID, "ver2", "ver3", 3)
        addPatchToService(patch1)
        commit()
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(emptyBlob)
        changeFileId("2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map1.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map1Blob)


        changeFileId("3", "2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map2.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map2Blob)

        addPatchToService(patch3, "3")
        commit()
        Mockito.verify(fakeStorage, times(2)).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                FileIdChangeMap.getDefaultInstance().toByteArray())
    }

    @Test
    fun hardRestoring() {
        val fileIdChangeMapName = fileIdChangeMapName(projectInfo())
        val fakeStorage: Storage = mock(Storage::class.java)
        val blob1 = getMockedBlob("ver1", 1, 1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
        val map1 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1"))
                .build()
        val map1Blob = getMockedBlobWithFileIdChangeMap(map1)
        val map2 = FileIdChangeMap.newBuilder()
                .putAllPrevIds(mutableMapOf("2" to "1", "3" to "2"))
                .build()
        val map2Blob = getMockedBlobWithFileIdChangeMap(map2)


        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        addPatchToService(patch1)
        commit()
        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                FileIdChangeMap.getDefaultInstance().toByteArray())
        changeFileId("2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map1.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map1Blob)


        changeFileId("3", "2")

        Mockito.verify(fakeStorage).create(
                this.service.getBlobInfo(fileIdChangeMapName, projectInfo()),
                map2.toByteArray())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(fileIdChangeMapName, projectInfo()))))
                .thenReturn(map2Blob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())

        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo()))))
                .thenReturn(blob1)
        val versionInfo = getFullVersionsList("3").first()
        assertEquals(1L, versionInfo.generation)
        assertEquals(FILE_ID, versionInfo.fileId)
    }

    private fun commitAndThenForcedCommit(maxWindow: Int = 2): Storage {
        val fakeStorage: Storage = mock(Storage::class.java)
        val v1 = createFileVersion("ver1")
        val v2 = createFileVersion("ver2", 0, listOf(createFileVersionInfo(1, 0)))
        val blob1 = getMockedBlob(v1, 1)
        val blob2 = getMockedBlob(v2, 2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)

        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo()))))
                .thenReturn(null)
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 2))))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
                .thenReturn(blob2)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock(), windowMaxSize = maxWindow)
        val patch1 = diffPatch(USER_ID, "", "ver1", 1)
        addPatchToService(patch1)
        commit()
        forcedCommit("ver2", 0)
        return fakeStorage
    }

    @Test
    fun versionListForcedCommitListInMemory() {
        val fakeStorage = commitAndThenForcedCommit()
        assertEquals(listOf(2L, 1L), getVersionsList())
        verify(fakeStorage, times(2)).create(
                eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))

    }

    @Test
    fun versionListForcedCommitListInStorage() {
        val fakeStorage = commitAndThenForcedCommit(1)
        assertEquals(listOf(1L), getVersionsList(startGeneration = 2L))
        verify(fakeStorage, times(2)).create(
                eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java))

    }

    @Test
    fun oneForcedCommitVersionList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val v1 = createFileVersion("ver1")
        val blob1 = getMockedBlob(v1, 1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, getMockedClock())
        forcedCommit("ver1", 0)
        assertEquals(listOf(1L), getVersionsList())
    }

    @Test
    fun correctVersionList() {
        val fakeStorage: Storage = mock(Storage::class.java)
        val v1 = createFileVersion("ver1")
        val v2 = createFileVersion("ver2", 0, listOf(createFileVersionInfo(1, 0)))
        val blob1 = getMockedBlob(v1, 1)
        val blob2 = getMockedBlob(v2, 2)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(blob1)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 2))))
                .thenReturn(blob2)

        Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                any(ByteArray::class.java)))
                .thenReturn(blob1)
                .thenReturn(blob2)

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

    }

    @Test
    fun checkUserName() {
        addManyVersions(2, 2, userName = "Keker")
        assertEquals(listOf(createFileVersionInfo(2L, userName = "Keker"),
                createFileVersionInfo(1L, userName = "Keker")), getFullVersionsList(FILE_ID, PROJECT_ID))
    }

    @Test
    fun getAllInMemory() {
        addManyVersions(3, 3)
        assertEquals(listOf(3L, 2L, 1L), getVersionsList(FILE_ID, PROJECT_ID))
    }

    @Test
    fun getAllInMemoryBigWindow() {
        addManyVersions(3, 5)
        assertEquals(listOf(3L, 2L, 1L), getVersionsList(FILE_ID, PROJECT_ID))
    }

    @Test
    fun getSecondWindow() {
        addManyVersions(4, 2)
        assertEquals(listOf(2L, 1L), getVersionsList(FILE_ID, PROJECT_ID, 3L))
    }

    @Test
    fun generationDoesntExists() {
        addManyVersions(2, 2)
        val (recorder, request) = getStreamRecorderAndRequestForVersionList(FILE_ID, projectInfo(), 43)
        this.service.fileVersionList(request, recorder)
        assertNotNull(recorder.error)
        assertEquals(Status.NOT_FOUND.code, (recorder.error as StatusException).status.code)
    }


    @Test
    fun windowInTheMiddle() {
        addManyVersions(6, 2)
        assertEquals(listOf(4L, 3L),
                getVersionsList(FILE_ID, PROJECT_ID, 5L))
    }

    @Test
    fun normalWorkEmulation() {
        addManyVersions(6, 2)
        assertEquals(listOf(6L, 5L),
                getVersionsList(FILE_ID, PROJECT_ID, -1L))
        assertEquals(listOf(4L, 3L),
                getVersionsList(FILE_ID, PROJECT_ID, 5L))
        assertEquals(listOf(2L, 1L),
                getVersionsList(FILE_ID, PROJECT_ID, 3L))
        val (recorder, request) = getStreamRecorderAndRequestForVersionList(FILE_ID, projectInfo(), 1L)
        this.service.fileVersionList(request, recorder)
        assertNotNull(recorder.error)
        assertEquals(Status.NOT_FOUND.code, (recorder.error as StatusException).status.code)
    }

    @Test
    fun smallWindow() {
        addManyVersions(3, 1)
        assertEquals(listOf(2L),
                getVersionsList(FILE_ID, PROJECT_ID, 3))
    }

    private fun addManyVersions(count: Long, windowMaxSize: Int, clock: Clock = getMockedClock(),
                                fakeStorage: Storage = mock(Storage::class.java),
                                userName: String = "") {

        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage, clock, windowMaxSize = windowMaxSize)

        var curWindow = mutableListOf<FileVersionInfo>()
        val patches = mutableListOf<Patch>()
        for (i in 1L..count) {
            val curTime = clock.millis()
            val versionInfo = createFileVersionInfo(i, curTime, userName = userName)

            val patch = if (i == 1L) {
                diffPatch(USER_ID, "", "kek $i", 0, userName = userName)
            } else {
                diffPatch(USER_ID, "kek ${i - 1}", "kek $i", 0, userName = userName)
            }
            patches.add(patch)
            val version = createFileVersion("kek $i", curTime, curWindow)
                    .toBuilder()
                    .addPatches(patch)
                    .build()
            val blob = getMockedBlob(version, i)
            Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), i))))
                    .thenReturn(blob)
            Mockito.`when`(fakeStorage.create(eq(service.getBlobInfo(FILE_ID, projectInfo())),
                    eq(version.toByteArray())))
                    .thenReturn(blob)
            curWindow = buildNewWindow(versionInfo, curWindow, windowMaxSize)
        }
        for (patch in patches) {
            addPatchToService(patch)
            commit()
        }
    }

    @Test
    fun versionsNotExistFromMemory() {
        val day = MILLIS_IN_DAY
        val clock = mock(Clock::class.java)

        // Creating versions with timestamps [1, 2, day + 1, day + 2] (second block of thenReturn is for commit)
        Mockito.`when`(clock.millis())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)

        val fakeStorage = mock(Storage::class.java)
        addManyVersions(4, 5, clock, fakeStorage)

        // Now fakeStorage doesn't contain versions with generation = 1 and 2
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 2))))
                .thenReturn(null)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(null)

        // Current time is day + 3
        Mockito.`when`(clock.millis())
                .thenReturn(day + 3L)

        // Versions with timestamp = 1 and timestamp = 2 have been created more than day ago - we're ignoring them
        assertEquals(listOf(4L, 3L), getVersionsList(FILE_ID, PROJECT_ID))
    }

    @Test
    fun versionsNotExistFromStorage() {
        val day: Long = MILLIS_IN_DAY
        val clock = mock(Clock::class.java)

        // Creating versions with timestamps [0, day, day + 1, day + 2] (second block of thenReturn is for commit)
        Mockito.`when`(clock.millis())
                .thenReturn(1L)
                .thenReturn(day)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)
                .thenReturn(1L)
                .thenReturn(day)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)

        val fakeStorage = mock(Storage::class.java)
        addManyVersions(4, 2, clock, fakeStorage)

        // Now fakeStorage doesn't contain version with generation = 1
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(null)

        // Current time is day + 3
        Mockito.`when`(clock.millis())
                .thenReturn(day + 3L)

        // Version with timestamp = 1 and generation = 1 has been created more than day ago - we're ignoring it
        assertEquals(listOf(2L), getVersionsList(FILE_ID, PROJECT_ID, 3))
    }

    @Test
    fun versionsNotExistFromStoragePaid() {
        val day: Long = MILLIS_IN_DAY
        val month = 30 * day
        val clock = mock(Clock::class.java)

        // Creating versions with timestamps [0, day, day + 1, day + 2] (second block of thenReturn is for commit)
        Mockito.`when`(clock.millis())
                .thenReturn(1L)
                .thenReturn(day)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)
                .thenReturn(1L)
                .thenReturn(day)
                .thenReturn(day + 1L)
                .thenReturn(day + 2L)

        val fakeStorage = mock(Storage::class.java)
        addManyVersions(4, 2, clock, fakeStorage)

        // Now fakeStorage doesn't contain version with generation = 1
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(FILE_ID, projectInfo(), 1))))
                .thenReturn(null)

        // Current time is month + 3
        Mockito.`when`(clock.millis())
                .thenReturn(month + 3L)

        // Version with timestamp = 1 and generation = 1 has been created more than month ago  - we're ignoring it
        assertEquals(listOf(2L), getVersionsList(FILE_ID, PROJECT_ID, 3, isFreePlan = false))
    }

    @Test
    fun renameWithNotEmptyFileIdGenerationNameMap() {
        val fakeStorage = mock(Storage::class.java)
        val dictionaryName = fileIdGenerationNameMapName(projectInfo())
        val dictionaryBefore = FileIdGenerationNameMap.newBuilder()
                .putValue("2", GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersionButOtherFile")
                        .build())
                .build()
        val dictionaryBlob = getMockedBlobWithFileIdGenerationNameMap(dictionaryBefore)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(dictionaryName, projectInfo()))))
                .thenReturn(dictionaryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        renameVersion(1L, "SuperVersion")
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(dictionaryName, projectInfo())))
        val dictionary = FileIdGenerationNameMap.newBuilder()
                .putValue("2", GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersionButOtherFile")
                        .build())
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersion")
                        .build())
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(dictionaryName, projectInfo())),
                eq(dictionary.toByteArray()))
    }

    @Test
    fun getVersionsListWithName() {
        val fakeStorage = mock(Storage::class.java)
        val dictionaryName = fileIdGenerationNameMapName(projectInfo())
        val dictionary = FileIdGenerationNameMap.newBuilder()
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersionButOtherFile")
                        .build())
                .build()
        val dictionaryBlob = getMockedBlobWithFileIdGenerationNameMap(dictionary)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(dictionaryName, projectInfo()))))
                .thenReturn(dictionaryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        addManyVersions(1, 10, fakeStorage = fakeStorage)
        val versions = getFullVersionsList()
        assertEquals("SuperVersionButOtherFile", versions[0].versionName)
    }

    @Test
    fun renameWithNotEmptyFileIdGenerationNameMapManyVersions() {
        val fakeStorage = mock(Storage::class.java)
        val dictionaryName = fileIdGenerationNameMapName(projectInfo())
        val dictionaryBefore = FileIdGenerationNameMap.newBuilder()
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(2L, "SuperVersionAnotherGeneration")
                        .putValue(3L, "AndAnotherOne")
                        .build())
                .build()
        val dictionaryBlob = getMockedBlobWithFileIdGenerationNameMap(dictionaryBefore)
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(dictionaryName, projectInfo()))))
                .thenReturn(dictionaryBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        renameVersion(1L, "SuperVersion")
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(dictionaryName, projectInfo())))
        val dictionary = FileIdGenerationNameMap.newBuilder()
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(2L, "SuperVersionAnotherGeneration")
                        .putValue(3L, "AndAnotherOne")
                        .putValue(1L, "SuperVersion")
                        .build())
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(dictionaryName, projectInfo())),
                eq(dictionary.toByteArray()))
    }

    @Test
    fun renameWithEmptyFileIdGenerationNameMap() {
        val fakeStorage = mock(Storage::class.java)
        val dictionaryName = fileIdGenerationNameMapName(projectInfo())
        val emptyFileIdGenerationNameMapBlob = getMockedBlobWithFileIdGenerationNameMap(FileIdGenerationNameMap.getDefaultInstance())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(dictionaryName, projectInfo()))))
                .thenReturn(emptyFileIdGenerationNameMapBlob)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        renameVersion(1L, "SuperVersion")
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(dictionaryName, projectInfo())))
        val dictionary = FileIdGenerationNameMap.newBuilder()
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersion")
                        .build())
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(dictionaryName, projectInfo())),
                eq(dictionary.toByteArray()))
    }

    @Test
    fun renameWithNullFileIdGenerationNameMap() {
        val fakeStorage = mock(Storage::class.java)
        val dictionaryName = fileIdGenerationNameMapName(projectInfo())
        Mockito.`when`(fakeStorage.get(eq(service.getBlobId(dictionaryName, projectInfo()))))
                .thenReturn(null)
        this.service = CosmasGoogleCloudService(this.BUCKET_NAME, fakeStorage)
        renameVersion(1L, "SuperVersion")
        Mockito.verify(fakeStorage).get(eq(service.getBlobId(dictionaryName, projectInfo())))
        val dictionary = FileIdGenerationNameMap.newBuilder()
                .putValue(FILE_ID, GenerationNameMap.newBuilder()
                        .putValue(1L, "SuperVersion")
                        .build())
                .build()
        Mockito.verify(fakeStorage).create(eq(service.getBlobInfo(dictionaryName, projectInfo())),
                eq(dictionary.toByteArray()))
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

    private fun getStreamRecorderAndRequestForVersionList(fileId: String = FILE_ID, info: ProjectInfo = projectInfo(),
                                                          startGeneration: Long):
            Pair<StreamRecorder<FileVersionListResponse>, FileVersionListRequest> {
        val listVersionsRecorder: StreamRecorder<FileVersionListResponse> = StreamRecorder.create()
        val listVersionsRequest = FileVersionListRequest
                .newBuilder()
                .setFileId(fileId)
                .setInfo(info)
                .setStartGeneration(startGeneration)
                .build()
        return Pair(listVersionsRecorder, listVersionsRequest)
    }

    private fun getVersionsList(fileId: String = FILE_ID, projectId: String = PROJECT_ID,
                                startGeneration: Long = -1L, isFreePlan: Boolean = true): List<Long> {
        val (listVersionsRecorder, listVersionsRequest) =
                getStreamRecorderAndRequestForVersionList(fileId,
                        projectInfo(projectId = projectId, isFreePlan = isFreePlan), startGeneration)
        this.service.fileVersionList(listVersionsRequest, listVersionsRecorder)
        return listVersionsRecorder.values[0].versionsList.map { e -> e.generation }
    }

    private fun getFullVersionsList(fileId: String = FILE_ID, projectId: String = PROJECT_ID,
                                    startGeneration: Long = -1L, isFreePlan: Boolean = true): List<FileVersionInfo> {
        val (listVersionsRecorder, listVersionsRequest) =
                getStreamRecorderAndRequestForVersionList(fileId,
                        projectInfo(projectId = projectId, isFreePlan = isFreePlan), startGeneration)
        this.service.fileVersionList(listVersionsRequest, listVersionsRecorder)
        return listVersionsRecorder.values[0].versionsList
    }


    private fun getMockedBlob(fileContent: String, generation: Long = 0, timestamp: Long = 0, fileId: String = FILE_ID): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(
                FileVersion.newBuilder().setContent(
                        ByteString.copyFrom(fileContent.toByteArray()))
                        .setTimestamp(timestamp)
                        .setFileId(fileId).build().toByteArray())
        Mockito.`when`(blob.generation)
                .thenReturn(generation)
        return blob
    }

    private fun getMockedBlob(fileVersion: FileVersion, generation: Long = 0): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent())
                .thenReturn(fileVersion.toByteArray())
        Mockito.`when`(blob.generation)
                .thenReturn(generation)
        return blob
    }

    private fun createFileVersion(fileContent: String, timestamp: Long = 0,
                                  list: List<FileVersionInfo> = emptyList(), fileId: String = FILE_ID): FileVersion {
        return FileVersion.newBuilder()
                .setContent(ByteString.copyFrom(fileContent.toByteArray()))
                .setTimestamp(timestamp)
                .addAllHistoryWindow(list)
                .setFileId(fileId)
                .build()
    }

    private fun createFileVersionInfo(generation: Long, timeStamp: Long = 0L,
                                      fileId: String = FILE_ID, userName: String = "", versionName: String = ""): FileVersionInfo {
        return FileVersionInfo.newBuilder()
                .setFileId(fileId)
                .setGeneration(generation)
                .setTimestamp(timeStamp)
                .setUserName(userName)
                .setVersionName(versionName)
                .build()
    }


    private fun getMockedBlobWithPatch(fileContent: String, createTime: Long = 0, patchList: MutableList<CosmasProto.Patch>): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(FileVersion.newBuilder().addAllPatches(patchList)
                .setContent(ByteString.copyFrom(fileContent.toByteArray()))
                .setTimestamp(createTime)
                .build().toByteArray())
        Mockito.`when`(blob.createTime)
                .thenReturn(createTime)
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

    private fun commit(projectId: String = PROJECT_ID, checkBadFiles: Boolean = true): MutableList<FileInfo> {
        return commit(projectInfo(projectId), checkBadFiles)
    }

    private fun commit(info: ProjectInfo, checkBadFiles: Boolean = true): MutableList<FileInfo> {
        val (commitRecorder, commitRequest) =
                getStreamRecorderAndRequestForCommitVersions(info)
        this.service.commitVersion(commitRequest, commitRecorder)
        val maybeError = commitRecorder.error
        if (maybeError != null)
            throw maybeError
        if (checkBadFiles && commitRecorder.values[0].badFilesList.size != 0) {
            throw IOException("Error while committing file")
        }
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
                           info: ProjectInfo = projectInfo()): StreamRecorder<DeleteFilesResponse> {
        val deleteFileRecorder: StreamRecorder<DeleteFilesResponse> = StreamRecorder.create()
        val deleteFileInfo = DeletedFileInfo.newBuilder()
                .setFileId(fileId)
                .setFileName(fileName)
        val deleteFileRequest = DeleteFilesRequest.newBuilder()
                .setInfo(info)
                .addFiles(deleteFileInfo)
                .setRemovalTimestamp(time)
                .build()
        this.service.deleteFiles(deleteFileRequest, deleteFileRecorder)
        return deleteFileRecorder
    }

    private fun renameVersion(generation: Long, name: String, fileId: String = FILE_ID,
                              info: ProjectInfo = projectInfo()): StreamRecorder<RenameVersionResponse> {
        val recorder: StreamRecorder<RenameVersionResponse> = StreamRecorder.create()
        val request = RenameVersionRequest.newBuilder()
                .setGeneration(generation)
                .setName(name)
                .setFileId(fileId)
                .setInfo(info)
                .build()
        this.service.renameVersion(request, recorder)
        return recorder
    }

    private fun deleteFiles(files: List<DeletedFileInfo>, time: Long,
                            info: ProjectInfo = projectInfo()): StreamRecorder<DeleteFilesResponse> {
        val deleteFileRecorder: StreamRecorder<DeleteFilesResponse> = StreamRecorder.create()
        val deleteFileRequest = DeleteFilesRequest.newBuilder()
                .setInfo(info)
                .addAllFiles(files)
                .setRemovalTimestamp(time)
                .build()
        this.service.deleteFiles(deleteFileRequest, deleteFileRecorder)
        return deleteFileRecorder
    }

    private fun deletedFileList(info: ProjectInfo = projectInfo()): StreamRecorder<DeletedFileListResponse> {
        val deletedFileListRecorder: StreamRecorder<DeletedFileListResponse> = StreamRecorder.create()
        val deletedFileListRequest = DeletedFileListRequest.newBuilder().setInfo(info).build()
        this.service.deletedFileList(deletedFileListRequest, deletedFileListRecorder)
        return deletedFileListRecorder
    }

    private fun restoreDeletedFile(fileId: String = FILE_ID,
                                   info: ProjectInfo = projectInfo(),
                                   newFileId: String = ""): StreamRecorder<RestoreDeletedFileResponse> {
        val recorder: StreamRecorder<RestoreDeletedFileResponse> = StreamRecorder.create()
        val request = RestoreDeletedFileRequest
                .newBuilder()
                .setOldFileId(fileId)
                .setNewFileId(newFileId)
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
        Mockito.`when`(blob.getContent())
                .thenReturn(cemetery.toByteArray())
        return blob
    }

    private fun getMockedBlobWithFileIdGenerationNameMap(dictionary: FileIdGenerationNameMap): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent())
                .thenReturn(dictionary.toByteArray())
        return blob
    }

    private fun diffPatch(userId: String, text1: String, text2: String, timeStamp: Long, userName: String = ""): Patch {
        return diffPatch(userId, text1, text2, timeStamp, md5Hash(text2), userName)
    }

    private fun diffPatch(userId: String, text1: String, text2: String, timeStamp: Long, hash: String, userName: String = ""): Patch {
        val text = dmp.patch_toText(dmp.patch_make(text1, text2))
        return CosmasProto.Patch
                .newBuilder()
                .setText(text)
                .setUserId(userId)
                .setTimestamp(timeStamp)
                .setActualHash(hash)
                .setUserName(userName)
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

    fun getMockedClock(): Clock {
        val clock = mock(Clock::class.java)
        Mockito.`when`(clock.millis())
                .thenReturn(0L)
        return clock
    }

    private fun getMockedBlobWithFileIdChangeMap(fileIdChangeMap: FileIdChangeMap): Blob {
        val blob = mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(fileIdChangeMap.toByteArray())
        return blob
    }


    fun getMockedRuntime(): Runtime {
        val runtime = mock(Runtime::class.java)
        val process = mock(Process::class.java)
        Mockito.`when`(process.waitFor())
                .thenReturn(0)
        Mockito.`when`(runtime.exec(Matchers.anyString())).thenReturn(process)
        return runtime
    }
}