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

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.protobuf.ByteString
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.io.IOException
import java.time.Clock

/**
 * Some tests for ServiceFilesMediator
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class ServiceFilesMediatorTest {
    private val BUCKET_NAME = "papeeria-interns-cosmas"
    private var service = getServiceForTests()
    private var mediator = getMediatorForTests()

    @Before
    fun testInitialization() {
        mediator = getMediatorForTests()
        println()
    }

    @Test
    fun readFileWorksNotExists() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(null)
        mediator.withReadFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            assertEquals(CosmasProto.FileVersion.getDefaultInstance(), it)
        }
    }

    @Test
    fun readFileWorksFileExists() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        val expectedContent = "file content"
        mediator = getMediatorWithMocked(fakeStorage)
        val blob = getMockedBlob(expectedContent)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(blob)
        mediator.withReadFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            assertEquals(CosmasProto.FileVersion.newBuilder()
                    .setContent(ByteString.copyFrom(expectedContent.toByteArray())).build(), it)
        }
    }


    @Test(expected = StorageException::class)
    fun readFileExceptionThrows() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(null)

        mediator.withReadFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            throw StorageException(IOException())
        }
    }

    @Test
    fun readFileExceptionUnlock() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(null)
        try {
            mediator.withReadFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
                throw StorageException(IOException())
            }
        } catch (e: StorageException) {
            // should be there
        }
        mediator.withReadFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            assertEquals(CosmasProto.FileVersion.getDefaultInstance(), it)
        }
    }

    @Test
    fun writeFileWorks() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        val expectedContent = "file content"
        val writtenVersion = CosmasProto.FileVersion.newBuilder()
                .setContent(ByteString.copyFrom("file content changed".toByteArray())).build()
        val blob = getMockedBlob(expectedContent)
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(blob)
        mediator = getMediatorWithMocked(fakeStorage)
        mediator.withWriteFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            assertEquals(CosmasProto.FileVersion.newBuilder()
                    .setContent(ByteString.copyFrom(expectedContent.toByteArray())).build(), it)
            return@withWriteFile writtenVersion
        }
        Mockito.verify(fakeStorage).create(service.getBlobInfo("file", projectInfo(), 1L),
                writtenVersion.toByteArray(), Storage.BlobTargetOption.generationMatch())
    }



    @Test(expected = StorageException::class)
    fun writeFileExceptionThrows() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(null)

        mediator.withWriteFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            throw StorageException(IOException())
        }
    }

    @Test
    fun writeFileExceptionUnlock() {
        val fakeStorage: Storage = Mockito.mock(Storage::class.java)
        val writtenVersion = CosmasProto.FileVersion.newBuilder()
                .setContent(ByteString.copyFrom("file content changed".toByteArray())).build()
        mediator = getMediatorWithMocked(fakeStorage)
        Mockito.`when`(fakeStorage.get(service.getBlobId("file", projectInfo())))
                .thenReturn(null)
        mediator = getMediatorWithMocked(fakeStorage)
        try {
            mediator.withWriteFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
                throw StorageException(IOException())
            }
        } catch (e: StorageException) {
            // should be there
        }
        mediator.withWriteFile("file", projectInfo(), CosmasProto.FileVersion.getDefaultInstance()) {
            assertEquals(CosmasProto.FileVersion.getDefaultInstance(), it)
            return@withWriteFile writtenVersion
        }
        Mockito.verify(fakeStorage).create(service.getBlobInfo("file", projectInfo(), 0L),
                writtenVersion.toByteArray(), Storage.BlobTargetOption.generationMatch())
    }


    private fun getMediatorForTests(): ServiceFilesMediator {
        val storage = LocalStorageHelper.getOptions().service
        service = CosmasGoogleCloudService(BUCKET_NAME, storage, getMockedClock())
        return ServiceFilesMediator(storage, service)
    }

    private fun getMediatorWithMocked(storage: Storage): ServiceFilesMediator {
        service = CosmasGoogleCloudService(BUCKET_NAME, storage, getMockedClock())
        return ServiceFilesMediator(storage, service)
    }

    private fun getServiceForTests(): CosmasGoogleCloudService {
        println(BUCKET_NAME)
        return CosmasGoogleCloudService(BUCKET_NAME, LocalStorageHelper.getOptions().service, getMockedClock())
    }

    private fun projectInfo(): CosmasProto.ProjectInfo {
        return CosmasProto.ProjectInfo.newBuilder()
                .setProjectId("1")
                .setOwnerId("1")
                .setIsFreePlan(true)
                .build()
    }

    private fun getMockedBlob(fileContent: String): Blob {
        val blob = Mockito.mock(Blob::class.java)
        Mockito.`when`(blob.getContent()).thenReturn(
                CosmasProto.FileVersion.newBuilder().setContent(
                        ByteString.copyFrom(fileContent.toByteArray()))
                        .build().toByteArray())
        Mockito.`when`(blob.generation)
                .thenReturn(1L)
        return blob
    }

    private fun getMockedClock(): Clock {
        val clock = Mockito.mock(Clock::class.java)
        Mockito.`when`(clock.millis())
                .thenReturn(0L)
        return clock
    }
}