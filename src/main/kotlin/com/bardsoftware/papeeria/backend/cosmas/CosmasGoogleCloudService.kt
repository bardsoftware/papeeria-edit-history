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
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

private val LOG = LoggerFactory.getLogger("CosmasGoogleCloudService")

/**
 * Special class that can work with requests from CosmasClient
 * This realization stores files in Google Cloud Storage.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudService(private val bucketName: String,
                               private val storage: Storage = StorageOptions.getDefaultInstance().service) : CosmasGrpc.CosmasImplBase() {

    private val fileBuffer = ConcurrentHashMap<String, ConcurrentMap<String,
            Pair<ByteString, MutableList<CosmasInMemoryService.Patch>>>>().withDefault { ConcurrentHashMap() }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        LOG.info("Get request for create new version of file # ${request.fileId}")
        synchronized(this.fileBuffer) {
            val project = this.fileBuffer.getValue(request.projectId)
            project[request.fileId] = Pair(request.file, mutableListOf())
            this.fileBuffer[request.projectId] = project
        }
        val response = CosmasProto.CreateVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun createPatch(request: CosmasProto.CreatePatchRequest,
                             responseObserver: StreamObserver<CosmasProto.CreatePatchResponse>) {
        LOG.info("Get request for create new patch of file # ${request.fileId} by user ${request.userId}")
        synchronized(this.fileBuffer) {
            val project = this.fileBuffer.getValue(request.projectId)
            val fileInformation = project[request.fileId]
            val patchesList = fileInformation?.second ?: mutableListOf()
            patchesList.add(CosmasInMemoryService.Patch(request.userId, request.text, request.timeStamp))
            project[request.fileId] = Pair(fileInformation?.first ?: ByteString.EMPTY, patchesList)
            this.fileBuffer[request.projectId] = project
        }
        val response: CosmasProto.CreatePatchResponse = CosmasProto.CreatePatchResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun commitVersion(request: CosmasProto.CommitVersionRequest, responseObserver: StreamObserver<CosmasProto.CommitVersionResponse>) {
        LOG.info("Get request for commit last version of files in project # ${request.projectId}")
        val project = this.fileBuffer[request.projectId]
        if (project == null) {
            val status = Status.INVALID_ARGUMENT.withDescription(
                    "There is no project in buffer with project id ${request.projectId}")
            LOG.error(status.description)
            responseObserver.onError(StatusException(status))
            return
        }
        try {
            for ((fileId, pair) in project) {
                val outputStream = ByteArrayOutputStream()
                val output = ObjectOutputStream(outputStream)
                output.writeObject(pair)
                output.flush()
                this.storage.create(
                        BlobInfo.newBuilder(this.bucketName, fileId).build(),
                        outputStream.toByteArray())
            }
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }

        val response = CosmasProto.CommitVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        LOG.info("Get request for version ${request.version} file # ${request.fileId}")
        val blob: Blob? = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, request.fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(request.version))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if (blob == null) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "There is no such file or file version in storage")
            LOG.error("This request is incorrect: " + requestStatus.description)
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        val ans = blob.getContent()
        val inputStream = ByteArrayInputStream(ans)
        val input = ObjectInputStream(inputStream)
        val pair = input.readObject()
        if (pair is Pair<*, *> && pair.first is ByteString)
            response.file = pair.first as ByteString
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun fileVersionList(request: CosmasProto.FileVersionListRequest,
                                 responseObserver: StreamObserver<CosmasProto.FileVersionListResponse>) {
        LOG.info("Get request for list of versions file # ${request.fileId}")
        val response = CosmasProto.FileVersionListResponse.newBuilder()
        val blobs: Page<Blob> = try {
            this.storage.list(this.bucketName, Storage.BlobListOption.versions(true),
                    Storage.BlobListOption.prefix(request.fileId))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        blobs.iterateAll().forEach {
            response.addVersions(it.generation)
        }
        if (response.versionsList.isEmpty()) {
            val status = Status.INVALID_ARGUMENT.withDescription(
                    "There is no file in storage with file id ${request.fileId}")
            LOG.error(status.description)
            responseObserver.onError(StatusException(status))
            return
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun handleStorageException(e: StorageException, responseObserver: StreamObserver<*>) {
        LOG.error("StorageException happened: ${e.message}")
        responseObserver.onError(e)
    }

    fun deleteFile(fileId: String) {
        LOG.info("Delete file # $fileId")
        try {
            this.storage.delete(BlobInfo.newBuilder(this.bucketName, fileId).build().blobId)
        } catch (e: StorageException) {
            LOG.info("Deleting file failed: ${e.message}")
        }
    }

    fun getPatchList(projectId: String, fileId: String): MutableList<CosmasInMemoryService.Patch>? {
        return fileBuffer[projectId]?.get(fileId)?.second
    }

    fun getPatchListFromMemory(fileId: String, version: Long): MutableList<CosmasInMemoryService.Patch>? {
        val blob: Blob? = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(version))
        } catch (e: StorageException) {
            return null
        }
        val ans = blob?.getContent()
        val inputStream = ByteArrayInputStream(ans)
        val input = ObjectInputStream(inputStream)
        val pair = input.readObject()
        if (pair is Pair<*, *> && pair.second is MutableList<*>)
            return pair.second as MutableList<CosmasInMemoryService.Patch>?
        return null
    }
}