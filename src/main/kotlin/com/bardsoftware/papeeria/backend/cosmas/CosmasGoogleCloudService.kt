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

    private val fileBuffer = ConcurrentHashMap<String, ConcurrentMap<String, CosmasProto.FileVersion>>().withDefault { ConcurrentHashMap() }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        LOG.info("Get request for create new version of file # ${request.fileId}")
        synchronized(this.fileBuffer) {
            val project = this.fileBuffer.getValue(request.projectId)
            val patchList = project[request.fileId]?.patchesList ?: mutableListOf()
            project[request.fileId] = CosmasProto.FileVersion.newBuilder()
                    .setContent(request.file)
                    .addAllPatches(patchList)
                    .build()
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
        LOG.info("Get request for create new patch of file # ${request.fileId} by user ${request.patch.userId}")
        synchronized(this.fileBuffer) {
            val project = this.fileBuffer.getValue(request.projectId)
            val fileVersion = project[request.fileId]
            if (fileVersion != null) {
                project[request.fileId] = fileVersion.toBuilder().addPatches(request.patch).build()
            } else {
                project[request.fileId] = CosmasProto.FileVersion.newBuilder().addPatches(request.patch).build()
            }
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
            for ((fileId, fileVersion) in project) {
                this.storage.create(
                        BlobInfo.newBuilder(this.bucketName, fileId).build(),
                        fileVersion.toByteArray())
            }
            project.clear()
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
        response.file = CosmasProto.FileVersion.parseFrom(blob.getContent())
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
            val versionInfo = CosmasProto.FileVersionInfo.newBuilder()
                    .setGeneration(it.generation)
                    .setTimestamp(it.createTime)
            response.addVersions(versionInfo.build())
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

    override fun getTextWithoutPatch(request: CosmasProto.getTextWithoutPatchRequest,
                              responseObserver: StreamObserver<CosmasProto.getTextWithoutPatchResponse>) {
        var askedVersion = request.getVersionRequest.version
        val blobRequestVersion: Blob? = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, request.getVersionRequest.fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(askedVersion))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        if (blobRequestVersion == null) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "Can't delete patch. There is no such file version in storage")
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        val blobPreviousVersion: Blob?  = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, request.getVersionRequest.fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(askedVersion - 1))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        if (blobPreviousVersion == null && askedVersion > 0) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "Can't delete patch. There is no such file version in storage")
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        val fileVersion1 = CosmasProto.FileVersion.parseFrom(blobRequestVersion.getContent())
        val fileVersion2 = if (blobPreviousVersion != null) { CosmasProto.FileVersion.parseFrom(blobPreviousVersion.getContent()) } else { null }
        val text = if (fileVersion2 != null) { fileVersion2.content.toStringUtf8() } else {""}
        val patchList = mutableListOf<CosmasProto.Patch>()
        patchList.addAll(fileVersion1.patchesList)
        var indexCandidateDeletePatch = -1
        for ((patchIndex, patch) in patchList.withIndex()) {
            if (patch.timeStamp == request.patchTimeStamp) {
                indexCandidateDeletePatch = patchIndex
            }
        }
        if (indexCandidateDeletePatch == -1) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "Can't delete patch. There is no such patch in storage")
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        while (true) {
            askedVersion++
            val blob: Blob = try {
                this.storage.get(BlobInfo.newBuilder(this.bucketName, request.getVersionRequest.fileId).build().blobId,
                        Storage.BlobGetOption.generationMatch(askedVersion))
            } catch (e: StorageException) {
                handleStorageException(e, responseObserver)
                return
            } ?: break
            val fileVersion = CosmasProto.FileVersion.parseFrom(blob.getContent())
            patchList.addAll(fileVersion.patchesList)
        }
        val textBeforeCandidateDelete = PatchCorrector.applyPatch(patchList.subList(0, indexCandidateDeletePatch), text)
        val finishText = PatchCorrector.applyPatch(patchList, text)
        val textNoPatch = PatchCorrector.applyPatch(
                PatchCorrector.deletePatch(
                        patchList[indexCandidateDeletePatch],
                        patchList.subList(indexCandidateDeletePatch + 1, patchList.size),
                textBeforeCandidateDelete),
                finishText
        )
        val response = CosmasProto.getTextWithoutPatchResponse.newBuilder()
        response.content = ByteString.copyFromUtf8(textNoPatch)
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

    fun getPatchList(projectId: String, fileId: String): List<CosmasProto.Patch>? {
        return fileBuffer[projectId]?.get(fileId)?.patchesList
    }

    fun getPatchListFromStorage(fileId: String, version: Long): List<CosmasProto.Patch>? {
        val blob: Blob? = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(version))
        } catch (e: StorageException) {
            return null
        }
        return CosmasProto.FileVersion.parseFrom(blob?.getContent()).patchesList
    }

}