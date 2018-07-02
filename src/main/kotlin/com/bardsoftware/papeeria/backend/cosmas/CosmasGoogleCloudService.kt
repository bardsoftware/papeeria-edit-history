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
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.stub.StreamObserver
import name.fraser.neil.plaintext.diff_match_patch
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

    companion object {
        fun md5Hash(text: String): String {
            return Hashing.md5().newHasher().putString(text, Charsets.UTF_8).hash().toString()
        }

        val COSMAS_ID = "robot:::cosmas"
    }

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
                project[request.fileId] = fileVersion.toBuilder()
                        .addPatches(request.patch)
                        .build()
            } else {
                project[request.fileId] = CosmasProto.FileVersion.newBuilder()
                        .addPatches(request.patch)
                        .build()
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

        val response = CosmasProto.CommitVersionResponse.newBuilder()

        try {
            for ((fileId, fileVersion) in project) {
                try {
                    val text = fileVersion.content.toStringUtf8()
                    val patches = mutableListOf<CosmasProto.Patch>()
                    patches.addAll(fileVersion.patchesList)
                    patches.sortBy { it.timestamp }

                    val newText = PatchCorrector.applyPatch(patches, text)
                    val cosmasHash = md5Hash(newText)
                    if (patches.isEmpty() || patches.last().actualHash == cosmasHash) {
                        val newVersion = fileVersion.toBuilder()
                                .setContent(ByteString.copyFrom(newText.toByteArray()))
                        this.storage.create(
                                BlobInfo.newBuilder(this.bucketName, fileId).build(),
                                newVersion.build().toByteArray())
                        project[fileId] = newVersion
                                .clearPatches()
                                .build()
                    } else {
                        val actualHash = patches.last().actualHash
                        LOG.error("Commit failure: " +
                                "File # $fileId has Cosmas hash=$cosmasHash, but last actual hash=$actualHash")
                        val badFile = CosmasProto.FileInfo.newBuilder()
                                .setFileId(fileId)
                                .setProjectId(request.projectId)
                                .build()
                        response.addBadFiles(badFile)
                    }
                } catch (e: Throwable) {
                    LOG.error("Error while applying patches to file # $fileId")
                    when (e) {
                        is PatchCorrector.ApplyPatchException, is IllegalArgumentException -> {
                            val badFile = CosmasProto.FileInfo.newBuilder()
                                    .setFileId(fileId)
                                    .setProjectId(request.projectId)
                                    .build()
                            response.addBadFiles(badFile)
                        }
                        else -> throw e
                    }

                }
            }
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }

        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        LOG.info("Get request for version ${request.version} file # ${request.fileId}")
        val blob: Blob? = try {
            this.storage.get(BlobId.of(this.bucketName, request.fileId, request.version))
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

    private fun getPatchListAndPreviousText(fileId: String, timestamp: Long): Pair<List<CosmasProto.Patch>, String> {
        val blobs: Page<Blob> = try {
            this.storage.list(this.bucketName, Storage.BlobListOption.versions(true),
                    Storage.BlobListOption.prefix(fileId))
        } catch (e: StorageException) {
            throw e
        }
        val patchList = mutableListOf<CosmasProto.Patch>()
        var closestTimestamp = -1L
        var previousText = ""
        blobs.iterateAll().forEach {
            if (it.createTime >= timestamp) {
                patchList.addAll(CosmasProto.FileVersion.parseFrom(it.getContent()).patchesList)
            } else if (it.createTime > closestTimestamp) {
                closestTimestamp = it.createTime
                previousText = CosmasProto.FileVersion.parseFrom(it.getContent()).content.toStringUtf8()
            }
        }
        patchList.sortBy { patch -> patch.timestamp }
        return Pair(patchList, previousText)
    }


    override fun deletePatch(request: CosmasProto.DeletePatchRequest,
                             responseObserver: StreamObserver<CosmasProto.DeletePatchResponse>) {
        // Timestamp of file version witch contains patch
        val versionTimestamp = this.storage.get(BlobId.of(this.bucketName, request.fileId, request.generation)).createTime
        // Text of version from which patch was applied (version before version witch contains patch)
        val (patchList, text) = try {
            getPatchListAndPreviousText(request.fileId, versionTimestamp)
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        var indexCandidateDeletePatch = -1
        for ((patchIndex, patch) in patchList.iterator().withIndex()) {
            if (patch.timestamp == request.patchTimestamp) {
                indexCandidateDeletePatch = patchIndex
                break
            }
        }
        if (indexCandidateDeletePatch == -1) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "Can't delete patch. There is no such patch in storage")
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        val textWithoutPatch: String
        try {
            val textBeforeCandidateDelete = PatchCorrector.applyPatch(patchList.subList(0, indexCandidateDeletePatch), text)
            val finishText = CosmasProto.FileVersion.parseFrom(
                    this.storage.get(BlobId.of(this.bucketName, request.fileId)).getContent()).content.toStringUtf8()
            textWithoutPatch = PatchCorrector.applyPatch(
                    PatchCorrector.deletePatch(
                            patchList[indexCandidateDeletePatch],
                            patchList.subList(indexCandidateDeletePatch + 1, patchList.size),
                            textBeforeCandidateDelete),
                    finishText)
        } catch (e: PatchCorrector.ApplyPatchException) {
            val status = Status.INTERNAL.withDescription(e.message)
            LOG.error("Can't apply patch: ${e.message}")
            responseObserver.onError(StatusException(status))
            return
        }
        val response = CosmasProto.DeletePatchResponse.newBuilder()
        response.content = ByteString.copyFromUtf8(textWithoutPatch)
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun deleteFile(request: CosmasProto.DeleteFileRequest,
                            responseObserver: StreamObserver<CosmasProto.DeleteFileResponse>) {
        LOG.info("""Get request for delete file "${request.fileName}" # ${request.fileId}""")
        val cemeteryName = "${request.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(BlobId.of(this.bucketName, cemeteryName))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val newTomb = CosmasProto.FileTomb.newBuilder()
                .setFileId(request.fileId)
                .setFileName(request.fileName)
                .setRemovalTimestamp(request.removalTimestamp)
                .build()
        val cemetery = if (cemeteryBytes == null) {
            CosmasProto.FileCemetery.newBuilder()
        } else {
            CosmasProto.FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        try {
            this.storage.create(
                    BlobInfo.newBuilder(this.bucketName, cemeteryName).build(),
                    cemetery.addCemetery(newTomb).build().toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.DeleteFileResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun deletedFileList(request: CosmasProto.DeletedFileListRequest,
                                 responseObserver: StreamObserver<CosmasProto.DeletedFileListResponse>) {
        LOG.info("Get request for list deleted files in project # ${request.projectId}")
        val cemeteryName = "${request.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(BlobId.of(this.bucketName, cemeteryName))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val cemetery = if (cemeteryBytes == null) {
            CosmasProto.FileCemetery.newBuilder()
        } else {
            CosmasProto.FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        val response = CosmasProto.DeletedFileListResponse.newBuilder()
        response.addAllFiles(cemetery.cemeteryList)
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun forcedFileCommit(request: CosmasProto.ForcedFileCommitRequest,
                                  responseObserver: StreamObserver<CosmasProto.ForcedFileCommitResponse>) {
        LOG.info("Get request for commit file # ${request.fileId} in force")
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getValue(request.projectId)
        }
        // get last version from storage
        val blob: Blob? = try {
            this.storage.get(BlobId.of(this.bucketName, request.fileId))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val previousVersion = if (blob != null) {
            CosmasProto.FileVersion.parseFrom(blob.getContent()).content.toStringUtf8()
        } else ""

        val actualVersion = request.actualContent.toStringUtf8()
        val diffPatch = diff_match_patch().patch_toText(diff_match_patch().patch_make(previousVersion, actualVersion))
        val patch = CosmasProto.Patch.newBuilder()
                .setText(diffPatch)
                .setUserId(COSMAS_ID)
                .setTimestamp(request.timestamp)
                .setActualHash(md5Hash(actualVersion))
                .build()
        val newVersion = CosmasProto.FileVersion.newBuilder()
                .addPatches(patch)
                .setContent(ByteString.copyFrom(actualVersion.toByteArray()))
                .build()
        try {
            this.storage.create(
                    BlobInfo.newBuilder(this.bucketName, request.fileId).build(),
                    newVersion.toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        synchronized(project) {
            project[request.fileId] = newVersion.toBuilder()
                    .clearPatches()
                    .build()
        }
        synchronized(this.fileBuffer) {
            this.fileBuffer[request.projectId] = project
        }
        val response = CosmasProto.ForcedFileCommitResponse.newBuilder().build()
        responseObserver.onNext(response)
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
            this.storage.get(BlobId.of(this.bucketName, fileId, version))
        } catch (e: StorageException) {
            return null
        }
        return CosmasProto.FileVersion.parseFrom(blob?.getContent()).patchesList
    }

}