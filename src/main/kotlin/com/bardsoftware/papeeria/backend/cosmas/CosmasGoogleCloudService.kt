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
import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import java.time.Clock


private val LOG = LoggerFactory.getLogger("CosmasGoogleCloudService")

/**
 * Special class that can work with requests from CosmasClient
 * This realization stores files in Google Cloud Storage.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudService(private val freeBucketName: String,
                               private val paidBucketName: String,
                               private val storage: Storage = StorageOptions.getDefaultInstance().service,
                               private val clock: Clock = Clock.systemUTC(),
                               gsutilImageName: String = "") : CosmasGrpc.CosmasImplBase() {

    constructor(bucketName: String,
                storage: Storage = StorageOptions.getDefaultInstance().service,
                clock: Clock = Clock.systemUTC(),
                gsutilImageName: String = "")
            : this(bucketName, bucketName, storage, clock, gsutilImageName)


    private val fileBuffer =
            ConcurrentHashMap<String, ConcurrentMap<String, CosmasProto.FileVersion>>()
                    .withDefault { ConcurrentHashMap() }

    private val gsutilCommand = if (gsutilImageName != "") "docker run ${gsutilImageName} gsutil" else "gsutil"

    companion object {
        fun md5Hash(text: String): String {
            return Hashing.md5().newHasher().putString(text, Charsets.UTF_8).hash().toString()
        }

        val COSMAS_ID = "robot:::cosmas"
    }

    fun bucketName(isFreePlan: Boolean): String = if (isFreePlan) this.freeBucketName else this.paidBucketName

    fun hashUserId(userId: String) = md5Hash(userId)

    fun fileStorageName(fileId: String, info: ProjectInfo): String = hashUserId(info.ownerId) + "/" + fileId

    fun getBlobId(fileId: String, info: ProjectInfo, generation: Long? = null): BlobId {
        return BlobId.of(bucketName(info.isFreePlan), fileStorageName(fileId, info), generation)
    }

    fun getBlobInfo(fileId: String, info: ProjectInfo): BlobInfo {
        return BlobInfo.newBuilder(getBlobId(fileId, info)).build()
    }

    override fun createPatch(request: CosmasProto.CreatePatchRequest,
                             responseObserver: StreamObserver<CosmasProto.CreatePatchResponse>) {
        if (request.patchesList.isEmpty()) {
            val errorStatus = Status.INVALID_ARGUMENT.withDescription(
                    "Patches list at request is empty")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        val userId = request.patchesList.first().userId
        LOG.info("Get request for create new patch of file={} by user={}", request.fileId, userId)
        synchronized(this.fileBuffer) {
            val project = this.fileBuffer.getValue(request.info.projectId)
            val fileVersion = project[request.fileId]
            if (fileVersion != null) {
                project[request.fileId] = fileVersion.toBuilder()
                        .addAllPatches(request.patchesList)
                        .build()
            } else {
                project[request.fileId] = CosmasProto.FileVersion.newBuilder()
                        .addAllPatches(request.patchesList)
                        .build()
            }
            this.fileBuffer[request.info.projectId] = project
        }
        val response: CosmasProto.CreatePatchResponse = CosmasProto.CreatePatchResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }


    override fun commitVersion(request: CosmasProto.CommitVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CommitVersionResponse>) {
        LOG.info("Get request for commit last version of files in project={}", request.info.projectId)
        val project = this.fileBuffer[request.info.projectId]
        if (project == null) {
            val errorStatus = Status.INVALID_ARGUMENT.withDescription(
                    "There is no project in buffer with project id ${request.info.projectId}")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }

        val response = CosmasProto.CommitVersionResponse.newBuilder()

        try {
            for ((fileId, fileVersion) in project) {
                try {
                    val text = fileVersion.content.toStringUtf8()
                    val patches = fileVersion.patchesList.toMutableList()
                    if (patches.isEmpty()) {
                        LOG.info("File={} has no patches, no need to commit it", fileId)
                        continue
                    }
                    patches.sortBy { it.timestamp }

                    val newText = PatchCorrector.applyPatch(patches, text)
                    val cosmasHash = md5Hash(newText)
                    if (patches.last().actualHash == cosmasHash) {
                        val newVersion = fileVersion.toBuilder()
                                .setContent(ByteString.copyFrom(newText.toByteArray()))
                                .setTimestamp(clock.millis())
                        this.storage.create(getBlobInfo(fileId, request.info),
                                newVersion.build().toByteArray())
                        project[fileId] = newVersion
                                .clearPatches()
                                .build()
                        LOG.info("File={} has been committed", fileId)
                    } else {
                        val actualHash = patches.last().actualHash
                        LOG.error("Commit failure: File={} has Cosmas hash={}, but last actual hash={}",
                                fileId, cosmasHash, actualHash)
                        val badFile = CosmasProto.FileInfo.newBuilder()
                                .setFileId(fileId)
                                .setProjectId(request.info.projectId)
                                .build()
                        response.addBadFiles(badFile)
                    }
                } catch (e: Throwable) {
                    LOG.error("Error while applying patches to file={}", fileId)
                    when (e) {
                        is PatchCorrector.ApplyPatchException, is IllegalArgumentException -> {
                            val badFile = CosmasProto.FileInfo.newBuilder()
                                    .setFileId(fileId)
                                    .setProjectId(request.info.projectId)
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
        // if request.generation is -1, Cosmas will return the latest version of file
        val generation = if (request.generation == -1L) {
            LOG.info("Get request for the latest version of file={}", request.fileId)
            null // In GCS if generation is null it returns the latest version
        } else {
            LOG.info("Get request for generation {} of file={}", request.generation, request.fileId)
            request.generation
        }

        val blob: Blob? = try {
            this.storage.get(getBlobId(request.fileId, request.info, generation))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if (blob == null) {
            val errorStatus = Status.NOT_FOUND.withDescription(
                    "There is no such file or file version in storage")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        response.file = CosmasProto.FileVersion.parseFrom(blob.getContent())
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun fileVersionList(request: CosmasProto.FileVersionListRequest,
                                 responseObserver: StreamObserver<CosmasProto.FileVersionListResponse>) {
        LOG.info("Get request for list of versions file={}", request.fileId)
        val prevIds = try {
            getPrevIds(request.info)
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.FileVersionListResponse.newBuilder()
        var curFileId = request.fileId
        val versionList = mutableListOf<CosmasProto.FileVersionInfo>()
        while (curFileId != null) {
            val blobs: Page<Blob> = try {
                this.storage.list(bucketName(request.info.isFreePlan), Storage.BlobListOption.versions(true),
                        Storage.BlobListOption.prefix(fileStorageName(curFileId, request.info)))
            } catch (e: StorageException) {
                handleStorageException(e, responseObserver)
                return
            }
            blobs.iterateAll().forEach {
                val fileVersion = FileVersion.parseFrom(it.getContent())
                val versionInfo = CosmasProto.FileVersionInfo.newBuilder()
                        .setGeneration(it.generation)
                        .setTimestamp(fileVersion.timestamp)
                        .setFileId(curFileId)
                versionList.add(versionInfo.build())
            }
            curFileId = prevIds[curFileId]
        }
        versionList.sortBy { it.timestamp }
        response.addAllVersions(versionList)
        if (response.versionsList.isEmpty()) {
            val errorStatus = Status.NOT_FOUND.withDescription(
                    "There is no file in storage with file id ${request.fileId}")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun getPatchListAndPreviousText(fileName: String, timestamp: Long,
                                            info: ProjectInfo): Pair<List<CosmasProto.Patch>, String> {
        val blobs: Page<Blob> = try {
            this.storage.list(bucketName(info.isFreePlan), Storage.BlobListOption.versions(true),
                    Storage.BlobListOption.prefix(fileName))
        } catch (e: StorageException) {
            throw e
        }
        val patchList = mutableListOf<CosmasProto.Patch>()
        var closestTimestamp = -1L
        var previousText = ""
        blobs.iterateAll().forEach {
            val fileVersion = FileVersion.parseFrom(it.getContent())
            if (fileVersion.timestamp >= timestamp) {
                patchList.addAll(CosmasProto.FileVersion.parseFrom(it.getContent()).patchesList)
            } else if (fileVersion.timestamp > closestTimestamp) {
                closestTimestamp = fileVersion.timestamp
                previousText = CosmasProto.FileVersion.parseFrom(it.getContent()).content.toStringUtf8()
            }
        }
        patchList.sortBy { patch -> patch.timestamp }
        return Pair(patchList, previousText)
    }


    override fun deletePatch(request: CosmasProto.DeletePatchRequest,
                             responseObserver: StreamObserver<CosmasProto.DeletePatchResponse>) {
        LOG.info("Get request for delete patch from file={} with generation={}", request.fileId, request.generation)
        // Timestamp of file version witch contains patch
        val versionTimestamp = FileVersion.parseFrom(
                this.storage.get(getBlobId(request.fileId, request.info, request.generation)).getContent()).timestamp
        // Text of version from which patch was applied (version before version witch contains patch)
        val (patchList, text) = try {
            getPatchListAndPreviousText(fileStorageName(request.fileId, request.info), versionTimestamp, request.info)
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
            val errorStatus = Status.NOT_FOUND.withDescription(
                    "Can't delete patch. There is no such patch in storage")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        val textWithoutPatch: String
        try {
            val textBeforeCandidateDelete = PatchCorrector.applyPatch(
                    patchList.subList(0, indexCandidateDeletePatch), text)
            val finishText = CosmasProto.FileVersion.parseFrom(
                    this.storage.get(getBlobId(request.fileId, request.info)).getContent()).content.toStringUtf8()
            textWithoutPatch = PatchCorrector.applyPatch(
                    PatchCorrector.deletePatch(
                            patchList[indexCandidateDeletePatch],
                            patchList.subList(indexCandidateDeletePatch + 1, patchList.size),
                            textBeforeCandidateDelete),
                    finishText)
        } catch (e: PatchCorrector.ApplyPatchException) {
            val errorStatus = Status.INTERNAL.withDescription(e.message)
                    .withDescription("Can't apply patch: ${e.message}")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        val response = CosmasProto.DeletePatchResponse.newBuilder()
        response.content = ByteString.copyFromUtf8(textWithoutPatch)
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun deleteFile(request: CosmasProto.DeleteFileRequest,
                            responseObserver: StreamObserver<CosmasProto.DeleteFileResponse>) {
        LOG.info("""Get request for delete file={} with name "{}"""", request.fileId, request.fileName)
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
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
                    getBlobInfo(cemeteryName, request.info),
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
        LOG.info("Get request for list deleted files in project={}", request.info.projectId)
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
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
        LOG.info("Get request for commit file={} in force", request.fileId)
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getValue(request.info.projectId)
        }
        // get last version from storage
        val blob: Blob? = try {
            this.storage.get(getBlobId(request.fileId, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val previousVersion = if (blob != null) {
            CosmasProto.FileVersion.parseFrom(blob.getContent()).content.toStringUtf8()
        } else ""

        val actualVersion = request.actualContent.toStringUtf8()
        val patch = getDiffPatch(previousVersion, actualVersion, request.timestamp)

        val newVersion = CosmasProto.FileVersion.newBuilder()
                .addPatches(patch)
                .setContent(ByteString.copyFrom(actualVersion.toByteArray()))
                .setTimestamp(clock.millis())
                .build()
        try {
            this.storage.create(
                    getBlobInfo(request.fileId, request.info),
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
            this.fileBuffer[request.info.projectId] = project
        }
        val response = CosmasProto.ForcedFileCommitResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun restoreDeletedFile(request: CosmasProto.RestoreDeletedFileRequest,
                                    responseObserver: StreamObserver<CosmasProto.RestoreDeletedFileResponse>) {
        LOG.info("Get request for restore deleted file={}", request.fileId)
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val cemetery = if (cemeteryBytes == null) {
            CosmasProto.FileCemetery.newBuilder()
        } else {
            CosmasProto.FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        val tombs = cemetery.cemeteryList.toMutableList()
        tombs.removeIf { it -> it.fileId == request.fileId }
        try {
            this.storage.create(
                    getBlobInfo(cemeteryName, request.info),
                    cemetery.clearCemetery().addAllCemetery(tombs).build().toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.RestoreDeletedFileResponse.getDefaultInstance()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun changeFileId(request: CosmasProto.ChangeFileIdRequest,
                              responseObserver: StreamObserver<CosmasProto.ChangeFileIdResponse>) {
        LOG.info("Get request for change files ids in project={}", request.info.projectId)
        val prevIds = try {
            getPrevIds(request.info).toMutableMap()
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getValue(request.info.projectId)
        }
        synchronized(project) {
            for (change in request.changesList) {
                prevIds[change.newFileId] = change.oldFileId
                if (project.contains(change.oldFileId)) {
                    project[change.newFileId] = project[change.oldFileId]
                    project.remove(change.oldFileId)
                }

            }
            try {
                this.storage.create(
                        getBlobInfo("${request.info.projectId}-fileIdMap", request.info),
                        CosmasProto.FileIdMap.newBuilder().putAllPrevIds(prevIds).build().toByteArray())
            } catch (e: StorageException) {
                handleStorageException(e, responseObserver)
                return
            }
        }
        val response = CosmasProto.ChangeFileIdResponse.getDefaultInstance()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun changeUserPlan(request: ChangeUserPlanRequest,
                                responseObserver: StreamObserver<ChangeUserPlanResponse>) {
        changeUserPlan(request, responseObserver, Runtime.getRuntime())
    }

    fun changeUserPlan(request: ChangeUserPlanRequest,
                       responseObserver: StreamObserver<ChangeUserPlanResponse>,
                       runtime: Runtime) {
        val isFreePlanNow = request.isFreePlanNow // plan AFTER changing
        if (isFreePlanNow) {
            LOG.info("Get request for change user={} plan from paid to free", request.userId)
        } else {
            LOG.info("Get request for change user={} plan from free to paid", request.userId)
        }
        val oldBucketName = bucketName(!isFreePlanNow)
        val newBucketName = bucketName(isFreePlanNow)
        val gsutilCopyCommand = "$gsutilCommand cp -r -A gs://$oldBucketName/${hashUserId(request.userId)}/*" +
                " gs://$newBucketName/${hashUserId(request.userId)}"
        val copyProcess = runtime.exec(gsutilCopyCommand)
        val copyRes = copyProcess.waitFor()
        if (copyRes != 0) {
            val errorStatus = Status.INTERNAL.withDescription(
                    "Can't copy user # ${request.userId} files from $oldBucketName to $newBucketName bucket")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        val gsutilRemoveCommand = "$gsutilCommand rm -r -a gs://$oldBucketName/${hashUserId(request.userId)}"
        val removeProcess = runtime.exec(gsutilRemoveCommand)
        val removeRes = removeProcess.waitFor()
        if (removeRes != 0) {
            val errorStatus = Status.INTERNAL.withDescription(
                    "Can't remove user # ${request.userId} files from old bucket $oldBucketName")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return
        }
        val response = CosmasProto.ChangeUserPlanResponse.getDefaultInstance()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun getDiffPatch(oldText: String, newText: String, timestamp: Long): CosmasProto.Patch {
        val diffPatch = diff_match_patch().patch_toText(diff_match_patch().patch_make(oldText, newText))
        return CosmasProto.Patch.newBuilder()
                .setText(diffPatch)
                .setUserId(COSMAS_ID)
                .setUserName("Papeeria")
                .setTimestamp(timestamp)
                .setActualHash(md5Hash(newText))
                .build()
    }


    private fun handleStorageException(e: StorageException, responseObserver: StreamObserver<*>) {
        LOG.error("StorageException happened at Cosmas", e)
        responseObserver.onError(e)
    }

    private fun getPrevIds(info: ProjectInfo): Map<String, String> {
        val mapName = "${info.projectId}-fileIdMap"
        val mapBytes: Blob = this.storage.get(getBlobId(mapName, info))
                ?: return mapOf()
        return CosmasProto.FileIdMap.parseFrom(mapBytes.getContent()).prevIdsMap
    }

    fun deleteFile(fileId: String, info: ProjectInfo) {
        LOG.info("Delete file={}", fileId)
        try {
            this.storage.delete(getBlobId(fileId, info))
        } catch (e: StorageException) {
            LOG.error("Deleting file failed at Cosmas", e)
        }
    }

    fun getPatchList(projectId: String, fileId: String): List<CosmasProto.Patch>? {
        return fileBuffer[projectId]?.get(fileId)?.patchesList
    }

    fun getPatchListFromStorage(fileId: String, generation: Long, info: ProjectInfo): List<CosmasProto.Patch>? {
        val blob: Blob? = try {
            this.storage.get(getBlobId(fileId, info, generation))
        } catch (e: StorageException) {
            return null
        }
        return CosmasProto.FileVersion.parseFrom(blob?.getContent()).patchesList
    }

}