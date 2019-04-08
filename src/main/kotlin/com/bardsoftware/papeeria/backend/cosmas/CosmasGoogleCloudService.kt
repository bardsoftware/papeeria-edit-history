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

import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.google.api.gax.paging.Page
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.storage.*
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.stub.StreamObserver
import name.fraser.neil.plaintext.diff_match_patch
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.threeten.bp.Duration
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors;
import kotlin.math.min


private val LOG = LoggerFactory.getLogger("CosmasGoogleCloudService")

private fun labels(entries: Map<String, String>) {
    entries.forEach { if (it.value.isNotBlank()) MDC.put(it.key, it.value) }
}

private fun logging(funName: String, projectId: String, fileId: String = "",
                    userId: String = "", other: Map<String, String> = mapOf(), body: () -> Unit) {
    labels(mapOf(
            "projectId" to projectId,
            "fileId" to fileId,
            "userId" to userId
    ))
    labels(other)
    LOG.info(">>> $funName")
    MDC.clear()
    try {
        body()
    } finally {
        MDC.clear()
        LOG.info("<<< $funName")
    }
}


/**
 * Special class that can work with requests from CosmasClient
 * This realization stores files in Google Cloud Storage.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudService(
        private val bucketName: String,
        private val storage: Storage = StorageOptions.getDefaultInstance().toBuilder().apply {
            this.setRetrySettings(RetrySettings.newBuilder()
                    .setInitialRetryDelay(Duration.ofSeconds(1))
                    .setMaxRetryDelay(Duration.ofSeconds(128))
                    .setRetryDelayMultiplier(2.0)
                    .setMaxAttempts(5)
                    .build()
            )
        }.build().service,
        private val clock: Clock = Clock.systemUTC(),
        private val windowMaxSize: Int = 10,
        private val bufferSaveExecutor: ExecutorService = Executors.newSingleThreadExecutor()) : CosmasGrpc.CosmasImplBase() {

    val fileBuffer = loadBufferFromGCS()

    companion object {
        fun md5Hash(text: String): String {
            return Hashing.md5().newHasher().putString(text, Charsets.UTF_8).hash().toString()
        }

        const val COSMAS_ID = "robot:::cosmas"
        const val COSMAS_NAME = "Version History Service"
        const val BUFFER_NAME_GCS = "File buffer"

        fun buildNewWindow(newInfo: FileVersionInfo, oldWindow: List<FileVersionInfo>,
                           windowMaxSize: Int): MutableList<FileVersionInfo> {
            val res = mutableListOf<FileVersionInfo>()
            res.add(newInfo)
            res.addAll(oldWindow.take(min(windowMaxSize - 1, oldWindow.size)))
            return res
        }
    }

    fun hashUserId(userId: String) = md5Hash(userId)

    fun fileStorageName(fileId: String, info: ProjectInfo): String = hashUserId(info.ownerId) + "/" + fileId

    fun getBlobId(fileId: String, info: ProjectInfo, generation: Long? = null): BlobId {
        return BlobId.of(bucketName, fileStorageName(fileId, info), generation)
    }

    fun getBlobInfo(fileId: String, info: ProjectInfo): BlobInfo {
        return BlobInfo.newBuilder(getBlobId(fileId, info)).build()
    }

    override fun createPatch(request: CosmasProto.CreatePatchRequest,
                             responseObserver: StreamObserver<CosmasProto.CreatePatchResponse>) {
        if (request.patchesList.isEmpty()) {
            return logging("createPatch", request.info.projectId, request.fileId) {
                val errorStatus = Status.INVALID_ARGUMENT.withDescription(
                        "No patches found in the request object")
                LOG.error(errorStatus.description)
                responseObserver.onError(StatusException(errorStatus))
            }
        }
        logging("createPatch", request.info.projectId, request.fileId, request.patchesList.first().userId) {
            val project = synchronized(this.fileBuffer) {
                this.fileBuffer.getOrPut(request.info.projectId) { ConcurrentHashMap() }
            }
            synchronized(project) {
                val fileVersion = try {
                    project[request.fileId] ?: restoreFileFromStorage(request.fileId, request.info, project)
                } catch (e: StorageException) {
                    handleStorageException(e, responseObserver)
                    return@logging
                }
                project[request.fileId] = fileVersion.toBuilder()
                        .addAllPatches(request.patchesList)
                        .build()
            }
            val response: CosmasProto.CreatePatchResponse = CosmasProto.CreatePatchResponse
                    .newBuilder()
                    .build()
            responseObserver.onNext(response)
            responseObserver.onCompleted()
        }
    }


    override fun commitVersion(request: CosmasProto.CommitVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CommitVersionResponse>) = logging(
            "commitVersion", request.info.projectId) {
        val project = this.fileBuffer[request.info.projectId]
        if (project == null) {
            LOG.info("No such project in the buffer")
            responseObserver.onNext(CosmasProto.CommitVersionResponse.getDefaultInstance())
            responseObserver.onCompleted()
            return@logging
        }

        val response = CosmasProto.CommitVersionResponse.newBuilder()
        synchronized(project) {
            try {
                for ((fileId, fileVersion) in project) {
                    try {
                        MDC.clear()
                        MDC.put("fileId", fileId)
                        val text = fileVersion.content.toStringUtf8()
                        val patches = fileVersion.patchesList.toMutableList()
                        if (patches.isEmpty()) {

                            LOG.info("File has no patches, no need to commit it")
                            continue
                        }
                        patches.sortBy { it.timestamp }

                        val newText = PatchCorrector.applyPatch(patches, text)
                        val cosmasHash = md5Hash(newText)
                        if (patches.last().actualHash == cosmasHash) {
                            commitFromMemoryToGCS(request.info, fileId, newText)
                            LOG.info("File has been committed")
                        } else {
                            val actualHash = patches.last().actualHash
                            LOG.error("""Commit failure: hash mismatch.
                              |Hash after applying patches={}. Last hash supplied by client={}.
                              |This means that the sequence of patches produces something different
                              | than actual file contents.""".trimMargin(),
                                    cosmasHash, actualHash)
                            val badFile = CosmasProto.FileInfo.newBuilder()
                                    .setFileId(fileId)
                                    .setProjectId(request.info.projectId)
                                    .build()
                            response.addBadFiles(badFile)
                        }
                    } catch (e: Throwable) {
                        LOG.error("Error while applying patches", e)
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
                return@logging
            }
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    // We have to pass newText there cause we can get new text version from this method
    // only by applying patches to version in memory. In "commit" we re applying patches
    // before we re calling this method, so we dont want to do it again.
    // In "forcedCommit" new text version is given in request.
    private fun commitFromMemoryToGCS(projectInfo: ProjectInfo, fileId: String, newText: String) {
        val project = this.fileBuffer[projectInfo.projectId] ?: return
        val fileVersion = project[fileId] ?: return

        // Just committing buffered version with new content and current timestamp to GCS
        val curTime = clock.millis()
        val newVersion = fileVersion.toBuilder()
                .setContent(ByteString.copyFromUtf8(newText))
                .setTimestamp(curTime)
        val resBlob = this.storage.create(getBlobInfo(fileId, projectInfo), newVersion.build().toByteArray())

        // User who made last patch
        val userName = newVersion.patchesList.last().userName

        // Preparing version in memory to save following invariants for it:
        // 1) History window points to the latest N versions in GCS
        // 2) It has content that equals to content of the latest version in GCS
        // 3) Patches are applicable to it content
        val newInfo = FileVersionInfo.newBuilder()
                .setFileId(fileId)
                // For in-memory storage implementation(used for tests only) resultBlob.generation == null,
                // but in this case we don't care about generation value, so I set it to 1L
                .setGeneration(resBlob.generation ?: 1L)
                .setTimestamp(curTime)
                .setUserName(userName)
                .build()
        val newWindow = buildNewWindow(newInfo, fileVersion.historyWindowList, windowMaxSize)
        project[fileId] = newVersion
                .clearPatches()
                .clearHistoryWindow()
                .addAllHistoryWindow(newWindow)
                .build()
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) = logging(
            "getVersion", request.info.projectId, request.fileId,
            other = mapOf("generation" to request.generation.toString())) {
        // if request.generation is -1, Cosmas will return the latest version of file
        val generation = if (request.generation == -1L) {
            null // In GCS if generation is null it returns the latest version
        } else {
            request.generation
        }

        val blob: Blob? = try {
            this.storage.get(getBlobId(request.fileId, request.info, generation))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if (blob == null) {
            val errorStatus = Status.NOT_FOUND.withDescription(
                    "There is no such file or file version in storage")
            LOG.error(errorStatus.description)
            responseObserver.onError(StatusException(errorStatus))
            return@logging
        }
        response.file = CosmasProto.FileVersion.parseFrom(blob.getContent())
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun fileVersionList(request: CosmasProto.FileVersionListRequest,
                                 responseObserver: StreamObserver<CosmasProto.FileVersionListResponse>) = logging(
            "fileVersionList", request.info.projectId, request.fileId,
            other = mapOf("generation" to request.startGeneration.toString())) {

        val response = CosmasProto.FileVersionListResponse.newBuilder()
        val versionList = try {
            getFileVersionList(request.info, request.fileId, request.startGeneration)
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        if (versionList.isEmpty()) {
            val description = "No versions found"
            val errorStatus = Status.NOT_FOUND.withDescription(description)
            LOG.error(description)
            responseObserver.onError(StatusException(errorStatus))
            return@logging
        }
        val actualVersionList = mutableListOf<FileVersionInfo>()
        val curTime = clock.millis()
        val day: Long = 24 * 60 * 60 * 1000
        val ttl = if (request.info.isFreePlan) {
            day
        } else {
            31 * day
        }
        for (version in versionList) {
            // Checking that version could been deleted by GCS after 31 days(Delta plan) or 1 day(Epsilon plan)
            if (version.timestamp + ttl > curTime) {
                actualVersionList.add(version)
            } else {
                break
            }
        }
        response.addAllVersions(actualVersionList)
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun getFileVersionList(info: ProjectInfo, fileId: String, startGeneration: Long): List<FileVersionInfo> {
        return if (startGeneration == -1L) {
            getFileVersionListFromMemory(info, fileId)
        } else {
            getFileVersionListFromStorage(info, fileId, startGeneration)
        }
    }

    private fun getFileVersionListFromMemory(projectInfo: ProjectInfo, fileId: String): List<FileVersionInfo> {
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getOrPut(projectInfo.projectId) { ConcurrentHashMap() }
        }
        val fileVersion = this.fileBuffer[projectInfo.projectId]?.get(fileId)
                ?: restoreFileFromStorage(fileId, projectInfo, project)
        return fileVersion.historyWindowList
    }

    private fun getFileVersionListFromStorage(projectInfo: ProjectInfo, fileId: String,
                                              startGeneration: Long): List<FileVersionInfo> {

        val blob: Blob? = this.storage.get(getBlobId(fileId, projectInfo, startGeneration)) // throws StorageException
        return if (blob != null) {
            CosmasProto.FileVersion.parseFrom(blob.getContent()).historyWindowList
        } else {
            emptyList()
        }
    }

    private fun getPatchListAndPreviousText(fileName: String, timestamp: Long,
                                            info: ProjectInfo): Pair<List<CosmasProto.Patch>, String> {
        val blobs: Page<Blob> = try {
            this.storage.list(bucketName, Storage.BlobListOption.versions(true),
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
        logging("deletePatch", request.info.projectId, request.fileId,
                other = mapOf(
                        "generation" to request.generation.toString(),
                        "patchTimestamp" to request.patchTimestamp.toString()
                )) {
            // Timestamp of file version witch contains patch
            val versionTimestamp = FileVersion.parseFrom(
                    this.storage.get(getBlobId(request.fileId, request.info, request.generation)).getContent()).timestamp
            // Text of version from which patch was applied (version before version witch contains patch)
            val (patchList, text) = try {
                getPatchListAndPreviousText(fileStorageName(request.fileId, request.info), versionTimestamp, request.info)
            } catch (e: StorageException) {
                handleStorageException(e, responseObserver)
                return@logging
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
                        "No patch found")
                LOG.error(errorStatus.description)
                responseObserver.onError(StatusException(errorStatus))
                return@logging
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
                LOG.error(errorStatus.description, e)
                responseObserver.onError(StatusException(errorStatus))
                return@logging
            }
            val response = CosmasProto.DeletePatchResponse.newBuilder()
            response.content = ByteString.copyFromUtf8(textWithoutPatch)
            responseObserver.onNext(response.build())
            responseObserver.onCompleted()
        }
    }

    override fun deleteFiles(request: CosmasProto.DeleteFilesRequest,
                             responseObserver: StreamObserver<CosmasProto.DeleteFilesResponse>) = logging(
            "deleteFiles", request.info.projectId) {
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        val cemetery = if (cemeteryBytes == null) {
            FileCemetery.newBuilder()
        } else {
            FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        for (file in request.filesList) {
            val newTomb = FileTomb.newBuilder()
                    .setFileId(file.fileId)
                    .setFileName(file.fileName)
                    .setRemovalTimestamp(request.removalTimestamp)
                    .build()
            cemetery.addCemetery(newTomb)
            LOG.info("File={} with name={} has been added to cemetery", file.fileId, file.fileName)
        }

        try {
            this.storage.create(
                    getBlobInfo(cemeteryName, request.info),
                    cemetery.build().toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }

        val response = DeleteFilesResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun deleteFile(request: CosmasProto.DeleteFileRequest,
                            responseObserver: StreamObserver<CosmasProto.DeleteFileResponse>) = logging(
            "deleteFile", request.info.projectId, request.fileId,
            other = mapOf("fileName" to request.fileName)) {
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        val newTomb = FileTomb.newBuilder()
                .setFileId(request.fileId)
                .setFileName(request.fileName)
                .setRemovalTimestamp(request.removalTimestamp)
                .build()
        val cemetery = if (cemeteryBytes == null) {
            FileCemetery.newBuilder()
        } else {
            FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        try {
            this.storage.create(
                    getBlobInfo(cemeteryName, request.info),
                    cemetery.addCemetery(newTomb).build().toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }

        val response = DeleteFileResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun deletedFileList(request: CosmasProto.DeletedFileListRequest,
                                 responseObserver: StreamObserver<CosmasProto.DeletedFileListResponse>) = logging(
            "deletedFileList", request.info.projectId) {
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
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
                                  responseObserver: StreamObserver<CosmasProto.ForcedFileCommitResponse>) = logging(
            "forcedFileCommit", request.info.projectId, request.fileId) {
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getOrPut(request.info.projectId) { ConcurrentHashMap() }
        }
        synchronized(project) {
            val versionToCommit = try {
                // Restoring the latest version in GCS to buffer
                restoreFileFromStorage(request.fileId, request.info, project)
            } catch (e: StorageException) {
                handleStorageException(e, responseObserver)
                return@logging
            }
            val actualText = request.actualContent.toStringUtf8()

            val patch = getDiffPatch(versionToCommit.content.toStringUtf8(), actualText, request.timestamp)
            project[request.fileId] = versionToCommit.toBuilder().addPatches(patch).build()

            // Committing correct version from buffer to GCS
            commitFromMemoryToGCS(request.info, request.fileId, actualText)
        }

        val response = CosmasProto.ForcedFileCommitResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun restoreFileFromStorage(fileId: String, projectInfo: ProjectInfo,
                                       project: ConcurrentMap<String, CosmasProto.FileVersion>): FileVersion {
        synchronized(project) {
            // Getting last version from storage or default instance if it doesn't exist
            val latestVersionBlob: Blob = this.storage.get(getBlobId(fileId, projectInfo))
                    ?: return FileVersion.getDefaultInstance()
            val latestVersion = CosmasProto.FileVersion.parseFrom(latestVersionBlob.getContent())
            LOG.info("Restoring from GCS to buffer file")

            val userName = if (latestVersion.patchesList.isEmpty()) {
                COSMAS_NAME
            } else {
                latestVersion.patchesList.last().userName
            }

            // Preparing new version in memory to replace bad or nonexistent one in buffer
            // Window should point to the latest N versions
            val latestVersionInfo = FileVersionInfo.newBuilder()
                    .setFileId(fileId)
                    // For in-memory storage implementation resultBlob.generation == null,
                    // but in this case we don't care about generation value, so I set it to 1L
                    .setGeneration(latestVersionBlob.generation ?: 1L)
                    .setUserName(userName)
                    .setTimestamp(latestVersion.timestamp)
                    .build()
            val bufferWindow = buildNewWindow(latestVersionInfo, latestVersion.historyWindowList, windowMaxSize)

            // Content should be equal to content of the latest version
            return FileVersion.newBuilder()
                    .setContent(latestVersion.content)
                    .addAllHistoryWindow(bufferWindow)
                    .build()
        }
    }

    override fun restoreDeletedFile(request: CosmasProto.RestoreDeletedFileRequest,
                                    responseObserver: StreamObserver<CosmasProto.RestoreDeletedFileResponse>) = logging(
            "restoreDeletedFile", request.info.projectId, request.oldFileId,
            other = mapOf("newFileId" to request.newFileId)) {
        LOG.info("")
        val cemeteryName = "${request.info.projectId}-cemetery"
        val cemeteryBytes: Blob? = try {
            this.storage.get(getBlobId(cemeteryName, request.info))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        val cemetery = if (cemeteryBytes == null) {
            CosmasProto.FileCemetery.newBuilder()
        } else {
            CosmasProto.FileCemetery.parseFrom(cemeteryBytes.getContent()).toBuilder()
        }
        val tombs = cemetery.cemeteryList.toMutableList()
        tombs.removeIf { it.fileId == request.oldFileId }
        try {
            this.storage.create(
                    getBlobInfo(cemeteryName, request.info),
                    cemetery.clearCemetery().addAllCemetery(tombs).build().toByteArray())
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return@logging
        }
        if (request.newFileId.isNotEmpty()) {
            val change = ChangeId.newBuilder()
                    .setOldFileId(request.oldFileId)
                    .setNewFileId(request.newFileId)
                    .build()
            changeFileId(request.info, listOf(change))
        }
        val response = CosmasProto.RestoreDeletedFileResponse.getDefaultInstance()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun changeFileId(request: CosmasProto.ChangeFileIdRequest,
                              responseObserver: StreamObserver<CosmasProto.ChangeFileIdResponse>) = logging(
            "changeFileId", request.info.projectId) {
        LOG.info("""Request:
          |${request}
        """.trimMargin())

        changeFileId(request.info, request.changesList)
        this.bufferSaveExecutor.submit {
            saveBufferToGCS()
        }

        val response = CosmasProto.ChangeFileIdResponse.getDefaultInstance()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun changeFileId(info: ProjectInfo, changes: List<ChangeId>) {
        val project = synchronized(this.fileBuffer) {
            this.fileBuffer.getOrPut(info.projectId) { ConcurrentHashMap() }
        }
        if (project != null) {
            synchronized(project) {
                for (change in changes) {
                    val oldVersion = project[change.oldFileId]
                            ?: restoreFileFromStorage(change.oldFileId, info, project)
                    project[change.newFileId] = oldVersion
                    project.remove(change.oldFileId)
                }
            }
        }
    }

    private fun saveBufferToGCS() {
        LOG.info("Saving buffer to GCS")
        val projects = mutableMapOf<String, ProjectBuffer>()
        for ((projectId, project) in this.fileBuffer.entries) {
            synchronized(project) {
                projects[projectId] = ProjectBuffer.newBuilder()
                        .putAllProject(project)
                        .build()
            }
        }
        val buffer = Buffer.newBuilder()
                .putAllBuffer(projects)
                .build()
        try {
            this.storage.create(
                    BlobInfo.newBuilder(BlobId.of(bucketName, BUFFER_NAME_GCS)).build(),
                    buffer.toByteArray())
        } catch (e: StorageException) {
            LOG.error("StorageException happened at Cosmas", e)
        }
    }

    private fun loadBufferFromGCS(): ConcurrentHashMap<String, ConcurrentMap<String, CosmasProto.FileVersion>> {
        val buffer = ConcurrentHashMap<String, ConcurrentMap<String, CosmasProto.FileVersion>>()
        val bufferBytes: Blob = try {
            this.storage.get(BlobId.of(bucketName, BUFFER_NAME_GCS))
        } catch (e: StorageException) {
            LOG.error("StorageException happened while loading buffer", e)
            return buffer
        } ?: return buffer
        LOG.info("Loading buffer from GCS")
        val bufferGRPC = Buffer.parseFrom(bufferBytes.getContent())
        for ((projectId, projectBuffer) in bufferGRPC.bufferMap.entries) {
            buffer[projectId] = ConcurrentHashMap(projectBuffer.projectMap)
        }
        return buffer
    }

    private fun getDiffPatch(oldText: String, newText: String, timestamp: Long): CosmasProto.Patch {
        val diffPatch = diff_match_patch().patch_toText(diff_match_patch().patch_make(oldText, newText))
        return CosmasProto.Patch.newBuilder()
                .setText(diffPatch)
                .setUserId(COSMAS_ID)
                .setUserName(COSMAS_NAME)
                .setTimestamp(timestamp)
                .setActualHash(md5Hash(newText))
                .build()
    }


    private fun handleStorageException(e: StorageException, responseObserver: StreamObserver<*>) {
        LOG.error("StorageException happened at Cosmas", e)
        responseObserver.onError(e)
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
