package com.bardsoftware.papeeria.backend.cosmas

import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.cemeteryName
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.fileIdChangeMapName
import com.bardsoftware.papeeria.backend.cosmas.CosmasGoogleCloudService.Companion.fileIdGenerationNameMapName
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.protobuf.GeneratedMessageV3
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class ServiceFilesMediator(private val storage: Storage, private val service: CosmasGoogleCloudService) {

    private val locks = ConcurrentHashMap<String, ReadWriteLock>()

    fun withReadFileIdGenerationNameMap(info: ProjectInfo, run: (FileIdGenerationNameMap) -> Unit) {
        withReadFile(fileIdGenerationNameMapName(info), info, FileIdGenerationNameMap.getDefaultInstance(), run)
    }

    fun withWriteFileIdGenerationNameMap(info: ProjectInfo, run: (FileIdGenerationNameMap) -> FileIdGenerationNameMap) {
        withWriteFile(fileIdGenerationNameMapName(info), info, FileIdGenerationNameMap.getDefaultInstance(), run)
    }

    fun withReadCemetery(info: ProjectInfo, run: (FileCemetery) -> Unit) {
        withReadFile(cemeteryName(info), info, FileCemetery.getDefaultInstance(), run)
    }

    fun withWriteCemetery(info: ProjectInfo, run: (FileCemetery) -> FileCemetery) {
        withWriteFile(cemeteryName(info), info, FileCemetery.getDefaultInstance(), run)
    }

    fun withReadFileIdChangeMap(info: ProjectInfo, run: (FileIdChangeMap) -> Unit) {
        withReadFile(fileIdChangeMapName(info), info, FileIdChangeMap.getDefaultInstance(), run)
    }

    fun withWriteFileIdChangeMap(info: ProjectInfo, run: (FileIdChangeMap) -> FileIdChangeMap) {
        withWriteFile(fileIdChangeMapName(info), info, FileIdChangeMap.getDefaultInstance(), run)
    }

    fun <T : GeneratedMessageV3> withReadFile(name: String, info: ProjectInfo,
                                              default: T, run: (T) -> Unit) {
        val lock = locks.getOrPut(name) { ReentrantReadWriteLock() }.readLock()
        lock.lock()
        try {
            val file = getFileFromGCS(info, name, default)
            run(file)
        } finally {
            lock.unlock()
        }
    }

    fun <T : GeneratedMessageV3> withWriteFile(name: String, info: CosmasProto.ProjectInfo,
                                               default: T, run: (T) -> T) {
        val lock = locks.getOrPut(name) { ReentrantReadWriteLock() }.writeLock()
        lock.lock()
        try {
            val file = getFileFromGCS(info, name, default)
            val updatedFile = run(file)
            saveFileToGCS(info, name, updatedFile)
        } finally {
            lock.unlock()
        }
    }

    private fun <T : GeneratedMessageV3> getFileFromGCS(info: CosmasProto.ProjectInfo,
                                                        name: String, default: T): T {
        val blob: Blob? = this.storage.get(this.service.getBlobId(name, info))
        val content = blob?.getContent() ?: return default
        return default.parserForType.parseFrom(content) as T
    }

    private fun <T : GeneratedMessageV3> saveFileToGCS(info: CosmasProto.ProjectInfo,
                                                       name: String, file: T) {
        this.storage.create(this.service.getBlobInfo(name, info), file.toByteArray())
    }
}