/**
Copyright 2019 BarD Software s.r.o

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
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.protobuf.GeneratedMessageV3
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * This class helps CosmasGoogleCloudService to work with "service" files in GCS, e.g. cemetery of file id change map.
 * It works thread-safety in the single Cosmas instance.
 * If 2 Cosmas instances have a conflict with one file, one of them will fall and throw exception
 * after attempt to create new version of the file.
 */
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
            val (file, _) = getFileFromGCS(info, name, default)
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
            val (file, generation) = getFileFromGCS(info, name, default)
            val updatedFile = run(file)
            saveFileToGCS(info, name, updatedFile, generation)
        } finally {
            lock.unlock()
        }
    }

    private fun <T : GeneratedMessageV3> getFileFromGCS(info: CosmasProto.ProjectInfo,
                                                        name: String, default: T): Pair<T, Long> {
        val blob: Blob? = this.storage.get(this.service.getBlobId(name, info))
        // If file doesn't exists in GCS, it thinks that it has generation = 0
        // when it checks, that generation that you get is equal to current generation in storage
        val content = blob?.getContent() ?: return Pair(default, 0L)
        return Pair(default.parserForType.parseFrom(content) as T,
                blob.generation)
    }

    private fun <T : GeneratedMessageV3> saveFileToGCS(info: CosmasProto.ProjectInfo,
                                                       name: String, file: T,
                                                       // file generation before we loaded it to GCS
                                                       prevGeneration: Long) {
        this.storage.create(this.service.getBlobInfo(name, info, prevGeneration), file.toByteArray(),
                // If in GCS file generation changed, we can't replace it content
                Storage.BlobTargetOption.generationMatch())
    }

    companion object {
        fun cemeteryName(info: ProjectInfo) = "${info.projectId}-cemetery"

        fun fileIdChangeMapName(info: ProjectInfo) = "${info.projectId}-fileIdChangeMap"

        fun fileIdGenerationNameMapName(info: ProjectInfo) = "${info.projectId}-fileIdGenerationNameMap"
    }
}