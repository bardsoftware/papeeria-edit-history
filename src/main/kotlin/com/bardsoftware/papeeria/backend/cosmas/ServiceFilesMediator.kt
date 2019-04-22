package com.bardsoftware.papeeria.backend.cosmas

import com.google.cloud.storage.Storage
import com.google.protobuf.MessageLite
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class ServiceFilesMediator(private val storage: Storage) {
    private val locks = ConcurrentHashMap<String, ReadWriteLock>()
    fun <T : MessageLite> withReadFile(info: CosmasProto.ProjectInfo, name: String, run: (T) -> Unit) {
        val lock = locks.getOrPut(name) { ReentrantReadWriteLock() }.readLock()
        lock.lock()
        this.storage.get(getBlobId(cemeteryName, request.info))
        run(getFileFromGCS(info, name))
        lock.unlock()
    }

    private fun <T : MessageLite> getFileFromGCS(info: CosmasProto.ProjectInfo,
                                                 name: String): T {
    }

    private fun <T : MessageLite> saveFileToGCS(info: CosmasProto.ProjectInfo,
                                                name: String, file: T) {

    }
}