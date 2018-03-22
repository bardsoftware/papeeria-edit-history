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

import com.google.protobuf.ByteString
import org.junit.Assert.*
import org.junit.Test
import io.grpc.internal.testing.StreamRecorder
import org.junit.Before

/**
 * This is simple test to check that CosmasImpl class returns correct response.
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasServiceTest {
    private var service = CosmasService()
    @Before
    fun testInitialization() {
       service = CosmasService()
    }
    @Test
    fun addOneVersion() {
        addFileToService("Here comes the sun")
        val file = getFileFromService(0)
        assertTrue(file.isValidUtf8)
        assertEquals("Here comes the sun", file.toStringUtf8())
    }

    @Test
    fun addSecondVersion() {
        addFileToService("Here comes the sun")
        addFileToService("Little darling, it's been a long cold lonely winter")
        val file0 = getFileFromService(0)
        val file1 = getFileFromService(1)
        assertTrue(file0.isValidUtf8 && file1.isValidUtf8)
        assertEquals("Here comes the sun", file0.toStringUtf8())
        assertEquals("Little darling, it's been a long cold lonely winter", file1.toStringUtf8())
    }

    private fun getFileFromService(version: Int, fileId: String = "43", projectId: String = "0"): ByteString {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest: CosmasProto.GetVersionRequest =
                CosmasProto.GetVersionRequest.newBuilder().
                        setVersion(version).
                        setFileId(fileId).
                        setProjectId(projectId).
                        build()
        service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder.values[0].file
    }

    private fun addFileToService(text: String, fileId: String = "43", projectId: String = "0") {
        val createVersionRecorder: StreamRecorder<CosmasProto.CreateVersionResponse> = StreamRecorder.create()
        val newVersionRequest =
                CosmasProto.CreateVersionRequest.newBuilder().
                        setFileId(fileId).
                        setProjectId(projectId).
                        setFile(ByteString.copyFromUtf8(text)).
                        build()
        service.createVersion(newVersionRequest, createVersionRecorder)
    }
}