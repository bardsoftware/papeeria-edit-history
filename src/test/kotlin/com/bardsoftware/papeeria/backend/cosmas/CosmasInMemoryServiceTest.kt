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
 * This is some tests for CosmasService class
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasInMemoryServiceTest {

    private lateinit var service: CosmasInMemoryService

    @Before
    fun testInitialization() {
        this.service = CosmasInMemoryService()
        println()
    }

    @Test
    fun addOneVersion() {
        addFileToService("Here comes the sun")
        val file = getFileFromService(0)
        assertFalse(file.isEmpty)
        assertTrue(file.isValidUtf8)
        assertEquals("Here comes the sun", file.toStringUtf8())
    }

    @Test
    fun addSecondVersion() {
        addFileToService("Here comes the sun")
        addFileToService("Little darling, it's been a long cold lonely winter")
        val file0 = getFileFromService(0)
        val file1 = getFileFromService(1)
        assertFalse(file0.isEmpty)
        assertFalse(file1.isEmpty)
        assertTrue(file0.isValidUtf8 && file1.isValidUtf8)
        assertEquals("Here comes the sun", file0.toStringUtf8())
        assertEquals("Little darling, it's been a long cold lonely winter", file1.toStringUtf8())
    }

    @Test
    fun addSecondFile() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        assertFalse(file1.isEmpty)
        assertFalse(file2.isEmpty)
        assertTrue(file1.isValidUtf8 && file2.isValidUtf8)
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
    }

    @Test
    fun tryToGetFileWithWrongId() {
        val getVersionRecorder = getStreamRecorderWithResult(0, "1")
        assertEquals(0, getVersionRecorder.values.size)
        assertNotNull(getVersionRecorder.error)
        assertEquals("INVALID_ARGUMENT: There is no file in storage with file id 1",
                getVersionRecorder.error!!.message)
    }

    @Test
    fun tryToGetFileWithWrongVersion() {
        addFileToService("file")
        val getVersionRecorder1 = getStreamRecorderWithResult(1, "0")
        val getVersionRecorder2 = getStreamRecorderWithResult(-1, "0")
        assertEquals(0, getVersionRecorder1.values.size)
        assertEquals(0, getVersionRecorder2.values.size)
        assertNotNull(getVersionRecorder1.error)
        assertNotNull(getVersionRecorder2.error)
    }

    @Test
    fun addManyFilesAndManyVersions() {
        addFileToService("file1", "1")
        addFileToService("file2", "2")
        addFileToService("file3", "3")
        addFileToService("file4", "4")
        addFileToService("file2ver1", "2")
        addFileToService("file4ver1", "4")
        addFileToService("file4ver2", "4")
        val file1 = getFileFromService(0, "1")
        val file2 = getFileFromService(0, "2")
        val file3 = getFileFromService(0, "3")
        val file4 = getFileFromService(0, "4")
        val file2_1 = getFileFromService(1, "2")
        val file4_1 = getFileFromService(1, "4")
        val file4_2 = getFileFromService(2, "4")
        assertEquals("file1", file1.toStringUtf8())
        assertEquals("file2", file2.toStringUtf8())
        assertEquals("file3", file3.toStringUtf8())
        assertEquals("file4", file4.toStringUtf8())
        assertEquals("file2ver1", file2_1.toStringUtf8())
        assertEquals("file4ver1", file4_1.toStringUtf8())
        assertEquals("file4ver2", file4_2.toStringUtf8())
    }

    private fun getFileFromService(version: Int, fileId: String = "0", projectId: String = "0"): ByteString {
        return getStreamRecorderWithResult(version, fileId, projectId).values[0].file
    }

    private fun getStreamRecorderWithResult(version: Int, fileId: String = "0", projectId: String = "0"):
            StreamRecorder<CosmasProto.GetVersionResponse> {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        val getVersionRequest = CosmasProto.GetVersionRequest
                .newBuilder()
                .setVersion(version)
                .setFileId(fileId)
                .setProjectId(projectId)
                .build()
        this.service.getVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder
    }

    private fun addFileToService(text: String, fileId: String = "0", projectId: String = "0") {
        val createVersionRecorder: StreamRecorder<CosmasProto.CreateVersionResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.CreateVersionRequest
                .newBuilder()
                .setFileId(fileId)
                .setProjectId(projectId)
                .setFile(ByteString.copyFromUtf8(text))
                .build()
        this.service.createVersion(newVersionRequest, createVersionRecorder)
    }

    private fun addFileWithHistoryToService(text: String, user: String = "No name", fileId: String = "0",
                                            time: Long = 0) {
        val createVersionRecorder: StreamRecorder<CosmasProto.CreateHistoryOfVersionResponse> = StreamRecorder.create()
        val newVersionRequest = CosmasProto.CreateHistoryOfVersionRequest
                .newBuilder()
                .setUser(user)
                .setFileId(fileId)
                .setPatch(text)
                .setTimeStamp(time)
                .build()
        this.service.createHistoryOfVersion(newVersionRequest, createVersionRecorder)
    }

    private fun getStreamRecorderWithResultHistory(version: Int, fileId: String = "0"):
            StreamRecorder<CosmasProto.GetHistoryOfVersionResponse> {
        val getVersionRecorder: StreamRecorder<CosmasProto.GetHistoryOfVersionResponse> = StreamRecorder.create()
        val getVersionRequest = CosmasProto.GetHistoryOfVersionRequest
                .newBuilder()
                .setVersion(version)
                .setFileId(fileId)
                .build()
        this.service.getHistoryOfVersion(getVersionRequest, getVersionRecorder)
        return getVersionRecorder
    }

    private fun checkCorrect(user : String, text : String, time : Long,
                             ans : CosmasProto.GetHistoryOfVersionResponse) : Boolean {
        return ans.user == user && ans.patch == text && ans.timeStamp == time
    }

    @Test
    fun addOneHistory() {
        addFileWithHistoryToService("Hey Jude, don't make it bad.", "The Beatles", "1", 1968)
        val ans = getStreamRecorderWithResultHistory(0, "1").values[0]
        assertEquals("The Beatles", ans.user)
        assertEquals("Hey Jude, don't make it bad.", ans.patch)
        assertEquals(1968, ans.timeStamp)
    }

    @Test
    fun addManyVersionsOfOneFile() {
        addFileWithHistoryToService("Hey Jude, don't make it bad", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Take a sad song and make it better", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Remember to let her into your heart", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Then you can start to make it better", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Hey Jude, don't be afraid", "The Beatles", "1", 1968)
        val ans0 = getStreamRecorderWithResultHistory(0, "1").values[0]
        val ans1 = getStreamRecorderWithResultHistory(1, "1").values[0]
        val ans2 = getStreamRecorderWithResultHistory(2, "1").values[0]
        val ans3 = getStreamRecorderWithResultHistory(3, "1").values[0]
        val ans4 = getStreamRecorderWithResultHistory(4, "1").values[0]
        assertTrue(checkCorrect("The Beatles", "Hey Jude, don't make it bad", 1968, ans0))
        assertTrue(checkCorrect("The Beatles", "Take a sad song and make it better", 1968, ans1))
        assertTrue(checkCorrect("The Beatles", "Remember to let her into your heart", 1968, ans2))
        assertTrue(checkCorrect("The Beatles", "Then you can start to make it better", 1968, ans3))
        assertTrue(checkCorrect("The Beatles", "Hey Jude, don't be afraid", 1968, ans4))
    }

    @Test
    fun addManyFilesAndManyVersionsHistory() {
        addFileWithHistoryToService("Hey Jude, don't make it bad", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Take a sad song and make it better", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Remember to let her into your heart", "The Beatles", "1", 1968)
        addFileWithHistoryToService("Then you can start to make it better", "The Beatles", "1", 1968)
        addFileWithHistoryToService("When I find myself in times of trouble", "The Beatles", "0", 1970)
        addFileWithHistoryToService("Mother Mary comes to me,", "The Beatles", "0", 1970)
        addFileWithHistoryToService("Speaking words of wisdom -", "The Beatles", "0", 1970)
        addFileWithHistoryToService("Let it be.", "The Beatles", "0", 1970)
        addFileWithHistoryToService("Help! I need somebody", "The Beatles", "3", 1965)
        addFileWithHistoryToService("Help! Not just anybody", "The Beatles", "3", 1965)
        assertTrue(checkCorrect("The Beatles", "Remember to let her into your heart", 1968,
                getStreamRecorderWithResultHistory(2, "1").values[0]))
        assertTrue(checkCorrect("The Beatles", "Let it be.", 1970,
                getStreamRecorderWithResultHistory(3, "0").values[0]))
        assertTrue(checkCorrect("The Beatles", "Help! I need somebody", 1965,
                getStreamRecorderWithResultHistory(0, "3").values[0]))
        assertFalse(checkCorrect("The Beatles", "Then you can start to make it better", 1968,
                getStreamRecorderWithResultHistory(3, "0").values[0]))
    }
}