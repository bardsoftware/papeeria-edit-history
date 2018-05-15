package com.bardsoftware.papeeria.backend.cosmas
import org.junit.Assert.*
import org.junit.Test
import java.util.*
import diff_match_patch

class PatchCorrectorTest {
    @Test
    fun plusReversePatchTest() {
        val text1 = "Hello world!"
        val text2 = "Hello, world!"
        val dmp = diff_match_patch()
        val patch = dmp.patch_make(text1, text2)
        val patchCorrector = PatchCorrector()
        val listOfPatch = LinkedList<diff_match_patch.Patch>()
        listOfPatch.addAll(patchCorrector.reversePatch(patch))
        val newText = dmp.patch_apply(listOfPatch, text2)
        assertEquals(text1, newText[0])
    }

    @Test
    fun minusReversePatchTest() {
        val text2 = "Hello world!"
        val text1 = "Hello, world!"
        val dmp = diff_match_patch()
        val patch = dmp.patch_make(text1, text2)
        val patchCorrector = PatchCorrector()
        val listOfPatch = LinkedList<diff_match_patch.Patch>()
        listOfPatch.addAll(patchCorrector.reversePatch(patch))
        val newText = dmp.patch_apply(listOfPatch, text2)
        assertEquals(text1, newText[0])
    }

    @Test
    fun deletePatchWithAdd() {
        val text1 = "Hello world"
        val text2 = "Hello world!"
        val text3 = "Hello, world!"
        val dmp = diff_match_patch()
        val patchForDelete  = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        val patchCorrector = PatchCorrector()
        val deletePatch = patchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text3)
        assertEquals("Hello, world", res[0])
    }

    @Test
    fun deletePatchWithDelete() {
        val text1 = "Many many many words"
        val text2 = "Many many words"
        val text3 = "Many ! many words"
        val dmp = diff_match_patch()
        val patchForDelete  = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        val patchCorrector = PatchCorrector()
        val deletePatch = patchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text3)
        assertEquals("Many ! many many words", res[0])
    }

    @Test
    fun deletePatchFromLongList() {
        val text1 = "Hello"
        val text2 = "Hello world"
        val text3 = "Hello beautiful world"
        val text4 = "Hello, beautiful world life"
        val text5 = "Hello, beautiful world life! Bye!"
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        patch.addAll(dmp.patch_make(text4, text5))
        val patchCorrector = PatchCorrector()
        val deletePatch = patchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text5)
        assertEquals("Hello, beautiful life! Bye!", res[0])
    }

    @Test
    fun bigFileChangeWorld() {
        val text1 = "Alice was beginning to get very tired of sitting by her sister on the bank, and of having nothing " +
                "to do: once or twice she had peeped into the book her sister was reading, but it had no pictures or " +
                "conversations in it, ‘and what is the use of a book,’ thought Alice ‘without pictures or conversation?’"
        val text2 = "Ann was beginning to get very tired of sitting by her sister on the bank, and of having nothing " +
                "to do: once or twice she had peeped into the book her sister was reading, but it had no pictures or " +
                "conversations in it, ‘and what is the use of a book,’ thought Ann ‘without pictures or conversation?’"
        val text3 =  "Ann was beginning to get very tired : once or twice she had peeped into the book her sister was reading," +
                " but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann ‘without" +
                " pictures or conversation?’"
        val text4 = "Ann was very tired : once or twice she had peeped into the book her sister was reading," +
                " but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann ‘without" +
                " pictures or conversation?’"
        val text5 = "Ann was very tired and upset: once or twice she had peeped into the book her sister was reading," +
                " but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann ‘without" +
                " pictures or conversation?’"
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        patch.addAll(dmp.patch_make(text4, text5))
        val patchCorrector = PatchCorrector()
        val deletePatch = patchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text5)
        assertEquals("Alice was very tired and upset: once or twice she had peeped into the book her sister was reading," +
                " but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Alice ‘without" +
                " pictures or conversation?’", res[0])
    }
}