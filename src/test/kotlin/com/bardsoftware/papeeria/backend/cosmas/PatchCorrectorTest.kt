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
import org.junit.Assert.*
import org.junit.Test
import java.util.*
import name.fraser.neil.plaintext.diff_match_patch

class PatchCorrectorTest {
    @Test
    fun plusReversePatchTest() {
        val text1 = "Hello world!"
        val text2 = "Hello, world!"
        val dmp = diff_match_patch()
        val patch = dmp.patch_make(text1, text2)
        val listOfPatch = LinkedList<diff_match_patch.Patch>()
        listOfPatch.addAll(PatchCorrector.reversePatch(patch))
        val newText = dmp.patch_apply(listOfPatch, text2)
        assertEquals(text1, newText[0])
    }

    @Test
    fun minusReversePatchTest() {
        val text2 = "Hello world!"
        val text1 = "Hello, world!"
        val dmp = diff_match_patch()
        val patch = dmp.patch_make(text1, text2)
        val listOfPatch = LinkedList<diff_match_patch.Patch>()
        listOfPatch.addAll(PatchCorrector.reversePatch(patch))
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
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
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
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
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
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text5)
        assertEquals("Hello, beautiful life! Bye!", res[0])
    }

    @Test
    fun bigFileChangeWorld() {
        val text1 = """Alice was beginning to get very tired of sitting by her sister on the bank, and of having nothing
            | to do: once or twice she had peeped into the book her sister was reading, but it had no pictures or conversations
            | in it, ‘and what is the use of a book,’ thought Alice ‘without pictures or conversation?’""".trimMargin().replace("\n","")
        val text2 = """Ann was beginning to get very tired of sitting by her sister on the bank, and of having nothing to
            | do: once or twice she had peeped into the book her sister was reading, but it had no pictures or conversations
            | in it, ‘and what is the use of a book,’ thought Ann ‘without pictures or conversation?’""".trimMargin().replace("\n","")
        val text3 =  """Ann was beginning to get very tired : once or twice she had peeped into the book her sister was
            | reading, but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann
            | ‘without pictures or conversation?’""".trimMargin().replace("\n","")
        val text4 = """Ann was very tired : once or twice she had peeped into the book her sister was reading, but it
            | had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann ‘without pictures or
            | conversation?’""".trimMargin().replace("\n","")
        val text5 = """Ann was very tired and upset: once or twice she had peeped into the book her sister was reading,
            | but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Ann ‘without
            | pictures or conversation?’""".trimMargin().replace("\n","")
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        patch.addAll(dmp.patch_make(text4, text5))
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text5)
        assertEquals("""Alice was very tired and upset: once or twice she had peeped into the book her sister was
             | reading, but it had no pictures or conversations in it, ‘and what is the use of a book,’ thought Alice ‘without
             | pictures or conversation?’""".trimMargin().replace("\n",""), res[0])
    }

    @Test
    fun multiLinesTest() {
        val text1 = """Hey Jude, don't make it bad
            |Take a sad song and make it better""".trimMargin()
        val text2 =  """Hey Jude, don't make it bad
            |Take a sad song and make it better
            |Remember to let her into your heart
            |Then you can start to make it better""".trimMargin()
        val text3 = """Hey Jude, don't make it bad
            |Take a sad song and make it better
            |Remember to let her into your heart
            |Then you can start to make it better
            |Hey Jude, don't be afraid
            |You were made to go out and get her""".trimMargin()
        val text4 = """Hey Jude, don't make it bad
            |Take a sad song and make it better
            |Remember to let her into your heart
            |Then you can start to make it better
            |Hey Jude, don't be afraid
            |You were made to go out and get her
            |The minute you let her under your skin
            |Then you begin to make it better""".trimMargin()
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text4)
        assertEquals("""Hey Jude, don't make it bad
            |Take a sad song and make it better
            |Hey Jude, don't be afraid
            |You were made to go out and get her
            |The minute you let her under your skin
            |Then you begin to make it better""".trimMargin(), res[0])
    }

    @Test
    fun multiLineReplaceString() {
        val text1 = """Imagine there's no heaven
            |It's easy if you try
            |No hell below us
            |Above us only sky
            |Imagine all the people living for today""".trimMargin()
        val text2 = """Imagine there's no heaven
            |It's easy if you try
            |No hell below us
            |Above us only sky
            |Imagine all the people living life in peace""".trimMargin()
        val text3 = """Imagine there's no countries
            |It isn't hard to do
            |No hell below us
            |Above us only sky
            |Imagine all the people living life in peace""".trimMargin()
        val text4 = """Imagine there's no countries
            |It isn't hard to do
            |Nothing to kill or die for
            |And no religion too
            |Imagine all the people living life in peace""".trimMargin()
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text4)
        assertEquals("""Imagine there's no countries
            |It isn't hard to do
            |Nothing to kill or die for
            |And no religion too
            |Imagine all the people living for today""".trimMargin(), res[0])
    }

    @Test
    fun multiLinesReplaceWorld() {
        val text1 = """All you need is love, all you need is love
            |All you need is love, love, love is all you need""".trimMargin()
        val text2 = """All you need is sleep, all you need is sleep
            |All you need is sleep, sleep, sleep is all you need""".trimMargin()
        val text3 = """All you need is sleep, all you need is sleep
            |All you need is sleep, sleep, sleep is all you need
            |There's nothing you can know that isn't known
            |Nothing you can see that isn't shown""".trimMargin()
        val text4 = """All you need is sleep, all you need is sleep
            |All you need is sleep, sleep, sleep is all you need
            |There's nothing you can know that isn't known
            |Nothing you can see that isn't shown
            |There's nowhere you can be that isn't where you're meant to be
            |It's easy""".trimMargin()
        val dmp = diff_match_patch()
        val patchForDelete = dmp.patch_make(text1, text2)
        val patch = dmp.patch_make(text2, text3)
        patch.addAll(dmp.patch_make(text3, text4))
        val deletePatch = PatchCorrector.deletePatch(patchForDelete, patch, text2)
        val res = dmp.patch_apply(deletePatch, text4)
        assertEquals("""All you need is love, all you need is love
            |All you need is love, love, love is all you need
            |There's nothing you can know that isn't known
            |Nothing you can see that isn't shown
            |There's nowhere you can be that isn't where you're meant to be
            |It's easy""".trimMargin(), res[0])
    }
}