package com.bardsoftware.papeeria.backend.cosmas
import java.util.LinkedList
import diff_match_patch

class PatchCorrector {
    private fun getReverseOperation(operation: diff_match_patch.Operation): diff_match_patch.Operation {
        if (operation == diff_match_patch.Operation.DELETE)
            return diff_match_patch.Operation.INSERT
        if (operation == diff_match_patch.Operation.INSERT)
            return diff_match_patch.Operation.DELETE
        return operation
    }

    fun reversePatch(patchList: LinkedList<diff_match_patch.Patch>): LinkedList<diff_match_patch.Patch> {
        val reversePatchList = LinkedList<diff_match_patch.Patch>()
        for (patch in patchList) {
            val newPatch = diff_match_patch.Patch()
            newPatch.length1 = patch.length2
            newPatch.length2 = patch.length1
            newPatch.start1 = patch.start2
            newPatch.start2 = patch.start1
            for (diffs in patch.diffs) {
                newPatch.diffs.add(diff_match_patch.Diff(getReverseOperation(diffs.operation), diffs.text))
            }
            reversePatchList.add(newPatch)
        }
        return reversePatchList
    }

    fun deletePatch(patchForDelete: LinkedList<diff_match_patch.Patch>,
                    patchList: List<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        val dmp = diff_match_patch()
        var currentVersionOfText = text
        val listOfPatches = LinkedList<diff_match_patch.Patch>()
        var listOfReversePatches = LinkedList<diff_match_patch.Patch>()
        listOfReversePatches.addAll(reversePatch(patchForDelete))
        for (patch in patchList) {
            listOfPatches.clear()
            listOfPatches.add(patch)
            val nextVersion = dmp.patch_apply(listOfPatches, currentVersionOfText)
            val nextVersionWithoutPatch = dmp.patch_apply(listOfReversePatches, nextVersion[0] as String)
            var successful = true
            for (result in nextVersion[1] as BooleanArray) {
                successful = successful && result
            }
            for (result in nextVersionWithoutPatch[1] as BooleanArray) {
                successful = successful && result
            }
            if (!successful) {
                throw DeletePatchException("Failure in patch apply")
            }
            listOfReversePatches = dmp.patch_make(nextVersion[0] as String, nextVersionWithoutPatch[0] as String)
            currentVersionOfText = nextVersion[0] as String
        }
        return listOfReversePatches
    }

    public class DeletePatchException(message: String) : Throwable(message)
}