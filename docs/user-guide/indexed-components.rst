.. _indexed-components:

#############################
Indexed components (salIndex)
#############################

`RFC-849`_ introduces a new indexing scheme for SAL verision 7, which changes the field name for indexed components to ``salIndex``.

SAL version 7 was deployed at the Summit on 2022-07-06, and the EFD client was updated to accommodate the new indexing scheme.

A boolean flag ``use_old_csc_indexing`` is available on query methods of the EFD client if you need to query data older that.

