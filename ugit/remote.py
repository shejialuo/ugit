import os

from . import data
from . import base

REMOTE_REFS_BASE = 'refs/heads/'
LOCAL_REFS_BASE = 'refs/remote/'

def fetch(remote_path):
    """
    use `_get_remote_refs` to fetch
    """
    refs = _get_remote_refs(remote_path, REMOTE_REFS_BASE)

    for oid in base.iter_objects_in_commits(refs.values()):
        data.fetch_object_if_missing(oid, remote_path)

    for remote_name, value in refs.items():
        refname = os.path.relpath(remote_name, REMOTE_REFS_BASE)
        data.update_ref(f'{LOCAL_REFS_BASE}/{refname}',
                        data.RefValue (symbolic=False, value=value))

def push(remote_path, refname):
    """
    get local reference, and push all objects to the remote
    """
    remote_refs = _get_remote_refs(remote_path)
    local_ref = data.get_ref(refname).value

    objects_to_push = base.iter_objects_in_commits({local_ref})

    known_remote_refs = filter(data.object_exists, remote_refs.values())
    remote_objects = set(base.iter_objects_in_commits(known_remote_refs))
    local_objects = set(base.iter_objects_in_commits({local_ref}))
    objects_to_push = local_objects - remote_objects

    for oid in objects_to_push:
        data.push_object(oid, remote_path)

    with data.change_git_dir(remote_path):
        data.update_ref(refname,
                        data.RefValue(symbolic=False, value=local_ref))

def _get_remote_refs(remote_path, prefix= ''):
    """
    get remote repository's reference
    """
    with data.change_git_dir(remote_path):
        return {refname: ref.value for refname, ref in data.iter_refs(prefix)}