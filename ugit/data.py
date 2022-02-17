import hashlib
import os

from collections import namedtuple

GIT_DIR='.ugit'

def init():
    """
    create empty ugit repository
    """
    os.makedirs(GIT_DIR)
    os.makedirs(f'{GIT_DIR}/objects')

RefValue = namedtuple('RefValue', ['symbolic', 'value'])

def update_ref(ref, value, deref=True):
    """
    update reference file OID
    """
    ref = _get_ref_internal(ref, deref)[0]
    if value.symbolic:
        value = f'ref: {value.value}'
    else:
        value = value.value
    ref_path = f'{GIT_DIR}/{ref}'
    os.makedirs(os.path.dirname(ref_path), exist_ok=True)
    with open(ref_path, 'w') as f:
        f.write(value)

def get_ref(ref, deref=True):
    """
    get reference file OID
    """
    return _get_ref_internal(ref, deref)[1]

def _get_ref_internal(ref, deref):
    """
    a helper function which returns the path and the value
    of the last ref pointed by a symbolic ref
    """
    ref_path = f'{GIT_DIR}/{ref}'
    value = None
    if os.path.isfile(ref_path):
        with open(ref_path) as f:
            value = f.read().strip()
    symbolic = bool (value) and value.startswith ('ref:')
    if symbolic:
        value = value.split(':', 1)[1].strip()
        if deref:
            return _get_ref_internal(value, deref=True)
    return ref, RefValue(symbolic=symbolic, value=value)


def iter_refs(prefix='', deref=True):
    """
    a generator which will iterate on all available refs
    from the ugit directory and everything under .ugit/refs
    """
    refs = ['HEAD']
    for root, _ , filenames in os.walk(f'{GIT_DIR}/refs'):
        root = os.path.relpath(root, GIT_DIR)
        refs.extend(f'{root}/{name}' for name in filenames)

    for refname in refs:
        if not refname.startswith(prefix):
            continue
        yield refname, get_ref(refname, deref=deref)

def hash_object(data, type_ = 'blob'):
    '''
    compute object ID and optionally create a blob from file
    '''
    obj = type_.encode () + b'\x00' + data
    oid = hashlib.sha1 (obj).hexdigest ()
    with open(f'{GIT_DIR}/objects/{oid}', 'wb') as out:
        out.write(obj)
    return oid

def get_object (oid, expected = None):
    """
    provide content or type information for repository objects
    """
    with open (f'{GIT_DIR}/objects/{oid}', 'rb') as f:
        obj = f.read()
    type_, _, content = obj.partition(b'\x00')
    type_ = type_.decode()

    if expected is not None and type_ != expected:
        raise ValueError(f'Expected {expected}, got {type_}')
    return content