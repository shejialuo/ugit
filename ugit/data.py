import hashlib
import os

GIT_DIR='.ugit'

def init():
    """
    create empty ugit repository
    """
    os.makedirs(GIT_DIR)
    os.makedirs(f'{GIT_DIR}/objects')

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