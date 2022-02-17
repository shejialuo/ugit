import itertools
import operator
import os
import string

from collections import deque, namedtuple

from . import data

def init():
    data.init()
    data.update_ref('HEAD', data.RefValue(symbolic=True, value='refs/heads/master'))

def write_tree(directory='.'):
    """
    create tree from current index
    """
    entries = []
    with os.scandir(directory) as it:
        for entry in it:
            full = f'{directory}/{entry.name}'
            if is_ignored(full):
                continue
            if entry.is_file(follow_symlinks=False):
                type_ = 'blob'
                with open(full, 'rb') as f:
                    oid = data.hash_object(f.read())
            elif entry.is_dir(follow_symlinks=False):
                type_ = 'tree'
                oid = write_tree(full)
            entries.append((entry.name, oid, type_))

    tree = ''.join(f'{type_} {oid} {name}\n' for name, oid, type_
                   in sorted(entries))
    return data.hash_object(tree.encode(), 'tree')

def _iter_tree_entries(oid):
    """
    a generator that will take an OID of a tree,
    tokenize it line-by-line and yield the raw string values
    """
    if not oid:
        return
    tree = data.get_object(oid, 'tree')
    for entry in tree.decode().splitlines():
        type_, oid, name = entry.split (' ', 2)
        yield type_, oid, name

def get_tree(oid, base_path=''):
    """
    use `_iter_tree_entries` to recursively parse a
    tree into a dict.
    """
    result = {}
    for type_, oid , name in _iter_tree_entries(oid):
        if '/' in name:
            raise ValueError(f'{name} should not contain /')
        if name in ('..', '.'):
            raise ValueError(f'{name} should not contain .. or .')
        path = base_path + name
        if type_ == 'blob':
            result[path] = oid
        elif type_ == 'tree':
            result.update(get_tree(oid, f'{path}/'))
        else:
            raise TypeError(f'Unknown tree entry {type_}')
    return result

def _empty_current_directory():
    """
    Delete all existing stuff before reading
    """
    for root, dirnames, filenames in os.walk('.', topdown=False):
        for filename in filenames:
            path = os.path.relpath(f'{root}/{filename}')
            if is_ignored(path) or not os.path.isfile(path):
                continue
            os.remove(path)
        for dirname in dirnames:
            path = os.path.relpath(f'{root}/{dirname}')
            if is_ignored(path):
                continue
            try:
                os.rmdir(path)
            except(FileNotFoundError, OSError):
                pass

def read_tree(tree_oid):
    """
    use `get_tree` to get the file OIDs and writes them into
    the working directory
    """
    _empty_current_directory()
    for path, oid in get_tree(tree_oid, base_path='./').items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open (path, 'wb') as f:
            f.write (data.get_object(oid))

def commit(message):
    """
    commit. A commit will just be a text file stored in
    the object database with the type of "commit"
    """
    commit = f'tree {write_tree()}\n'
    HEAD = data.get_ref('HEAD').value
    if HEAD:
        commit += f'parent {HEAD}\n'
    commit += '\n'
    commit += f'{message}\n'
    oid = data.hash_object(commit.encode(), 'commit')
    data.update_ref('HEAD', data.RefValue(symbolic=False, value=oid))
    return oid

def checkout(name):
    """
    checkout branch or paths to working directory
    """
    oid = get_oid(name)
    commit = get_commit(oid)
    read_tree(commit.tree)
    if is_branch(name):
        HEAD = data.RefValue(symbolic=True, value=f'refs/heads/{name}')
    else:
        HEAD = data.RefValue(symbolic=False, value=oid)
    
    data.update_ref('HEAD', HEAD, deref=False)

def reset(oid):
    data.update_ref('HEAD', data.RefValue(symbolic=False, value=oid))

def create_tag(name, oid):
    """
    create the tag in refs/tags/
    """
    data.update_ref(f'refs/tags/{name}', data.RefValue(symbolic=False, value=oid))

def create_branch(name, oid):
    """
    create the branch in refs/heads/
    """
    data.update_ref(f'refs/heads/{name}', data.RefValue(symbolic=False, value=oid))

def iter_branch_names():
    """
    iterate all the branch
    """
    for refname, _ in data.iter_refs('refs/heads/'):
        yield os.path.relpath(refname, 'refs/heads/')

def is_branch(branch):
    """
    a helper function to test whether it is a branch
    """
    return data.get_ref(f'refs/heads/{branch}').value is not None

def get_branch_name():
    """
    get branch name
    """
    HEAD = data.get_ref('HEAD', deref=False)
    if not HEAD.symbolic:
        return None
    HEAD = HEAD.value
    if not HEAD.startswith('refs/heads/'):
        raise ValueError(f'{HEAD} not start with refs/heads/')
    return os.path.relpath(HEAD, 'refs/heads')

Commit = namedtuple('Commit', ['tree', 'parent', 'message'])

def get_commit(oid):
    """
    Traverse commit object to achieve `ugit log`
    """
    parent = None
    commit = data.get_object(oid, 'commit').decode()
    lines = iter(commit.splitlines())
    for line in itertools.takewhile(operator.truth, lines):
        key, value = line.split(' ', 1)
        if key == 'tree':
            tree = value
        elif key == 'parent':
            parent = value
        else:
            raise TypeError(f'Unknown field {key}')
    message = '\n'.join(lines)
    return Commit(tree=tree, parent=parent, message=message)

def iter_commits_and_parents(oids):
    """
    a generator that returns all commits that it can reach
    from a given set of OIDs
    """
    oids = deque(oids)
    visited = set()

    while oids:
        oid = oids.popleft()
        if not oid or oid in visited:
            continue
        visited.add(oid)
        yield oid

        commit = get_commit(oid)
        oids.appendleft(commit.parent)

def get_oid(name):
    """
    get OID from reference or just its value
    """
    if name == '@': name = 'HEAD'
    refs_to_try = [
      f'{name}',
      f'refs/{name}',
      f'refs/tags/{name}',
      f'refs/heads/{name}',
    ]
    for ref in refs_to_try:
      if data.get_ref(ref, deref=False).value:
          return data.get_ref(ref).value
    is_hex = all(c in string.hexdigits for c in name)
    if len(name) == 40 and is_hex:
        return name
    else:
        raise ValueError(f'Unknown name {name}')

def is_ignored(path):
    return '.ugit' in path.split('/')
