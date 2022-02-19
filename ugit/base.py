import itertools
import operator
import os
from pickle import FALSE
import string

from collections import deque, namedtuple

from . import data
from . import diff

def init():
    data.init()
    data.update_ref('HEAD', data.RefValue(symbolic=True, value='refs/heads/master'))

def write_tree():
    """
    create tree from current index
    """
    index_as_tree = {}
    with data.get_index() as index:
        for path, oid in index.items():
            path = path.split ('/')
            dirpath, filename = path[:-1], path[-1]

            current = index_as_tree

            for dirname in dirpath:
                current = current.setdefault(dirname, {})
            current[filename] = oid

    def write_tree_recursive(tree_dict):
        entries = []
        for name, value in tree_dict.items():
            if type(value) is dict:
                type_ = 'tree'
                oid = write_tree_recursive(value)
            else:
                type_ = 'blob'
                oid = value
            entries.append((name, oid, type_))

        tree = ''.join(f'{type_} {oid} {name}\n'
                       for name, oid, type_
                       in sorted (entries))
        return data.hash_object(tree.encode(), 'tree')

    return write_tree_recursive(index_as_tree)

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

def get_working_tree():
    """
    walk over all files in the working directory, put them
    in the object database and create a dict that holds all
    the OIDs
    """
    result = {}
    for root, _, filenames in os.walk('.'):
        for filename in filenames:
            path = os.path.relpath(f'{root}/{filename}')
            if is_ignored(path) or not os.path.isfile(path):
                continue
            with open(path, 'rb') as f:
                result[path] = data.hash_object(f.read())
    return result

def get_index_tree():
    with data.get_index() as index:
        return index

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

def read_tree(tree_oid, update_working=False):
    """
    use `get_tree` to get the file OIDs and writes them into
    the working directory
    """
    with data.get_index() as index:
        index.clear()
        index.update(get_tree(tree_oid))

        if update_working:
            _checkout_index(index)

def read_tree_merged(t_base, t_HEAD, t_other, update_working=False):
    """
    take two trees and extract a merged version
    of them into the working directory
    """
    with data.get_index() as index:
        index.clear()
        index.update(diff.merge_trees(
            get_tree(t_base),
            get_tree(t_HEAD),
            get_tree(t_other)
        ))

        if update_working:
            _checkout_index(index)

def _checkout_index(index):
    _empty_current_directory()
    for path, oid in index.items():
        os.makedirs(os.path.dirname(f'./{path}'), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data.get_object(oid, 'blob'))

def commit(message):
    """
    commit. A commit will just be a text file stored in
    the object database with the type of "commit"
    """
    commit = f'tree {write_tree()}\n'
    HEAD = data.get_ref('HEAD').value
    if HEAD:
        commit += f'parent {HEAD}\n'
    MERGE_HEAD = data.get_ref('MERGE_HEAD').value
    if MERGE_HEAD:
        commit += f'parent {MERGE_HEAD}\n'
        data.delete_ref('MERGE_HEAD', deref=FALSE)
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

def merge(other):
    HEAD = data.get_ref('HEAD').value
    merge_base = get_merge_base(other, HEAD)
    c_other = get_commit(other)

    if merge_base == HEAD:
        read_tree(c_other.tree)
        data.update_ref('HEAD',
                        data.RefValue(symbolic=False, value=other))
        print('Fast-forward merge, no need to commit')
        return

    data.update_ref('MERGE_HEAD', data.RefValue(symbolic=False, value=other))

    c_base = get_commit(merge_base)
    c_HEAD = get_commit(HEAD)
    read_tree_merged(c_base.tree ,c_HEAD.tree, c_other.tree)
    print('Merged in working tree\nPlease commit')

def get_merge_base(oid1, oid2):
    """
    receive two commit OIDs and find their common ancestor
    """
    parents1 = set(iter_commits_and_parents({oid1}))

    for oid in iter_commits_and_parents({oid2}):
        if oid in parents1:
            return oid

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

Commit = namedtuple('Commit', ['tree', 'parents', 'message'])

def get_commit(oid):
    """
    Traverse commit object to achieve `ugit log`
    """
    parents = []
    commit = data.get_object(oid, 'commit').decode()
    lines = iter(commit.splitlines())
    for line in itertools.takewhile(operator.truth, lines):
        key, value = line.split(' ', 1)
        if key == 'tree':
            tree = value
        elif key == 'parent':
            parents.append(value)
        else:
            raise TypeError(f'Unknown field {key}')
    message = '\n'.join(lines)
    return Commit(tree=tree, parents=parents, message=message)

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
        oids.extendleft(commit.parents[:1])
        oids.extend(commit.parents[1:])

def iter_objects_in_commits(oids):
    """
    take a list of commit OIDs and return all
    objects that are reachable from these commits
    """
    visited = set()
    def iter_objects_in_tree(oid):
        visited.add(oid)
        yield oid
        for type_, oid, _ in _iter_tree_entries(oid):
            if oid not in visited:
                if type_ == 'tree':
                    yield from iter_objects_in_tree(oid)
                else:
                    visited.add(oid)
                    yield oid
    for oid in iter_commits_and_parents(oids):
        yield oid
        commit = get_commit(oid)
        if commit.tree not in visited:
            yield from iter_objects_in_tree(commit.tree)

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

def add(filenames):
    def add_file(filename):
        filename = os.path.relpath(filename)
        with open(filename, 'rb') as f:
            oid = data.hash_object(f.read())
        index[filename] = oid
    
    def add_directory(dirname):
        for root, _, filenames in os.walk(dirname):
            for filename in filenames:
                path = os.path.relpath(f'{root}/{filename}')
                if is_ignored(path) or not os.path.isfile(path):
                    continue
                add_file(path)

    with data.get_index() as index:
        for name in filenames:
            if os.path.isfile(name):
                add_file(name)
            elif os.path.isdir(name):
                add_directory(name)

def is_ignored(path):
    return '.ugit' in path.split('/')
