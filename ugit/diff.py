import subprocess

from collections import defaultdict
from tempfile import NamedTemporaryFile as Temp

from . import data

def compare_trees(*trees):
    """
    take a list of trees and will return them
    grouped by filename
    """
    entries = defaultdict(lambda: [None] * len(trees))
    for i, tree in enumerate(trees):
        for path, oid in tree.items():
            entries[path][i] = oid
    for path, oids in entries.items():
        yield(path, *oids)

def iter_changed_files(t_from, t_to):
    """
    take two trees and output all changed paths along with
    the change type (deleted, created, modified)
    """
    for path, o_from, o_to in compare_trees(t_from, t_to):
      if o_from != o_to:
          action =('new file' if not o_from else
                  'deleted' if not o_to else
                  'modified')
          yield path, action

def diff_trees(t_from, t_to):
    """
    takes two trees and compares them
    """
    output = b''
    for path, o_from, o_to in compare_trees(t_from, t_to):
        if o_from != o_to:
            output += diff_blobs(o_from, o_to, path)
    return output

def diff_blobs(o_from, o_to, path='blob'):
    """
    take two blo OID and compare
    """
    with Temp() as f_from, Temp() as f_to:
        for oid, f in ((o_from, f_from), (o_to, f_to)):
            if oid:
                f.write(data.get_object(oid))
                f.flush()
        with subprocess.Popen(
            ['diff', '--unified', '--show-c-function',
             '--label', f'a/{path}', f_from.name,
             '--label', f'b/{path}', f_to.name],
            stdout=subprocess.PIPE) as proc:
            output, _ = proc.communicate ()
        return output

def merge_trees(t_base, t_HEAD, t_other):
    """
    get two trees and in turn call `merge_blobs` to
    merge each two files in the trees, output one
    merged tree
    """
    tree = {}
    for path, o_base, o_HEAD, o_other in compare_trees(t_base ,t_HEAD, t_other):
        tree[path] = merge_blobs(o_base, o_HEAD, o_other)
    return tree

def merge_blobs(o_base, o_HEAD, o_other):
    """
    get two OIDs and return their merged content
    """
    with Temp() as f_base, Temp() as f_HEAD, Temp() as f_other:
        for oid, f in ((o_base, f_base), (o_HEAD, f_HEAD), (o_other, f_other)):
            if oid:
                f.write(data.get_object(oid))
                f.flush()
        with subprocess.Popen (
            ['diff3', '-m',
             '-L', 'HEAD', f_HEAD.name,
             '-L', 'BASE', f_base.name,
             '-L', 'MERGE_HEAD', f_other.name,
            ], stdout=subprocess.PIPE) as proc:
            output, _ = proc.communicate ()

        return output