"""Fail closed if the upstream source differs from the reviewed v3.0.0 file."""
import ast
import hashlib
from pathlib import Path
import sys
import textwrap


def apply(target):
    here = Path(__file__).parent
    original = target.read_bytes()
    expected = (here / 'upstream.sha256').read_text().strip()
    if hashlib.sha256(original).hexdigest() != expected:
        raise RuntimeError('Upstream nightscout.py checksum changed; review patch before building')
    source = original.decode()
    replacement = (here / 'queries.py').read_text()
    tree, new_tree = ast.parse(source), ast.parse(replacement)
    old_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'NightscoutApi')
    new_class = next(n for n in new_tree.body if isinstance(n, ast.ClassDef))
    old_nodes = {n.name: n for n in [*tree.body, *old_class.body] if isinstance(n, ast.FunctionDef)}
    changes = []
    for node in [new_tree.body[0], *new_class.body]:
        if node.name == '_last_uploaded':
            continue
        old = old_nodes[node.name]
        code = ast.get_source_segment(replacement, node)
        if node.name != 'time_range':
            # get_source_segment omits indentation only on the first line.
            code = textwrap.dedent('    ' + code)
            code = textwrap.indent(code, '\t')
        changes.append((old.lineno - 1, old.end_lineno, code + '\n'))
    helper = next(n for n in new_class.body if n.name == '_last_uploaded')
    code = textwrap.dedent('    ' + ast.get_source_segment(replacement, helper))
    line = old_nodes['last_uploaded_entry'].lineno - 1
    changes.append((line, line, textwrap.indent(code, '\t') + '\n\n'))
    lines = source.splitlines(keepends=True)
    for start, end, code in sorted(changes, reverse=True):
        lines[start:end] = [code]
    result = ''.join(lines)
    ast.parse(result)
    target.write_text(result)


if __name__ == '__main__':
    apply(Path(sys.argv[1]))
