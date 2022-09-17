import os
import json
from typing import List
from tree_sitter import Language, Parser, Node

lib = os.path.join(os.path.dirname(__file__), "my-languages.so")
RUST_LANGUAGE = Language(lib, 'rust')

parser = Parser()
parser.set_language(RUST_LANGUAGE)


def parse_ast(s: bytes):
    tree = parser.parse(s)
    return tree


def tokenize(s: bytes) -> list[bytes]:
    tree = parse_ast(s)
    tokens = []
    nodes_to_expand: List[Node] = [tree.root_node]
    while nodes_to_expand:
        node = nodes_to_expand.pop(0)
        if not node.children and node.text:
            tokens.append(node.text.decode())
        nodes_to_expand = node.children + nodes_to_expand
    return tokens


def treeify(n: Node) -> dict:
    this = {}
    this["type"] = n.type
    this["start_byte"] = n.start_byte
    this["end_byte"] = n.end_byte
    this["sexp"] = n.sexp()
    this["children"] = [treeify(c) for c in n.children]
    return this


if __name__ == "__main__":
    code = b"""#use rtrb::{RingBuffer, PopError};

let (mut p, mut c) = RingBuffer::new(4);

let mut it = vec![10, 20, 30].into_iter();
if let Ok(chunk) = p.write_chunk_uninit(2) {
    assert_eq!(chunk.fill_from_iter(&mut it), 2);
} else {
    unreachable!();
}
assert_eq!(c.pop(), Ok(10));
assert_eq!(c.pop(), Ok(20));
assert_eq!(c.pop(), Err(PopError::Empty));
assert_eq!(it.next(), Some(30));
}"""
    print(code)

    tree = parse_ast(code)

    pytree = treeify(tree.root_node)
    jsonstring = json.dumps(pytree, indent=2)
    with open("sample.tstree.json", "w") as f:
        f.write(jsonstring)

    tokens = tokenize(code)
    print(" ".join(tokens))

    func_query = RUST_LANGUAGE.query("""(
        (line_comment)*
        .
        (attribute_item)*
        .
        (function_item name: (identifier)) @func
    )""")
    for node, label in func_query.captures(tree.root_node):
        print(label, node)
