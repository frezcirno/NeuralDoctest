import argparse
from itertools import repeat
from multiprocessing import Pool
import os
import re
from typing import Literal
import uuid
import numpy as np
import pandas as pd
import langdetect
import token_utils
import regex
import swifter
from ts.parse import parse_ast

BAD_WORDS = ['TODO :', 'XXX :', '/ *',
             '{ @', '<! --', '---', '///', '***', '~~~', 'http : //', 'https : //']

XML_REGEXP = re.compile(r"<\w+ |< \/ \w+ >")


# class TokenizerV14International(BaseTokenizer):
tokenizer_v14_international_regexp = [
    # Separate out punctuations preceeded by a non-digit
    (regex.compile(r'(\P{N})(\p{P})'), r'\1 \2 '),
    # Separate out punctuations followed by a non-digit
    (regex.compile(r'(\p{P})(\P{N})'), r' \1 \2'),
    # Separate out symbols
    (regex.compile(r'(\p{S})'), r' \1 '),
]


def tokenizer_v14_international(line: str) -> str:
    for (_re, repl) in tokenizer_v14_international_regexp:
        line = _re.sub(repl, line)

    return ' '.join(line.split())


def index(haystack: str, pattern: str, *args, **kwargs) -> int:
    try:
        return haystack.index(pattern, *args, **kwargs)
    except ValueError:
        return -1


def find_code(doc: str) -> list[str]:
    """ Find all code blocks in a docstring.
    The docstring should be stripped of leading and trailing whitespace and slashs.
    """
    result = []

    opening = index(doc, '```')
    while opening != -1:
        eol = index(doc, '\n', opening)
        if eol == -1:
            break

        closing = index(doc, '```', opening + 3)
        if closing != -1:
            options = doc[opening + 3: eol].split(',')
            blacklist = ['python', 'sql', 'javascript', 'css', 'html', 'java', 'shell',
                         'c++', 'c', 'go', 'text', 'json', 'bash', 'dot', 'math', 'plain', 'text', 'txt', 'grammar', 'cargo', 'notrust', 'raw',
                         'no_run', 'ignore']
            if any(w in options for w in blacklist):
                opening = index(doc, '```', closing + 3)
                continue
            result.append(doc[eol + 1: closing])
        else:
            result.append(doc[eol + 1:])
            break

        opening = index(doc, '```', closing + 3)

    return result


def find_longest_code(doc: str) -> str:
    codes = find_code(doc)
    if len(codes) == 0:
        return ""
    return max(codes, key=len)


def multiline_strip(doc: str) -> str:
    """ strip beginning and ending whitespace in doc """
    return '\n'.join(line.lstrip(" \t").lstrip("/")[1:].rstrip() for line in doc.splitlines())


def get_lang(s: str):
    try:
        return langdetect.detect(s)
    except BaseException:
        return 'unk'


def is_english(s: str):
    return get_lang(s) in ['unk', 'en', 'ca', 'nl']


RUST_TOKEN2CHAR = {
    'STOKEN0': "//",
    'STOKEN1': "/*",
    'STOKEN2': "*/",
    'STOKEN3': '"""',
    'STOKEN4': '\\n'
}
RUST_CHAR2TOKEN = {
    "//": ' STOKEN0 ',
    "/*": ' STOKEN1 ',
    "*/": ' STOKEN2 ',
    '"""': ' STOKEN3 ',
    '\\n': ' STOKEN4 '
}


def truncate_docstring(s: str) -> str:
    """ truncate docstrings at the first "@param" or "." """
    if ".</p>" in s:
        s = s[:s.index(".</p>") + 1].replace("\n", " ")
    if ".\n" in s:
        s = s[:s.index(".\n") + 1].replace("\n", " ")

    if "\n\n" in s:
        s = s[:s.index("\n\n")].replace("\n", " ")
    if ". " in s:
        s = s[:s.index(". ") + 1].replace("\n", " ")
    if "\n" in s:
        s = s[:s.index("\n")]

    if s.startswith("<p>"):
        s = s[3:]

    assert '\n' not in s
    return s


def replace_tokens(tok: str, dictionary: dict) -> str:
    for char, special_token in dictionary.items():
        tok = tok.replace(char, special_token)
    return tok


def replace_general_string_tok(tok: str) -> str:
    return (
        tok.replace(" ", " ▁ ")
        .replace("\n", " STRNEWLINE ")
        .replace("\t", " TABSYMBOL ")
    )


def process_string(tok: str, is_comment: bool) -> str:
    if is_comment:
        tok = re.sub(" +", " ", tok)
        tok = re.sub(r"(.)\1\1\1\1+", r"\1\1\1\1\1", tok)
        if len(re.sub(r"\W", "", tok)) < 2:
            return ""

    tok = replace_general_string_tok(tok)
    tok = replace_tokens(tok, RUST_CHAR2TOKEN)
    if tok.strip().startswith("STOKEN00"):
        if " STRNEWLINE " in tok:
            tok = tok.replace(" STRNEWLINE ", " ENDCOM", 1)
        else:
            tok += " ENDCOM"

    tok = re.sub(" +", " ", tok)
    tok = tokenizer_v14_international(tok)
    tok = re.sub(" +", " ", tok)
    tok = tok.replace("\r", "")
    for special_token, char in RUST_TOKEN2CHAR.items():
        tok = tok.replace(special_token, char)
    if tok[0].isalpha():
        # for special strings, (e.g. L "s" we should remove the space after L)
        tok = tok.replace(f"{tok[0]} ", tok[0])
    return tok


def parse_code(code: str,
               handle_str: Literal['none|mask|process'],
               handle_num: Literal['none|mask'],
               handle_comment: Literal['none|drop']) -> str:
    """ Parse a code block and return processed codes. """
    if len(code) == 0:
        return ""

    if len(code) > 1.5e6:
        print(f"Code is too long: {len(code)}")
        return ""

    bcode = code.encode('utf-8')
    tree = parse_ast(bcode)

    # retrival
    nodes = []
    nodes_to_expand = [tree.root_node]
    while nodes_to_expand:
        node = nodes_to_expand.pop(0)
        if node.type == 'string_literal':
            nodes.append(node)
            continue

        if not node.children and node.start_byte < node.end_byte:
            nodes.append(node)
            continue

        nodes_to_expand = node.children + nodes_to_expand

    # process
    okcode = b''
    last = 0
    for node in nodes:
        text = bcode[node.start_byte:node.end_byte]

        okcode += bcode[last:node.start_byte]

        # Let BPE handle the identifier
        if node.type == 'line_comment' or node.type == 'block_comment':
            if handle_comment == 'drop':
                pass
            else:
                okcode += text

        elif node.type == 'string_literal' or node.type == 'raw_string_literal':
            if handle_str == 'mask':
                okcode += b'<STR>'
            elif handle_str == 'process':
                okcode += process_string(text.decode('utf-8'), False).encode('utf-8')
            else:
                okcode += text

        elif node.type == 'float_literal' or node.type == 'integer_literal':
            if handle_num == 'mask':
                okcode += b"<NUM>"
            else:
                okcode += text

        else:
            okcode += text

        last = node.end_byte

    okcode += bcode[last:]
    return okcode.decode('utf-8')


def process_df(
    df: pd.DataFrame,
    handle_str: Literal['none|mask|process'],
    handle_num: Literal['none|mask'],
    handle_comment: Literal['none|drop'],
) -> pd.DataFrame:
    # Filter out the rows
    df['doc'] = df.doc.map(multiline_strip)
    df = df[df.doc.map(is_english)]

    # Preprocess the rows
    doctest = df.doc.map(find_longest_code)
    doctest = doctest.apply(parse_code, args=(handle_str, handle_num, handle_comment))
    df['doctest'] = doctest
    # df = df[doctest.map(lambda s: '\n' not in s)]

    df['doc'] = df.doc.map(truncate_docstring)
    # df = df[df.doc.map(lambda s: '\t' not in s)]

    df['code'] = df.code.apply(parse_code, args=(handle_str, handle_num, handle_comment))
    # df = df[df.code.map(lambda s: '\t' not in s and '\n' not in s)]
    df = df[df.code.map(lambda s: len(s) > 0)]

    return df


def process(
    path: str,  # df: pd.DataFrame,
    handle_str: Literal['none|mask|process'],
    handle_num: Literal['none|mask'],
    handle_comment: Literal['none|drop'],
) -> None:
    df = pd.read_parquet(path)
    df = process_df(df, handle_str, handle_num, handle_comment)
    df.to_parquet(path)


def main():
    parser = argparse.ArgumentParser(description='Preprocess the dataset')
    parser.add_argument('--input', type=str, help='input file',
                        default="data-new/codedocdata.parquet")
    parser.add_argument('--output', type=str, help='output file',
                        default="data-new/codedocdata.preprocessed.parquet")
    parser.add_argument('--handle_str', type=str,
                        default='process', help='handle str')
    parser.add_argument('--handle_num', type=str,
                        default='none', help='handle num')
    parser.add_argument('--handle_comment', type=str,
                        default='drop', help='handle comment')
    args = parser.parse_args()

    df = pd.read_parquet(args.input)

    # Drop duplicates
    print("Dropping duplicates...", len(df), end=' ')
    df = df.drop_duplicates(subset=['code'])
    print("->", len(df))

    # Split the dataframe into multiple dataframes
    paths = []
    for i in range(0, df.shape[0], 10000):
        path = f'/tmp/part_{i}.parquet'
        df.iloc[i:i + 10000].to_parquet(path)
        paths.append(path)
    print(f"{len(paths)} files created")

    # Process the dataframe
    with Pool() as p:
        p.starmap(process, zip(paths, repeat(args.handle_str), repeat(args.handle_num), repeat(args.handle_comment)))

    # Merge the parts
    print("Merging...")
    df = pd.concat([pd.read_parquet(path) for path in paths])

    # Drop duplicates again
    print("Dropping duplicates...", len(df), end=' ')
    df = df.drop_duplicates(subset=['code'])
    print("->", len(df))

    df.to_parquet(args.output)

    # Clean up
    print("Cleaning up...")
    for path in paths:
        os.unlink(path)

    print(f"{args.output} created")


if __name__ == '__main__':
    main()
