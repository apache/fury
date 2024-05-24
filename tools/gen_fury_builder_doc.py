# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import re
import sys
import dataclasses

PROJECT_ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")

FURY_BUILDER_PATH = os.path.join(
    PROJECT_ROOT_DIR,
    "java/fury-core/src/main/java/org/apache/fury/config/FuryBuilder.java",
)
JAVA_DOC_PATH = os.path.join(PROJECT_ROOT_DIR, "docs/guide/java_serialization_guide.md")

JAVA_CONFIG_BEGIN = "// ======== Config Area Begin ========"
JAVA_CONFIG_END = "// ======== Config Area End ========="
DOC_GEN_BEGIN = "<!-- Auto generate region begin -->"
DOC_GEN_END = "<!-- Auto generate region end -->"

SPLIT_FIELD_PATTERN = re.compile(r"(?![*]+);\s*$", flags=re.MULTILINE)
COMMENT_TMPL_PATTERN = re.compile(r"^\s*/[*]+\s*.*\s*[*]+/$", re.S | re.MULTILINE)
SINGLE_LINE_COMMENT_PATTERN = re.compile(r"^\s*/[*]+(.+)[*]+/$", re.MULTILINE)
MULTI_LINE_COMMENT_PATTERN = re.compile(
    r"^(?!^\s*[*]\s?$)(\s*[*]+[\x20]*)[\x20]?(.*)[\x20]?(?<![*]/)$", re.MULTILINE
)
FIELD_PATTERN = re.compile(
    r"^(?!^\s*[*]+)\s*(public\s+|private\s+|protected\s+|)(\w+|\w+<.+>)\s+(\w+)\s*=?\s*(.*)$",
    re.MULTILINE,
)
FILE_REPLACE_PATTERN = re.compile(
    rf"^{DOC_GEN_BEGIN}.*{DOC_GEN_END}$", flags=re.MULTILINE | re.S
)


@dataclasses.dataclass
class FieldInfo:
    field_scope: str
    field_name: str
    field_type: str
    field_default_val: str
    field_comment: str


def _parse_fields(content):
    fields = SPLIT_FIELD_PATTERN.split(content)
    result = []
    for field in fields:
        field_info = _parse_field(field)
        if field_info is not None:
            result.append(field_info)

    return result


def _parse_field(field):
    # 1. Format comment section
    comment_overview_match = COMMENT_TMPL_PATTERN.search(field)
    if comment_overview_match is None:
        comment = None
        default_val = None
    else:
        single_line_match = SINGLE_LINE_COMMENT_PATTERN.search(field)
        if single_line_match is not None:
            fat_comment = single_line_match.group(1)
        else:
            multi_line_matches = MULTI_LINE_COMMENT_PATTERN.finditer(field)
            fat_comment = ""
            for match in multi_line_matches:
                data = match.group(2)
                fat_comment += data + " "

        cutting_with_tag = "<p>defaultValue:"
        cutting = "defaultValue:"
        if field.find(cutting_with_tag) != -1:
            split_data = fat_comment.split(cutting_with_tag)
        else:
            split_data = fat_comment.split(cutting)
        assert (
            len(split_data) <= 2
        ), "Only one defaultValue can be specified in the comment."

        default_val = split_data[1].strip() if len(split_data) == 2 else None
        comment = split_data[0].strip()

    # 2. Format field section
    field_match = FIELD_PATTERN.search(field)
    if field_match is None:
        return None

    scope = field_match.group(1).strip()
    type = field_match.group(2)
    name = field_match.group(3)
    if (
        default_val is None
        and field_match.group(4) is not None
        and len(field_match.group(4)) > 0
    ):
        default_val = field_match.group(4)
    field_info = FieldInfo(scope, name, type, default_val, comment)
    return field_info


def _write_content(fields):
    if len(fields) == 0:
        return

    with open(JAVA_DOC_PATH) as f:
        content = f.read()
        if content is None:
            return

    """
        Table format:
            | Option Name   | Description        | Default Value     |       <------ Table header
            |---------------|--------------------|-------------------|       <------ Table delimiter
            | `xxxxxx`      | xxxxxxxx           | xxxxx             |       <------ Table body
    """
    hdr1 = " Option Name"
    hdr2 = " Description"
    hdr3 = " Default Value"
    margin_right = 10
    hdr1_width = len(hdr1) + margin_right
    hdr2_width = len(hdr2) + margin_right
    hdr3_width = len(hdr3) + margin_right
    for field in fields:
        fname = field.field_name
        fdesc = (
            field.field_comment
            if field.field_comment is not None and len(field.field_comment) > 0
            else "None"
        )
        fval = (
            field.field_default_val
            if field.field_default_val is not None and len(field.field_default_val) > 0
            else "None"
        )

        if len(fname) > hdr1_width:
            hdr1_width = len(fname) + margin_right

        if len(fdesc) > hdr2_width:
            hdr2_width = len(fdesc) + margin_right

        if len(fval) > hdr3_width:
            hdr3_width = len(fval) + margin_right

    table_header = (
        f"|{hdr1}{(hdr1_width - len(hdr1)) * ' '}"
        f"|{hdr2}{(hdr2_width - len(hdr2)) * ' '}"
        f"|{hdr3}{(hdr3_width - len(hdr3)) * ' '}|"
        f"\n"
    )
    table_delimiter = f"|{hdr1_width * '-'}|{hdr2_width * '-'}|{hdr3_width * '-'}|\n"
    table_body = ""
    for field in fields:
        fname = field.field_name
        fdesc = (
            field.field_comment
            if field.field_comment is not None and len(field.field_comment) > 0
            else "None"
        )
        fval = (
            field.field_default_val
            if field.field_default_val is not None and len(field.field_default_val) > 0
            else "None"
        )

        row = (
            f"|`{fname}`{(hdr1_width - len(fname) - 2) * ' '}"
            f"| {fdesc}{(hdr2_width - len(fdesc) - 1) * ' '}"
            f"| {fval}{(hdr3_width - len(fval) - 1) * ' '}|"
            f"\n"
        )
        table_body += row

    table = table_header + table_delimiter + table_body
    repl = f"{DOC_GEN_BEGIN}\n{table}\n{DOC_GEN_END}"
    to_write = FILE_REPLACE_PATTERN.sub(repl, content)

    with open(JAVA_DOC_PATH, "w") as f:
        f.write(to_write)


def main():
    with open(FURY_BUILDER_PATH) as f:
        content = f.read()
        if content is None:
            return 1
    start_idx = content.find(JAVA_CONFIG_BEGIN)
    end_idx = content.find(JAVA_CONFIG_END)
    if start_idx == -1 or end_idx == -1:
        return 0

    fields = _parse_fields(content[start_idx + len(JAVA_CONFIG_BEGIN) : end_idx])
    _write_content(fields)


if __name__ == "__main__":
    sys.exit(main())
