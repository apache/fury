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
import dataclasses
import subprocess
import tempfile
import shutil

PROJECT_ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")

FURY_BUILDER_PATH = os.path.join(
    PROJECT_ROOT_DIR,
    "java/fury-core/src/main/java/org/apache/fury/config/FuryBuilder.java",
)
JAVA_DOC_PATH = os.path.join(PROJECT_ROOT_DIR, "docs/guide/java_serialization_guide.md")

DOC_GEN_BEGIN = "<!-- Auto generate region begin -->"
DOC_GEN_END = "<!-- Auto generate region end -->"

FIELD_LINE_PATTERN = re.compile(
    r"^(public\s+|protected\s+|private\s+|)(\w+|\w+<.+>)\s+\w+$"
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


def _parse_fields(fields_content):
    fields_info = []
    for field in fields_content:
        """
        Field format:
        <ul class="blockList">
            <li class="blockList">
                <h4>language</h4>
                <pre><a href="Language.html" title="enum in org.apache.fury.config">Language</a> language</pre>
                <div class="block">Whether cross-language serialize the object. If you used fury for java only, please set
                    language to <a href="Language.html#JAVA"><code>Language.JAVA</code></a>, which will have much better performance.
                </div>
                <dl>
                    <dt><span class="simpleTagLabel">defaultValue</span></dt>
                    <dd>Language.JAVA</dd>
                </dl>
            </li>
        </ul>
        """
        tag_labels = field.xpath('li/dl/dt/span[@class="simpleTagLabel"]/text()')
        is_config_field = "defaultValue" in tag_labels
        if not is_config_field:
            continue

        field_default_val = "".join(field.xpath("li/dl/dd//text()"))
        field_name = field.xpath("li/h4/text()")[0]
        field_comment = "".join(field.xpath("li/div//text()")).replace("\n", "")

        field_line = "".join(field.xpath("li/pre//text()"))
        match = FIELD_LINE_PATTERN.search(field_line)
        scope_group = match.group(1).strip()
        field_scope = None if len(scope_group) == 0 else scope_group
        field_type = match.group(2)
        fields_info.append(
            FieldInfo(
                field_scope, field_name, field_type, field_default_val, field_comment
            )
        )

    return fields_info


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
    # 1. Try installing lxml
    try_count = 3
    while try_count >= 0:
        try:
            from lxml import etree
        except Exception:
            if try_count == 0:
                raise Exception(f"Retrying {try_count} times to install lxml failed.")
            print("Try installing lxml.")
            subprocess.check_call("pip3 install lxml", shell=True)
        finally:
            try_count -= 1

    # 2. Generating javadoc
    print("Generating javadoc...")
    tmp_dir = tempfile.gettempdir()
    output_dir = "output"
    subprocess.call(
        f"cd {PROJECT_ROOT_DIR}java/fury-core;"
        f"mvn javadoc:javadoc -DreportOutputDirectory={tmp_dir} -DdestDir={output_dir} -Dshow=private",
        shell=True,
        stdout=subprocess.DEVNULL,
    )
    print("javadoc generated successfully.")

    # 3. Parsing javadoc
    javadoc_dir = os.path.join(tmp_dir, output_dir)
    fury_build_src = os.path.join(
        javadoc_dir, "org/apache/fury/config/FuryBuilder.html"
    )
    with open(fury_build_src) as f:
        content = f.read()
    shutil.rmtree(javadoc_dir)

    html = etree.HTML(content)
    # There is only one `Field Detail`
    field_detail = html.xpath('//div[@class="details"]//section[1]')[0]
    if field_detail is None:
        raise Exception(
            "There is no `Field Detail` related content in the current Javadoc."
        )
    fields_content = field_detail.xpath("ul/li/ul")
    fields_info = _parse_fields(fields_content)
    _write_content(fields_info)
    print("Doc update completed.")


if __name__ == "__main__":
    main()
