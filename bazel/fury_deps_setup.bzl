# This file is derived from  https://github.com/ray-project/ray/blob/7fb3feb25e2c19616944f16b026b7f5052451e8d/bazel/ray_deps_setup.bzl

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

def get_mirror_url(name, sha256, url):
    """ Get internal URL of the library if no public network access. """
    SUPPORTED_EXTENSIONS = [".zip", ".tar.gz", ".tar.bz2"]
    extension = None
    for ex in SUPPORTED_EXTENSIONS:
        if url.endswith(ex):
            extension = ex
            break
    if extension == None:
        fail("Unknown extension: " + url)

    # This needs to be in sync with `oss_uploader.py`
    file_name = "%s-%s%s" % (name, sha256, extension)
    # return "http://internal/" + file_name
    return url

def urlsplit(url):
    """ Splits a URL like "https://example.com/a/b?c=d&e#f" into a tuple:
        ("https", ["example", "com"], ["a", "b"], ["c=d", "e"], "f")
    A trailing slash will result in a correspondingly empty final path component.
    """
    split_on_anchor = url.split("#", 1)
    split_on_query = split_on_anchor[0].split("?", 1)
    split_on_scheme = split_on_query[0].split("://", 1)
    if len(split_on_scheme) <= 1:  # Scheme is optional
        split_on_scheme = [None] + split_on_scheme[:1]
    split_on_path = split_on_scheme[1].split("/")
    return {
        "scheme": split_on_scheme[0],
        "netloc": split_on_path[0].split("."),
        "path": split_on_path[1:],
        "query": split_on_query[1].split("&") if len(split_on_query) > 1 else None,
        "fragment": split_on_anchor[1] if len(split_on_anchor) > 1 else None,
    }

def auto_http_archive(
        *,
        name = None,
        url = None,
        urls = True,
        build_file = None,
        build_file_content = None,
        strip_prefix = True,
        **kwargs):
    """ Intelligently choose mirrors based on the given URL for the download.

    Either url or urls is required.

    If name         == None , it is auto-deduced, but this is NOT recommended.
    If urls         == True , mirrors are automatically chosen.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    """
    DOUBLE_SUFFIXES_LOWERCASE = [("tar", "bz2"), ("tar", "gz"), ("tar", "xz")]
    mirror_prefixes = ["https://mirror.bazel.build/"]

    canonical_url = url if url != None else urls[0]
    url_parts = urlsplit(canonical_url)
    url_except_scheme = (canonical_url.replace(url_parts["scheme"] + "://", "") if url_parts["scheme"] != None else canonical_url)
    url_path_parts = url_parts["path"]
    url_filename = url_path_parts[-1]
    url_filename_parts = (url_filename.rsplit(".", 2) if (tuple(url_filename.lower().rsplit(".", 2)[-2:]) in
                                                          DOUBLE_SUFFIXES_LOWERCASE) else url_filename.rsplit(".", 1))
    is_github = url_parts["netloc"] == ["github", "com"]

    if name == None:  # Deduce "com_github_user_project_name" from "https://github.com/user/project-name/..."
        name = "_".join(url_parts["netloc"][::-1] + url_path_parts[:2]).replace("-", "_")

    if build_file == True:
        build_file = "@fury//%s:%s" % ("bazel", "BUILD." + name)

    if urls == True:
        prefer_url_over_mirrors = is_github
        urls = [
            mirror_prefix + url_except_scheme
            for mirror_prefix in mirror_prefixes
            if not canonical_url.startswith(mirror_prefix)
        ]
        urls.insert(0 if prefer_url_over_mirrors else len(urls), canonical_url)
    else:
        print("No implicit mirrors used because urls were explicitly provided")

    if strip_prefix == True:
        prefix_without_v = url_filename_parts[0]
        if prefix_without_v.startswith("v") and prefix_without_v[1:2].isdigit():
            # GitHub automatically strips a leading 'v' in version numbers
            prefix_without_v = prefix_without_v[1:]
        strip_prefix = (url_path_parts[1] + "-" + prefix_without_v if is_github and url_path_parts[2:3] == ["archive"] else url_filename_parts[0])

    # Use internal OSS address.
    url = get_mirror_url(name, kwargs["sha256"], url or urls[0])
    urls = None

    # Add `@com_github_ray_project_ray` prefix to the patch files,
    # otherwise when other projects loads Ray, there will be issues finding these files.
    if "patches" in kwargs:
        kwargs["patches"] = [
            "@fury" + patch
            for patch in kwargs["patches"]
        ]

    return http_archive(
        name = name,
        url = url,
        urls = urls,
        build_file = build_file,
        build_file_content = build_file_content,
        strip_prefix = strip_prefix,
        **kwargs
    )

def setup_deps():
    # Fix @platforms error, see https://groups.google.com/g/bazel-discuss/c/iQyt08ZaNek
    http_archive(
        name = "bazel_skylib",
        sha256 = "74d544d96f4a5bb630d465ca8bbcfe231e3594e5aae57e1edbf17a6eb3ca2506",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.3.0/bazel-skylib-1.3.0.tar.gz",
            "https://github.com/bazelbuild/bazel-skylib/releases/download/1.3.0/bazel-skylib-1.3.0.tar.gz",
        ],
    )
    http_archive(
        name = "rules_python",
        sha256 = "9fcf91dbcc31fde6d1edb15f117246d912c33c36f44cf681976bd886538deba6",
        strip_prefix = "rules_python-0.8.0",
        url = "https://github.com/bazelbuild/rules_python/archive/refs/tags/0.8.0.tar.gz",
    )
    auto_http_archive(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        url = "https://github.com/grpc/grpc/archive/refs/tags/v1.46.6.tar.gz",
        sha256 = "6514b3e6eab9e9c7017304512d4420387a47b1a9c5caa986643692977ed44e8a",
        patches = [
            "//bazel:grpc-cython-copts.patch",
            "//bazel:grpc-python.patch",
        ],
    )
    # leary cython failed with: `found 'CKeyValueMetadata'`
    # see https://github.com/apache/arrow/issues/28629
    auto_http_archive(
        name = "cython",
        build_file = "@com_github_grpc_grpc//third_party:cython.BUILD",
        url = "https://github.com/cython/cython/releases/download/3.0.0/Cython-3.0.0.tar.gz",
        sha256 = "350b18f9673e63101dbbfcf774ee2f57c20ac4636d255741d76ca79016b1bd82",
    )
    auto_http_archive(
        name = "com_google_googletest",
        url = "https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz",
        sha256 = "b4870bf121ff7795ba20d20bcdd8627b8e088f2d1dab299a031c1034eddc93d5",
    )
    auto_http_archive(
        name = "com_google_absl",
        sha256 = "5366d7e7fa7ba0d915014d387b66d0d002c03236448e1ba9ef98122c13b35c36",
        strip_prefix = "abseil-cpp-20230125.3",
        urls = [
            "https://github.com/abseil/abseil-cpp/archive/20230125.3.tar.gz",
        ],
    )
