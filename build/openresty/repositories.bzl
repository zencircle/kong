"""A module defining the third party dependency OpenResty"""

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

def openresty_repositories():
    maybe(
        new_git_repository,
        name = "kong_build_tools",
        branch = "master",
        remote = "https://github.com/Kong/kong-build-tools",
        build_file = "//build/openresty:BUILD.kong-build-tools.bazel",
    )
