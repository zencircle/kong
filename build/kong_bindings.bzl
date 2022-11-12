"""
Global varibles from environment variables
"""

def _load_vars(ctx):
    # Read env from .requirements
    requirements = ctx.read(Label("@kong//:.requirements"))
    content = ctx.execute(["bash", "-c", "echo '%s' | " % requirements +
        """sed -E 's/(.*)=(.*)$/"\\1": "\\2",/'"""]).stdout
    content = content.replace('""', '"')

    # Load addtional env
    for name in ["PATH", "OPENRESTY_PREFIX", "OPENRESTY_RPATH", "OPENSSL_PREFIX", "LUAROCKS_PREFIX", "INSTALL_ROOT"]:
        value = ctx.os.environ.get(name, "")
        if value:
            content += '"%s": "%s",' % (name,value)

    # Workspace path
    content += '"WORKSPACE_PATH": "%s",' % ctx.path(Label("@//:WORKSPACE")).dirname
    content += '"EXEC_ROOT_PATH": "%s",' % ctx.path("@").dirname

    ctx.file("BUILD.bazel", "")
    ctx.file("variables.bzl", "KONG_ENV = {\n" + content + "\n}")

def _load_bindings_impl(ctx):
    _load_vars(ctx)

load_bindings = repository_rule(
    implementation = _load_bindings_impl,
)
