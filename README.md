# cwl-dummy-tool

An alternative to [cwl-dummy][] that relies on a modified
`DockerRequirement` rather than rewriting the whole CWL file.

[cwl-dummy]: https://github.com/wtsi-hgi/cwl-dummy

Mainly intended to be used with a `DockerRequirement` that points to an
image built from this repository, but rudimentary support for non-Docker
execution is available on the `no-docker` branch (add the `bin`
directory to your path before running cwltool). The original cwl-dummy
can be used to modify `DockerRequirement`s to point to such an image.
