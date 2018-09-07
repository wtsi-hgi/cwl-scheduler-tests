# cwl-dummy-tool

An alternative to [cwl-dummy][] that relies on a modified
`DockerRequirement` rather than rewriting the whole CWL file.

[cwl-dummy]: https://github.com/wtsi-hgi/cwl-dummy

Mainly intended to be used with a `DockerRequirement` that points to an
image built from this repository, but rudimentary support for non-Docker
execution is available on the `no-docker` branch (add the `bin`
directory to your path before running cwltool). The original cwl-dummy
can be used to modify `DockerRequirement`s to point to such an image.

The cwl-dummy-tool Docker container is available on Docker Hub at
[`mercury/cwl-scheduler-tests`](https://hub.docker.com/r/mercury/cwl-scheduler-tests/).

To use this, change the `DockerRequirement` on a CWL tool or workflow
to point to the above Docker container, then use your favourite CWL
runner to execute the tool/workflow. If cwl-dummy-tool supports the
program your CWL file uses, it will automatically create the outputs the
real tool would have created, then immediately exit.

NB: cwl-dummy-tool is intended to be used with HGI's workflows, so it
contains a few hacks that will probably break if you try to use it on
other CWL files.

HGI people: even though it doesn't do any useful computation, this
container still needs real interval list files in order to work
properly, since the workflow starts one tool for each interval.
