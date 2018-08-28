#!/usr/bin/env python3.6


import argparse
import functools
import itertools
import json
import os
import pathlib
import random
import re
import shlex
import sys
import time
from typing import Match, Optional

import split_interval_list


def _print(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)


def _touch(name):
    _print("creating file {!r}".format(name))
    open(name, "w").close()


def _mkdir(name):
    _print("creating directory {!r}".format(name))
    os.mkdir(name)


def _parser():
    # allow_abbrev could lead to misparsed arguments if using
    # parse_known_args, so disable it.
    return argparse.ArgumentParser(allow_abbrev=False)


def _sleep_mins(min, max=None):
    if max is None:
        max = min
    t = random.uniform(min, max)
    _print("Sleeping for {:.5} minutes".format(t))
    time.sleep(t * 60)


class _Regex:
    """Assignment expressions, kind of.

    r = _Regex()
    if r.match("foo", s):
        process(r.last_match)
    elif r.match("bar", s):
        process(r.last_match)
    else:
        ...
    """

    def __init__(self):
        self.last_match: Optional[Match] = None

    def __getattr__(self, item):
        meth = getattr(re, item)

        @functools.wraps(meth)
        def _wrapper(*args, **kwargs):
            self.last_match = meth(*args, **kwargs)
            return self.last_match

        return _wrapper


class UnhandledCommandError(Exception):
    pass


def bash(argv):
    _print("emulating bash")
    _print("with argv: {!r}".format(argv))
    parser = _parser()
    parser.add_argument("-c")
    args, _ = parser.parse_known_args(argv)
    if argv[0] == "/interval_list_to_json.sh":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/interval-list-to-json/interval_list_to_json.sh
        # Note: this is used for scattering.
        _print("emulating interval_list_to_json")
        lines = []
        with open(argv[1], "r") as f:
            for line in f:
                if not line.startswith("@"):
                    lines.append(line)
        if not lines:
            _print("no lines found, using fallback output")
            lines = ["1:2-3"]
        print(json.dumps(lines))
        _print("scattering note: {} list items written".format(len(lines)))
    elif args.c:
        r = _Regex()
        command_line = list(shlex.shlex(args.c, posix=True, punctuation_chars=True))
        _print("with command line: {!r}".format(command_line))
        if all(itertools.starmap(
            lambda a, b: a is None or a == b,
            zip(["samtools", "view", "-H", None, "|", "awk", None, ">", None], command_line)
        )):
            # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/cwl/tools/list_rgs.cwl
            # Note: this is used for scattering.
            infile = command_line[3]
            outfile = command_line[8]
            _print("emulating list_rgs")
            _print("with infile: {!r}".format(infile))
            _print("with outfile: {!r}".format(outfile))
            with open(outfile, "w") as out:
                for i in range(1, 9):
                    out.write("{}\n".format(i))
            _print("scattering note: 8 lines written")
        elif all(itertools.starmap(
            lambda a, b: a is None or a == b,
            zip(["cat", None, "|", "awk", None, ">", None], command_line)
        )) and r.search(r"/\^@/\s*\|\|\s*\$1~/([^/]+)/", command_line[4]):
            # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/cwl/tools/filter_interval_list.cwl
            _print("emulating filter_interval_list")
            lines = []
            # The code used in filter_interval_list is:
            #     /^@/ || $1~/$(inputs.chromosome_regex)/
            # i.e. if the whole line starts with an "@", or if the
            # first field (tab-separated) matches the chromosome regex.
            regex = re.compile(r"(^@|{})".format(r.last_match.group(1)))
            _print("with regex: {!r}".format(regex.pattern))
            with open("/interval_list", "r") as f:
                lines.extend(filter(lambda l: regex.match(l.split("\t")[0]), f))
            _touch(command_line[6])
            with open(command_line[6], "w") as f:
                f.writelines(lines)
        else:
            raise UnhandledCommandError("Unrecognised bash command string")
    else:
        raise UnhandledCommandError("Unrecognised bash command")


def bcftools(argv):
    # https://samtools.github.io/bcftools/bcftools.html
    _print("emulating bcftools")
    _print("with argv: {!r}".format(argv))
    if argv[0] == "concat":
        _print("emulating bcftools concat")
        parser = _parser()
        parser.add_argument("-o", "--output")
        args, _ = parser.parse_known_args(argv[1:])
        _touch(args.output)
    elif argv[0] == "index":
        _print("emulating bcftools index")
        parser = _parser()
        parser.add_argument("-o", "--output")
        parser.add_argument("-c", "--csi", action="store_true")
        parser.add_argument("-t", "--tbi", action="store_true")
        parser.add_argument("infile")
        args, _ = parser.parse_known_args(argv[1:])
        if args.output:
            _touch(args.output)
        elif args.tbi:
            _touch(args.infile + ".tbi")
        else:
            _touch(args.infile + ".csi")
    else:
        raise UnhandledCommandError("Unrecognised bcftools command")


def bedtools(argv):
    _print("emulating bedtools")
    if argv[0] == "intersect":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/cwl/tools/bedtools/bedtools-intersect.cwl
        # NB: this tool prints to stdout.
        _print("emulating bedtools intersect")
    else:
        raise UnhandledCommandError("Unrecognised bedtools command")


def capmq(argv):
    # https://github.com/mcshane/capmq
    _print("emulating capmq")
    parser = _parser()
    parser.add_argument("-C")
    parser.add_argument("-S", action="store_true")
    parser.add_argument("-r", action="store_true")
    parser.add_argument("-v", action="store_true")
    parser.add_argument("-g")
    parser.add_argument("-G")
    parser.add_argument("-f", action="store_true")
    parser.add_argument("-m")
    parser.add_argument("-I")
    parser.add_argument("-O")
    parser.add_argument("infile")
    parser.add_argument("outfile")
    args = parser.parse_args(argv)
    _touch(args.outfile)


def java(argv):
    _print("emulating java")
    _print("with argv: {!r}".format(argv))
    i = argv.index("-jar")
    jar = argv[i + 1]
    argv = argv[i + 2 :]
    if jar == "/gatk/gatk.jar":
        if argv[0] in {"HaplotypeCaller", "GenotypeGVCFs"}:
            # https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.7.0/org_broadinstitute_hellbender_tools_walkers_haplotypecaller_HaplotypeCaller.php
            # https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.7.0/org_broadinstitute_hellbender_tools_walkers_GenotypeGVCFs.php
            _print("emulating gatk 4 command {}".format(argv[0]))
            parser = _parser()
            parser.add_argument("-O", "--output")
            parser.add_argument("-OVI", "--create-output-variant-index")
            parser.add_argument("-OVM", "--create-output-variant-md5")
            args, _ = parser.parse_known_args(argv[1:])
            _touch(args.output)
            if args.create_output_variant_index:
                if args.output.endswith(".gz"):
                    _touch(args.output + ".tbi")
                else:
                    _touch(args.output + ".idx")
            if args.create_output_variant_md5:
                _touch(args.output + ".md5")
        elif argv[0] == "GenomicsDBImport":
            # https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.7.0/org_broadinstitute_hellbender_tools_genomicsdb_GenomicsDBImport.php
            _print("emulating gatk 4 command {}".format(argv[0]))
            parser = _parser()
            parser.add_argument("--genomicsdb-workspace-path")
            args, _ = parser.parse_known_args(argv[1:])
            try:
                _mkdir(args.genomicsdb_workspace_path)
            except FileExistsError:
                pass
        else:
            raise UnhandledCommandError("Unrecognised gatk 4 command")
    else:
        raise UnhandledCommandError("Unrecognised java command")


def python(argv):
    _print("emulating python")
    _print("with argv: {!r}".format(argv))
    if argv[0] == "/get_read_group_caps.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/get-read-group-caps/get_read_group_caps.py
        _print("emulating get_read_group_caps")
        _touch("caps_file")
    elif argv[0] == "/dict_to_interval_list.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/dict-to-interval-list/dict_to_interval_list.py
        _print("emulating dict_to_interval_list")
        parser = _parser()
        parser.add_argument("--path")
        parser.add_argument("--output_dir")
        args = parser.parse_args(argv[1:])
        filename = pathlib.Path(args.path).with_suffix(".interval_list").name
        _touch(pathlib.Path(args.output_dir) / filename)
    elif argv[0] == "/il_to_bed.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/interval-list-to-bed/il_to_bed.py
        _print("emulating interval-list-to-bed")
        _touch("output.bed")
        _touch("header.txt")
    elif argv[0] == "/bed_to_il.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/bed-to-interval-list/bed_to_il.py
        _print("emulating bed-to-interval-list")
        _touch("output.bed")
    elif argv[0] == "/split_interval_list.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/split-interval-list/split_interval_list.py
        # Note: this is used for scattering (both number of files and
        # number of lines per file). Therefore, we use the real
        # implementation of split_interval_list. The interval lists come
        # from filter_interval_list.
        _print("running split_interval_list")
        parser = _parser()
        parser.add_argument("--path", type=pathlib.Path)
        parser.add_argument("--output_dir", type=pathlib.Path)
        parser.add_argument("--chunks", type=int)
        args = parser.parse_args(argv[1:])
        split_interval_list.split_interval_lists(args.chunks, args.path, args.output_dir)
        _print("scattering note: created {} files".format(args.chunks))
    elif argv[0] == "-c":
        _print("emulating python command")
        if re.search(r"cwl\.output\.json.*json\.dumps.*transposed_array", argv[1], flags=re.DOTALL):
            # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/cwl/expression-tools/matrix_transpose.cwl
            # Note: this is used for scattering.
            _print("running matrix_transpose")
            exec(argv[1], {}, {})
        else:
            raise UnhandledCommandError("Unrecognised python command string")
    else:
        raise UnhandledCommandError("Unrecognised python command")


def python3(argv):
    _print("emulating python3")
    _print("with argv: {!r}".format(argv))
    if argv[0] == "/gatk-local-io-wrapper.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/gatk-4.0.0.0-local-io-wrapper/gatk-local-io-wrapper.py
        gatk_command = argv[4]
        _print("emulating gatk-local-io-wrapper for {}".format(gatk_command))
        java(["-d64", *json.loads(argv[3]), "-jar", "/gatk/gatk.jar", gatk_command, *argv[5:]])
    elif argv[0] == "/gatk-tmpdir-output-wrapper.py":
        # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/gatk-4.0.0.0-tmpdir-output-wrapper/gatk-tmpdir-output-wrapper.py
        gatk_command = argv[3]
        _print("emulating gatk-tmpdir-output-wrapper for {}".format(gatk_command))
        java(["-d64", *json.loads(argv[2]), "-jar", "/gatk/gatk.jar", gatk_command, *argv[4:]])
    else:
        raise UnhandledCommandError("Unrecognised python3 command")


def samtools(argv):
    _print("emulating samtools")
    _print("with argv: {!r}".format(argv))
    if argv[0] in {"fastaref", "dict"}:
        _print("emulating samtools {}".format(argv[0]))
        parser = _parser()
        parser.add_argument("-o", "--output")
        args, _ = parser.parse_known_args(argv[1:])
        _touch(args.output)
    elif argv[0] == "faidx":
        _print("emulating samtools faidx")
        _touch(argv[1] + ".fai")
    elif argv[0] == "index":
        _print("emulating samtools index")
        parser = _parser()
        parser.add_argument("-b", action="store_true")
        parser.add_argument("-c", action="store_true")
        parser.add_argument("-m")
        parser.add_argument("infile")
        parser.add_argument("outfile", nargs="?")
        args = parser.parse_args(argv[1:])
        if not args.outfile:
            if args.infile.endswith(".cram"):
                args.outfile = args.infile + ".crai"
            elif args.infile.endswith(".bam"):
                if args.c or args.m:
                    args.outfile = args.infile + ".csi"
                else:
                    args.outfile = args.infile + ".bai"
            else:
                raise UnhandledCommandError("Unrecognised file type")
        _touch(args.outfile)
    else:
        raise UnhandledCommandError("Unrecognised samtools command")


def seq_cache_populate_pl(argv):
    # https://github.com/samtools/samtools/blob/develop/misc/seq_cache_populate.pl
    _print("emulating seq_cache_populate.pl")
    parser = _parser()
    parser.add_argument("-root")
    args, _ = parser.parse_known_args(argv)
    try:
        _mkdir(args.root)
    except FileExistsError:
        pass


def sh(argv):
    _print("emulating sh")
    _print("with argv: {!r}".format(argv))
    try:
        i = argv.index("-c")
    except IndexError:
        raise UnhandledCommandError("Unrecognised sh command (no command to execute)")
    # Detect situations like this and bail out:
    #   sh \
    #   -c \
    #   'python /create-file.py "$1"' \
    #   touch \
    #   /tmp/foo.cram
    # since then we would end up creating a file called "$1"
    # rather than "/tmp/foo.cram" (assuming we handled "python
    # /create-file.py" correctly).
    if len(argv) > i + 2:
        raise UnhandledCommandError("Unrecognised sh command (has arguments)")
    assert argv[i + 1] is argv[-1]
    cmd = list(shlex.shlex(argv[i + 1], posix=True, punctuation_chars=True))
    _invoke(cmd)


def verifybamid_rg(argv):
    # https://github.com/wtsi-hgi/arvados-pipelines/blob/master/docker/verifybamid2-1.0.4-samtools-1.6/verifybamid_rg.sh
    _print("emulating verifybamid_rg")
    parser = _parser()
    parser.add_argument("-c", "--cram-file")
    args, _ = parser.parse_known_args(argv)
    filename = pathlib.Path(args.cram_file)
    if filename.suffix == ".cram":
        filename = filename.with_suffix(".verifybamid2.out")
    else:
        filename = filename.with_suffix(filename.suffix + ".verifybamid2.out")
    _touch(filename.name)


def _invoke(argv):
    prog = argv[0].split(os.sep)[-1].replace(".", "_")
    _print("emulating program: {!r}".format(prog))
    _print("with argv: {!r}".format(argv))
    try:
        globals()[prog](argv[1:])
    except KeyError:
        raise UnhandledCommandError("Unrecognised program")


if __name__ == "__main__":
    try:
        _invoke(sys.argv[1:])
    except UnhandledCommandError as e:
        if e.args:
            print(*e.args, file=sys.stderr)
        else:
            print("Unhandled command", file=sys.stderr)
        sys.exit(127)
