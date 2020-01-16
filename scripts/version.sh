#! /bin/sh

## `ocaml-version` should be in sync with `README.rst` and
## `lib.protocol-compiler/tezos-protocol-compiler.opam`

ocaml_version=4.09.1
opam_version=2.0

## Please update `.gitlab-ci.yml` accordingly
## full_opam_repository is a commit hash of the public OPAM repository, i.e.
## https://github.com/ocaml/opam-repository
full_opam_repository_tag=4dd2620bcc821418bae53669a6c6163964c090a2

## opam_repository is an additional, tezos-specific opam repository.
opam_repository_tag=5bf955c2fefa0a6ab812e22c59b8b45bbe37cce8
opam_repository_url=https://gitlab.com/nomadic-labs/opam-repository.git
opam_repository=$opam_repository_url\#$opam_repository_tag

## Other variables, used both in Makefile and scripts
COVERAGE_OUTPUT=_coverage_output
