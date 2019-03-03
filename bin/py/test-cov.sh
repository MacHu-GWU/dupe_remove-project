#!/bin/bash
# -*- coding: utf-8 -*-

dir_here="$( cd "$(dirname "$0")" ; pwd -P )"
dir_bin="$(dirname "${dir_here}")"
dir_project_root=$(dirname "${dir_bin}")

source ${dir_bin}/py/python-env.sh

print_colored_line $color_cyan "[DOING] Run code coverage tests in ${path_test_dir} ..."
cd ${dir_project_root}
rm -r ${path_coverage_annotate_dir}
(
    docker run --rm --name dupe-remove-test-db -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres
    sleep 3
    ${bin_pytest} ${path_test_dir} -s --cov=${package_name} --cov-report term-missing --cov-report annotate:${path_coverage_annotate_dir}
    docker container stop dupe-remove-test-db
)
