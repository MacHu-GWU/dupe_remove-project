#!/bin/bash
# -*- coding: utf-8 -*-
#
# This script should be sourced to use.


# GitHub
github_account="MacHu-GWU"
github_repo_name="dupe_remove-project"


# Python
package_name="dupe_remove"
py_ver_major="3"
py_ver_minor="6"
py_ver_micro="2"
use_pyenv="N" # "Y" or "N"
supported_py_versions="3.6.2" # e.g: "2.7.13 3.6.2"


#--- Doc Build
rtd_project_name="dupe_remove"

# AWS profile name for hosting doc on S3
# should be defined in ~/.aws/credentials
# read https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html for more information
aws_profile_doc_host="sanhe"

# html doc will be upload to:
# "s3://${S3_BUCKET_DOC_HOST}/docs/${PACKAGE_NAME}/${PACKAGE_VERSION}"
s3_bucket_doc_host="sanherabbit.com"


#--- AWS Lambda
# AWS profile name for deploy lambda function
# should be defined in ~/.aws/credentials
# read https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html for more information
aws_profile_for_lambda="identitysandbox.gov"

# deployment package file will be upload to:
# "s3://${s3_bucket_lambda_deploy}/lambda/${github_account}/${github_repo_name}/${package_name}-${package_version}.zip"
s3_bucket_lambda_deploy="login.gov-dev-sanhe"


# Docker
# deployment package will be built in this container
docker_image_for_build="lambci/lambda:build-python3.6"
# this container will be used for testing lambda invoke
docker_image_for_run="lambci/lambda:python3.6"
dir_container_workspace="/var/task"
