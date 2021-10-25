#! /usr/bin/env bash

yamllint .
flake8 .
black .
