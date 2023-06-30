#! /usr/bin/env bash

yamllint .
flake8 --exclude 'venv' .
black .
