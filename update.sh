#!/bin/bash

eval `ssh-agent -s`
ssh-add ./deploy
git pull
