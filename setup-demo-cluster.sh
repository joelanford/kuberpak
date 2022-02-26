#!/bin/bash

kind delete cluster
kind create cluster
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.7.0/cert-manager.yaml
make install
kubectl create namespace kuberpak-system
