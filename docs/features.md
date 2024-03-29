# Key Features

ChubaoDB is a cloud-native memory-centric distributed database. 

## APIs

NoSQL - redis API

NewSQL - MySQL API + PostgreSQL API

Graph - Gremlin

## sharding

table ranges, multi-raft replication, logical split, dynamic rebalancing

## storage engines

hot ranges: currently masstree as the in-memory store

warm ranges: rocksdb

## smart scheduling of storage medium

according to the access temperature, intelligent transition of range replicas between RAM and disk.  

## transactions

read committed, currently implemented

txn record, intent, version, 2PC

## auto-incr rowid

native support

*temporal locality*

## change data capture

## online schema change

## backup & recovery

on ChubaoFS

## deployment

Kubernetes native






