# Key Features

ChubaoDB is a cloud-native memory-centric distributed database. 

## APIs

NoSQL - redis API

NewSQL - MySQL API + PostgreSQL API

## sharding

table ranges, multi-raft replication, logical split, dynamic rebalancing

## storage engines

hot ranges: masstree as the in-memory store

warm ranges: rocksdb

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






