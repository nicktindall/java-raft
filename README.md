# java-raft
[![CircleCI](https://circleci.com/gh/nicktindall/java-raft.svg?style=svg)](https://circleci.com/gh/nicktindall/java-raft)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=nicktindall_java-raft&metric=alert_status)](https://sonarcloud.io/dashboard?id=nicktindall_java-raft)

A Java implementation of the [Raft consensus algorithm](https://raft.github.io/)

## motivation
I'm implementing the protocol in order to get a more in-depth understanding of it. Also as a challenge to make a readable implementation of the "understandable" consensus algorithm.

## Status
The parts of the algorithm I have implemented (according to the thesis chapters) are:

|Chapter   |Name   | Status   |
|----------|-------|----------|
| 3.4 | Leader election | :heavy_check_mark: |
| 3.5 | Log replication | :heavy_check_mark: |
| 3.6.1 | Election restriction | :heavy_check_mark: |
| 3.6.2 | Committing entries from previous terms | :heavy_check_mark: |
| 3.7 | Follower and candidate crashes | :heavy_check_mark: |
| 3.8 | Persisted state and server restarts | :heavy_check_mark: |
| 3.10 | Leadership transfer extension | :heavy_check_mark: |
| 4 | Cluster membership changes | :heavy_check_mark: |
| 4.2.1 | Catching up new servers | :heavy_check_mark: |
| 4.2.2 | Removing the current leader | :x: |
| 4.2.3 | Disruptive servers | :x: |
| 4.3 | Arbitrary configuration changes using joint consensus | :x: |
| 5 | Log compaction | :heavy_check_mark: |
| 6.1 | Finding the cluster | :x: |
| 6.2 | Routing requests to the leader | :heavy_check_mark: |
| 6.3 | Implementing linearizable semantics | :heavy_check_mark: |
| 6.4 | Processing read-only queries more efficiently | :x: |

## Dependencies
The only runtime dependency is log4j 2, the aim is to implement with minimal dependencies
