# RADAR-Commons
[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Commons.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Commons)

Common utilities library containing basic schemas, streaming features, testing bridges and utils.

# Dependencies
  1. [RADAR-Schema](https://github.com/RADAR-CNS/RADAR-Schemas)

# Using in other repositories

## Use as a submodule
    
```shell
git submodule add https://github.com/RADAR-CNS/RADAR-Commons.git your-folder

git submodule update --init --recursive
```

## Use it as a dependency

```shell
wget https://github.com/RADAR-CNS/RADAR-Commons/releases/download/0.1-SNAPSHOT/radar-commons-0.1-SNAPSHOT.jar
```
Add the `jar` file as a dependency

For gradle users, add
```
dependencies {
....
....
compile group: 'org.radarcns', name: 'radar-commons', version: 0.1-SNAPSHOT
}
```
under in your `build.gradle`

Note: This repository is still in WIP. Expect frequent changes. 
For latest code use `dev` branch 
