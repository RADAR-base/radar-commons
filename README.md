# RADAR-Commons
[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Commons.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Commons)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9fe7a419c83e4798af671e468c7e91cf)](https://www.codacy.com/app/RADAR-CNS/RADAR-Commons?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Commons&amp;utm_campaign=Badge_Grade)

Common utilities library containing basic schemas, streaming features, testing bridges and utils.

# Usage

Add the RADAR-Commons library to your project with Gradle by updating your `build.gradle` file with:

```gradle
repositories {
    jcenter()
}

dependencies {
    compile group: 'org.radarcns', name: 'radar-commons', version: '0.1'
}
```

# Installation

First, add the current repository as a submodule to your Git repository

```shell
git submodule add https://github.com/RADAR-CNS/RADAR-Commons.git commons
git submodule update --init --recursive
```

Then, add it as a dependency in Gradle by adding the following dependency in your `build.gradle`:

```gradle
dependencies {
    compile project(':commons')
}
```
and adding the line

```gradle
include ':commons'
```
in your `settings.gradle`.

Note: This repository is still in WIP. Expect frequent changes. 
For latest code use `dev` branch 
