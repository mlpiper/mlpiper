# README #

Parallel Machines Reflex solution algorithms

## Unit tests / Integration tests

In general, the Surefire Plugin is designed to run unit tests, while the Failsafe Plugin is
designed to run integration tests.

In reflex project, we include maven-surefire-plugin version 2.20 in order to enable inclusion and
exclusion of tests using regular expression. In which case, the regular expression replaces
the default inclusion/exclusion behaviour of the given plugins.


### Examples for inclusions and Exclusions of Tests

#### Include tests

To **include** specific tests (unit-tests/integration tests) from **command line**:

```mvn '-Dtest=%regex[com.parallelmachines.reflex.test.reflexpipeline.*]' test```


Multiple regex clauses:

```mvn '-Dtest=%regex[.*ComponentsInfoTest.*], %regex[.*SystemITCase.*]' test```

#### Exclude tests


```mvn '-Dtest=!%regex[.*flink.*]' test```


### More resources

* http://maven.apache.org/surefire/maven-surefire-plugin/examples/inclusion-exclusion.html
* http://maven.apache.org/components/surefire/maven-failsafe-plugin/examples/inclusion-exclusion.html
* https://confluence.atlassian.com/clover/using-with-surefire-and-failsafe-plugins-294489218.html
