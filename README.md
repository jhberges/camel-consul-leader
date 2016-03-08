# camel-consul-leader
[![Circle CI](https://circleci.com/gh/jhberges/camel-consul-leader.svg?style=svg)](https://circleci.com/gh/jhberges/camel-consul-leader)

## About
This is an Apache Camel utility that will provide cluster wide leader election of a clustered route by using the ControlBus component to start and stop the route based on leader election in Consul.

## Usage
### Maven POM

Add the following to your POM:

    <dependency>
      <groupId>com.github.jhberges</groupId>
      <artifactId>camel-consul-leader</artifactId>
      <version>${camel-consul-leader.version}</version>
    </dependency>

Where `${camel-consul-leader.version}` is a property containing a released version of this artifact.

The artifact is published via [Sonatype's OSS repository manager](http://oss.sonatype.org), and is synced to Maven Central from there.
Hence propagation to central is on it's terms.

### Mark the route to be controlled as non-autostartup

For example:

    from("direct:la-di-da")
      .routeId("routeId")
      .autoStartup(false);

### Initialize in your Camel app

    ConsulLeaderElector.Builder
      .forConsulHost(consulUrl)             // For example https://consul.acme.com:8500
      .usingBasicAuth(username, password)   // If using basic-auth. Optional
      .controllingRoute("routeId")          // The route id to start and stop, based on leadership
      .inCamelContext(camelContext)         // The Camel context to operate in
      .usingServiceName("service-name")     // camel-consul-leader will register the key: service/<service-name>/leader and use this for session-locking
      .usingExecutor(executor)              // Execution service to use
      .usingLockDelay(consulLockDelay)      // "lock-delay" (see https://www.consul.io/docs/internals/sessions.html for details)
      .usingTimeToLive(consulSessionTTL)    // TimeToLive for the session (in seconds)
      .withPollConfiguration(initialDelay, 
                              pollInterval) // Time before, and between polls (seconds)
      .build();                             // Registers and starts the service

### Island-mode

If the configured Consul URL cannot be reached, the app will start up as "an island".

This means that the app will assume it's a leader and just start the route.

### Time and polling

The following time values are hardcoded at the time of writing:

* _Consul polling interval_: intervall for checking the leadership status in the KV store. Set to 5 seconds.
* _Session TTL_: timeout for the session lock. Set to 8 seconds.

## References

* Consul documentation of Leader-Election:  https://www.consul.io/docs/guides/leader-election.html
* Camel controlbus: http://camel.apache.org/controlbus.html

# Contributing

Contributions are welcome and are preferred in form of normal pull requests.

## Code format
This repo prefers TABs over SPACEs, and generally follow more or less normal Sun Java conventions :-p 