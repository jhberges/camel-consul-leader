package jhberges.camel.consul.leader;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLeaderElectorBuilder {
	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElectorBuilder.class);

	private static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;
	private static final int DEFAULT_TRIES = 5;
	private static final int DEFAULT_RETRY_PERIOD = 2;
	private static final int POLL_INTERVAL = 5;
	private static final int POLL_INITIAL_DELAY = 1;

	public static final ConsulLeaderElectorBuilder forConsulHost(final String url) {
		return new ConsulLeaderElectorBuilder(url);
	}

	private final String consulUrl;
	private String serviceName;
	private String routeId;
	private CamelContext camelContext;
	private String username;
	private String password;
	private ScheduledExecutorService executor;
	private int ttlInSeconds = 60;
	private int lockDelayInSeconds = 0;
	private long pollInterval = POLL_INTERVAL;
	private long pollInitialDelay = POLL_INITIAL_DELAY;
	private int createSessionTries = DEFAULT_TRIES;
	private int retryPeriod = DEFAULT_RETRY_PERIOD;
	private double backOffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
	private boolean allowIslandMode = true;

	private ConsulLeaderElectorBuilder(final String url) {
		this.consulUrl = url;
	}

	public ConsulLeaderElectorBuilder allowingIslandMode(final boolean flag) {
		this.allowIslandMode = flag;
		return this;
	}

	public ConsulLeaderElector build() throws Exception {
		Objects.requireNonNull(camelContext, "No CamelContext provided!");
		final ProducerTemplate producerTemplate = DefaultProducerTemplate.newInstance(camelContext, ConsulLeaderElector.CONTROLBUS_ROUTE);
		final ConsulLeaderElector consulLeaderElector = new ConsulLeaderElector(
				new ConsulFacadeBean(
						consulUrl,
						Optional.ofNullable(username), Optional.ofNullable(password),
						ttlInSeconds, lockDelayInSeconds,
						allowIslandMode,
						createSessionTries, retryPeriod, backOffMultiplier),
				serviceName,
				routeId, camelContext, producerTemplate,
				allowIslandMode);
		logger.debug("pollInitialDelay={} pollInterval={}", pollInitialDelay, pollInterval);
		executor.scheduleAtFixedRate(consulLeaderElector, pollInitialDelay, pollInterval, TimeUnit.SECONDS);
		camelContext.addLifecycleStrategy(consulLeaderElector);
		producerTemplate.start();

		return consulLeaderElector;
	}

	public ConsulLeaderElectorBuilder controllingRoute(final String routeId) {
		this.routeId = routeId;
		return this;
	}

	public ConsulLeaderElectorBuilder inCamelContext(final CamelContext camelContext) {
		this.camelContext = camelContext;
		return this;
	}

	public ConsulLeaderElectorBuilder usingBasicAuth(final String username, final String password) {
		this.username = username;
		this.password = password;
		return this;
	}

	public ConsulLeaderElectorBuilder usingExecutor(final ScheduledExecutorService executor) {
		this.executor = executor;
		return this;
	}

	public ConsulLeaderElectorBuilder usingLockDelay(final int seconds) {
		this.lockDelayInSeconds = seconds;
		return this;
	}

	public ConsulLeaderElectorBuilder usingRetryStrategy(final int countOfTries, final int retryPeriodBase,
			final int backOffMultiplier) {
		this.createSessionTries = countOfTries;
		this.retryPeriod = retryPeriodBase;
		this.backOffMultiplier = backOffMultiplier;
		return this;
	}

	public ConsulLeaderElectorBuilder usingServiceName(final String serviceName) {
		this.serviceName = serviceName;
		return this;
	}

	public ConsulLeaderElectorBuilder usingTimeToLive(final int seconds) {
		this.ttlInSeconds = seconds;
		return this;
	}

	public ConsulLeaderElectorBuilder withPollConfiguration(final int initialDelayInSeconds, final int intervalInSeconds) {
		this.pollInitialDelay = initialDelayInSeconds;
		this.pollInterval = intervalInSeconds;
		return this;
	}
}
