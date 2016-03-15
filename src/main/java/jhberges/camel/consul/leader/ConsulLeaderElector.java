package jhberges.camel.consul.leader;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.apache.camel.support.LifecycleStrategySupport;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsulLeaderElector extends LifecycleStrategySupport implements Runnable {

	public static class Builder {

		private static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;
		private static final int DEFAULT_TRIES = 5;
		private static final int DEFAULT_RETRY_PERIOD = 2;

		public static final Builder forConsulHost(final String url) {
			return new Builder(url);
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

		private Builder(final String url) {
			this.consulUrl = url;
		}

		public Builder allowingIslandMode(final boolean flag) {
			this.allowIslandMode = flag;
			return this;
		}

		public ConsulLeaderElector build() throws Exception {
			final ConsulLeaderElector consulLeaderElector = new ConsulLeaderElector(
					consulUrl,
					Optional.ofNullable(username), Optional.ofNullable(password),
					serviceName,
					routeId, camelContext,
					ttlInSeconds, lockDelayInSeconds,
					allowIslandMode,
					createSessionTries, retryPeriod, backOffMultiplier);
			logger.debug("pollInitialDelay={} pollInterval={}", pollInitialDelay, pollInterval);
			executor.scheduleAtFixedRate(consulLeaderElector, pollInitialDelay, pollInterval, TimeUnit.SECONDS);
			camelContext.addLifecycleStrategy(consulLeaderElector);
			return consulLeaderElector;
		}

		public Builder controllingRoute(final String routeId) {
			this.routeId = routeId;
			return this;
		}

		public Builder inCamelContext(final CamelContext camelContext) {
			this.camelContext = camelContext;
			return this;
		}

		public Builder usingBasicAuth(final String username, final String password) {
			this.username = username;
			this.password = password;
			return this;
		}

		public Builder usingExecutor(final ScheduledExecutorService executor) {
			this.executor = executor;
			return this;
		}

		public Builder usingLockDelay(final int seconds) {
			this.lockDelayInSeconds = seconds;
			return this;
		}

		public Builder usingRetryStrategy(final int countOfTries, final int retryPeriodBase,
				final int backOffMultiplier) {
			this.createSessionTries = countOfTries;
			this.retryPeriod = retryPeriodBase;
			this.backOffMultiplier = backOffMultiplier;
			return this;
		}

		public Builder usingServiceName(final String serviceName) {
			this.serviceName = serviceName;
			return this;
		}

		public Builder usingTimeToLive(final int seconds) {
			this.ttlInSeconds = seconds;
			return this;
		}

		public Builder withPollConfiguration(final int initialDelayInSeconds, final int intervalInSeconds) {
			this.pollInitialDelay = initialDelayInSeconds;
			this.pollInterval = intervalInSeconds;
			return this;
		}
	}

	private static final int POLL_INTERVAL = 5;
	private static final int POLL_INITIAL_DELAY = 1;
	private static final String CONTROLBUS_ROUTE = "controlbus:language:simple";

	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElector.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	private static Optional<String> createSession(final Executor executor, final String consulUrl, final String serviceName,
			final int ttlInSeconds, final int lockDelayInSeconds, final int createSessionTries, final int retryPeriod,
			final double backOffMultiplier) {
		HttpResponse response;
		for (int i = 0; i < createSessionTries; i++) {
			try {
				final String sessionUrl = String.format("%s/v1/session/create", consulUrl);
				final String sessionBody = String.format("{\"Name\": \"%s\", \"TTL\": \"%ds\", \"LockDelay\" : \"%ds\"}",
						serviceName,
						10 > ttlInSeconds ? 10 : ttlInSeconds,
						0 > ttlInSeconds ? 0 : ttlInSeconds);
				logger.debug("PUT {}\n{}", sessionUrl, sessionBody);
				response = executor.execute(
						Request.Put(sessionUrl)
								.bodyString(
										sessionBody,
										ContentType.APPLICATION_JSON))
						.returnResponse();
				if (response.getStatusLine().getStatusCode() == 200) {
					final Optional<String> newSessionKey = unpackSessionKey(response.getEntity());
					logger.info("Consul sessionKey={}", newSessionKey);
					return newSessionKey;
				} else {
					logger.warn("Unable to obtain sessionKey: {}/{}",
							response.getStatusLine().toString(), EntityUtils.toString(response.getEntity()));
				}
			} catch (final ClientProtocolException e) {
				logger.warn("Failed to obtain sessionKey \"{}\"", e.getMessage());
			} catch (final IOException e) {
				logger.error("Failed to obtain sessionKey \"{}\"", e.getMessage());
			}
			logger.info("Failed to create session try {}/{}", i, createSessionTries);
			try {
				Thread.sleep(
						TimeUnit.MILLISECONDS.convert(
								(long) (retryPeriod * ((i + 1) * Math.max(1, i * backOffMultiplier))),
								TimeUnit.SECONDS));
			} catch (final InterruptedException e) {
				logger.warn("Sleep interrupted");
			}
		}
		logger.error("Failed to obtain sessionKey -- will potentially continue as an island");
		return Optional.empty();
	}

	private static void destroySession(final Executor executor, final String consulUrl, final String sessionKey) {
		logger.info("Destroying consul session {}", sessionKey);
		try {
			final HttpResponse response = executor.execute(
					Request.Put(String.format("%s/v1/session/destroy/%s", consulUrl, sessionKey))).returnResponse();
			if (response.getStatusLine().getStatusCode() == 200) {
				logger.debug("All OK");
			} else {
				logger.warn("Failed to destroy consul session: {}",
						response.getStatusLine().toString(), EntityUtils.toString(response.getEntity()));
			}
		} catch (final IOException e) {
			logger.error("Failed to destroy consul session: {}", e.getMessage());
		}

	}

	private static boolean isCurrentLeader(final Executor executor, final String url, final String serviceName,
			final Optional<String> sessionKey) {
		return sessionKey.map(_sessionKey -> {
			try {
				final String uri = leaderKeyInfo(url, serviceName);
				logger.debug("GET {}", uri);
				final HttpResponse response = executor.execute(Request
						.Get(uri))
						.returnResponse();
				if (response.getStatusLine().getStatusCode() == 200) {
					final Optional<String> leaderSessionKey = unpackCurrentSessionOnKey(response.getEntity());
					logger.debug("Consul current leader: service=\"{}\", sessionKey=\"{}\"", serviceName, leaderSessionKey);
					return leaderSessionKey.map(s -> s.equals(_sessionKey)).isPresent();
				} else {
					logger.debug("Unable to obtain current leader -- will continue as an not the current leader: {}",
							EntityUtils.toString(response.getEntity()));
					return Boolean.FALSE;
				}
			} catch (final Exception exception) {
				logger.warn("Failed to poll consul for leadership: {}", exception.getMessage());
				return Boolean.FALSE;
			}
		}).orElse(Boolean.FALSE);
	}

	private static String leaderKey(final String baseUrl, final String serviceName, final String command, final String sessionKey) {
		return String.format("%s/v1/kv/service/%s/leader?%s=%s", baseUrl, serviceName, command, sessionKey);
	}

	private static String leaderKeyInfo(final String baseUrl, final String serviceName) {
		return String.format("%s/v1/kv/service/%s/leader", baseUrl, serviceName);
	}

	private static Optional<Boolean> pollConsul(final Executor executor, final String url, final Optional<String> sessionKey,
			final String serviceName) {
		return sessionKey.map(_sessionKey -> {
			try {
				if (renewSession(executor, url, _sessionKey)) {
					if (isCurrentLeader(executor, url, serviceName, sessionKey)) {
						logger.debug("I am the current leader, no need to acquire leadership");
						return Optional.of(true);
					} else {
						logger.debug("I am not the current leader, and I need to acquire leadership");
						final String uri = leaderKey(url, serviceName, "acquire", _sessionKey);
						logger.debug("PUT {}", uri);
						final Response response = executor.execute(Request
								.Put(uri));
						final Optional<Boolean> result = Optional.ofNullable(Boolean.valueOf(response.returnContent().asString()));
						logger.debug("Result: {}", result);
						return result;
					}
				} else {
					return Optional.of(false);
				}
			} catch (final Exception exception) {
				logger.warn("Failed to poll consul for leadership: {}", exception.getMessage());
				return Optional.<Boolean> empty();
			}
		}).orElse(Optional.empty());
	}

	private static boolean renewSession(final Executor executor, final String url, final String _sessionKey) throws IOException {
		final String uri = String.format("%s/v1/session/renew/%s", url, _sessionKey);
		logger.debug("PUT {}", uri);
		final Response response = executor.execute(Request.Put(uri));
		final boolean renewedOk = response.returnResponse().getStatusLine().getStatusCode() == 200;
		logger.debug("Session {} renewed={}", _sessionKey, renewedOk);
		return renewedOk;
	}

	private static Optional<String> unpackCurrentSessionOnKey(final HttpEntity entity) {
		try {
			final List<Map<String, String>> mapList = objectMapper.readValue(entity.getContent(),
					new TypeReference<List<Map<String, String>>>() {
					});
			if (Objects.nonNull(mapList)) {
				return mapList.stream().findFirst()
						.map(stringStringMap -> stringStringMap.get("Session"));
			}
		} catch (UnsupportedOperationException | IOException e) {
			logger.warn("Failed to parse JSON: {}\n {}", entity.toString(), e.getMessage());
		}
		return Optional.empty();
	}

	private static Optional<String> unpackSessionKey(final HttpEntity entity) {
		try {
			final Map<String, String> map = objectMapper.readValue(entity.getContent(), new TypeReference<Map<String, String>>() {
			});
			if (Objects.nonNull(map) && map.containsKey("ID")) {
				return Optional.ofNullable(map.get("ID"));
			} else {
				logger.warn("What? No \"ID\"?");
			}
		} catch (UnsupportedOperationException | IOException e) {
			logger.warn("Failed to parse JSON: {}\n {}", entity.toString(), e.getMessage());
		}
		return Optional.empty();
	}

	private final String consulUrl;
	private final String routeToControl;

	private final ProducerTemplate producerTemplate;

	private final String serviceName;

	private Optional<String> sessionKey = Optional.empty();

	private final Executor executor;

	private final CamelContext camelContext;

	protected ConsulLeaderElector(
			final String consulUrl, final Optional<String> username, final Optional<String> password, final String serviceName,
			final String routeToControl, final CamelContext camelContext, final int ttlInseconds, final int lockDelayInSeconds,
			final boolean allowIslandMode, final int createSessionTries, final int retryPeriod, final double backOffMultiplier)
					throws Exception {
		this.consulUrl = consulUrl;
		this.serviceName = serviceName;
		this.routeToControl = routeToControl;
		this.camelContext = camelContext;
		this.producerTemplate = DefaultProducerTemplate.newInstance(camelContext, CONTROLBUS_ROUTE);
		this.producerTemplate.start();
		this.executor = Executor.newInstance();
		if (username.isPresent()) {
			executor
					.auth(username.get(), password.get())
					.authPreemptive(new HttpHost(new URL(consulUrl).getHost()));
		}
		this.sessionKey = getSessionKey(ttlInseconds, lockDelayInSeconds, createSessionTries, retryPeriod, backOffMultiplier);
		if (!this.sessionKey.isPresent() && !allowIslandMode) {
			logger.error("Island mode disabled -- terminating abruptly!");
		}
	}

	private void destroySession(final Optional<String> sessionKey) {
		sessionKey.ifPresent(_sessionKey -> {
			logger.info("Releasing Consul session");
			final String uri = leaderKey(consulUrl, serviceName, "release", _sessionKey);
			logger.debug("PUT {}", uri);
			try {
				final Response response = executor.execute(Request
						.Put(uri));
				final Optional<Boolean> result = Optional.ofNullable(Boolean.valueOf(response.returnContent().asString()));
				logger.debug("Result: {}", result);

				destroySession(executor, consulUrl, _sessionKey);
			} catch (final Exception e) {
				logger.warn("Failed to release session key in Consul: {}", e);
			}
		});
	}

	private Optional<String> getSessionKey(final int ttlInseconds, final int lockDelayInSeconds, final int createSessionTries,
			final int retryPeriod, final double backOffMultiplier) {
		if (!sessionKey.isPresent()) {
			return createSession(executor, consulUrl, serviceName, ttlInseconds, lockDelayInSeconds, createSessionTries, retryPeriod,
					backOffMultiplier);
		} else {
			return sessionKey;
		}
	}

	private boolean isRunning(final String routeToControl) {
		final ServiceStatus routeStatus = camelContext.getRouteStatus(routeToControl);
		return Objects.nonNull(routeStatus) && (routeStatus.isStarted() || routeStatus.isStarting());
	}

	@Override
	public void onContextStop(final CamelContext context) {
		super.onContextStop(context);
		final Optional<String> sessionKey = getSessionKey(2, 0, 1, 0, 0);
		destroySession(sessionKey);
	}

	@Override
	public void run() {
		final Optional<Boolean> isLeader = pollConsul(executor, consulUrl, sessionKey, serviceName);
		try {
			if (isLeader.orElse(true)) { // I.e if explicitly leader, or poll
											// failed.
				if (!isRunning(routeToControl)) {
					logger.info("Starting route={}", routeToControl);
					producerTemplate.sendBody(
							CONTROLBUS_ROUTE,
							String.format("${camelContext.startRoute(\"%s\")}", routeToControl));
				}
			} else if (isRunning(routeToControl)) {
				logger.info("Stopping route={}", routeToControl);
				producerTemplate.sendBody(
						CONTROLBUS_ROUTE,
						String.format("${camelContext.stopRoute(\"%s\")}", routeToControl));
			}
		} catch (final Exception exc) {
			logger.error("Exception during route management", exc);
		}
	}
}
