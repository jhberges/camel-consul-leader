package jhberges.camel.consul.leader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

public class ConsulFacadeBean {
	private static final Logger logger = LoggerFactory.getLogger(ConsulFacadeBean.class);

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private static String leaderKey(final String baseUrl, final String serviceName, final String command, final String sessionKey) {
		return String.format("%s/v1/kv/service/%s/leader?%s=%s", baseUrl, serviceName, command, sessionKey);
	}

	private static String leaderKeyInfo(final String baseUrl, final String serviceName) {
		return String.format("%s/v1/kv/service/%s/leader", baseUrl, serviceName);
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

	private final Optional<String> username;

	private final Optional<String> password;

	private final Executor executor;

	public ConsulFacadeBean(final String consulUrl, final Optional<String> username, final Optional<String> password)
			throws MalformedURLException {
		this(consulUrl, username, password, Executor.newInstance());
	}

	public ConsulFacadeBean(final String consulUrl, final Optional<String> username, final Optional<String> password,
			final Executor executor)
					throws MalformedURLException {
		this.consulUrl = consulUrl;
		this.username = username;
		this.password = password;
		this.executor = executor;
		if (username.isPresent()) {
			executor
					.auth(username.get(), password.get())
					.authPreemptive(new HttpHost(new URL(consulUrl).getHost()));
		}
	}

	public Optional<String> createSession(final String serviceName,
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

	public void destroySession(final Optional<String> sessionKey, final String serviceName) {
		sessionKey.ifPresent(_sessionKey -> {
			logger.info("Releasing Consul session");
			final String uri = leaderKey(consulUrl, serviceName, "release", _sessionKey);
			logger.debug("PUT {}", uri);
			try {
				final Response response = executor.execute(Request
						.Put(uri));
				final Optional<Boolean> result = Optional.ofNullable(Boolean.valueOf(response.returnContent().asString()));
				logger.debug("Result: {}", result);

				destroySession(consulUrl, _sessionKey);
			} catch (final Exception e) {
				logger.warn("Failed to release session key in Consul: {}", e);
			}
		});
	}

	public void destroySession(final String consulUrl, final String sessionKey) {
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

	public boolean isCurrentLeader(final String url, final String serviceName, final Optional<String> sessionKey) {
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

	public Optional<Boolean> pollConsul(final Optional<String> sessionKey,
			final String serviceName) {
		return sessionKey.map(_sessionKey -> {
			try {
				if (renewSession(executor, consulUrl, _sessionKey)) {
					if (isCurrentLeader(consulUrl, serviceName, sessionKey)) {
						logger.debug("I am the current leader, no need to acquire leadership");
						return Optional.of(true);
					} else {
						logger.debug("I am not the current leader, and I need to acquire leadership");
						final String uri = leaderKey(consulUrl, serviceName, "acquire", _sessionKey);
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

}
