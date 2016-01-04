package jhberges.camel.consul.leader;

import java.util.Optional;

import org.apache.camel.ProducerTemplate;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLeaderElector implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElector.class);
	public static class Builder {
		private final String consulUrl;
		private String serviceName;
		private String routeId;
		private Optional<String> sessionKey = Optional.empty();
		private Builder(final String url) {
			this.consulUrl = url;
		}
		public static final Builder forConsuleHost(final String url) {
			return new Builder(url);
		}
		public Builder usingServiceName(final String serviceName) {
			this.serviceName = serviceName;
			return this;
		}
		public Builder controllingRoute(final String routeId) {
			this.routeId = routeId;
			return this;
		}
		// TODO Auth
		// TODO Scheduling Service / Executor
		public ConsulLeaderElector build() {
			return new ConsulLeaderElector(consulUrl, serviceName, routeId);
		}
	}

	private final String consulUrl;
	private final String sessionKey;
	private final String routeToControl;
	private final ProducerTemplate producerTemplate;
	private final String serviceName;
	protected ConsulLeaderElector(final String consulUrl, final String serviceName, final String routeToControl) {
		this.consulUrl = consulUrl;
		this.serviceName = serviceName;
		this.routeToControl = routeToControl;
		this.sessionKey = getSessionKey();
		// TODO Schedule polling.
	}

	@Override
	public void run() {
		final Optional<Boolean> isLeader = pollConsul(consulUrl, sessionKey, serviceName);
		if (isLeader.orElse(true)) { // I.e if explicitly leader, or poll failed.
			producerTemplate.sendBody(
					String.format("controlbus:route?routeId=%s&action=start", routeToControl), null);
		} else {
			producerTemplate.sendBody(
					String.format("controlbus:route?routeId=%s&action=stop", routeToControl), null);
		}
	}

	private static Optional<Boolean> pollConsul(final String url, final String sessionKey, final String serviceName) {
		try {
			final Response response = Request
					.Post(url + "/v1/k1/" + serviceName + "/leader?session=" + sessionKey)
					.execute();
			return Optional.ofNullable(Boolean.valueOf(response.returnContent().asString()));
		} catch (final Exception exception) {
			logger.warn("Failed to poll consul for leadership: {}", exception.getMessage());
			return Optional.empty();
		}
	}

	private Optional<String> getSessionKey() {
		if (!sessionKey.isPresent()) {
			return createSession(consulUrl, serviceName);
		} else {
			return sessionKey;
		}
	}

	private static Optional<String> createSession(final String consulUrl, final String serviceName) {
		
	}
}
