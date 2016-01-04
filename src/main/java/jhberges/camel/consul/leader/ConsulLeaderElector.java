package jhberges.camel.consul.leader;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsulLeaderElector implements Runnable {
	private static final String CONTROLBUS_ROUTE = "controlbus:route";
	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElector.class);
	public static class Builder {
		private final String consulUrl;
		private String serviceName;
		private String routeId;
		private CamelContext camelContext;
		private Builder(final String url) {
			this.consulUrl = url;
		}
		public static final Builder forConsulHost(final String url) {
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
		public Builder inCamelContext(final CamelContext camelContext) {
			this.camelContext = camelContext;
			return this;
		}
		// TODO Auth
		// TODO Scheduling Service / Executor
		public ConsulLeaderElector build() {
			return new ConsulLeaderElector(consulUrl, serviceName, routeId, camelContext);
		}
	}

	private final String consulUrl;
	private final String routeToControl;
	private final ProducerTemplate producerTemplate;
	private final String serviceName;
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private Optional<String> sessionKey = Optional.empty();

	protected ConsulLeaderElector(final String consulUrl, final String serviceName, final String routeToControl, final CamelContext camelContext) {
		this.consulUrl = consulUrl;
		this.serviceName = serviceName;
		this.routeToControl = routeToControl;
		this.producerTemplate = DefaultProducerTemplate.newInstance(camelContext, CONTROLBUS_ROUTE);
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

	private static Optional<Boolean> pollConsul(final String url, final Optional<String> sessionKey, final String serviceName) {
		return sessionKey.map(_sessionKey -> {
			try {
				final Response response = Request
						.Post(url + "/v1/k1/service/" + serviceName + "/leader?acquire=" + _sessionKey)
						.execute();
				return Optional.ofNullable(Boolean.valueOf(response.returnContent().asString()));
			} catch (final Exception exception) {
				logger.warn("Failed to poll consul for leadership: {}", exception.getMessage());
				return Optional.<Boolean>empty();
			}
		}).orElse(Optional.empty());
	}

	private Optional<String> getSessionKey() {
		if (!sessionKey.isPresent()) {
			return createSession(consulUrl, serviceName);
		} else {
			return sessionKey;
		}
	}

	private static Optional<String> createSession(final String consulUrl, final String serviceName) {
		HttpResponse response;
		try {
			response = Request.Put(String.format("%s/v1/session/create"))
					.bodyString(String.format("{\"Name\": \"%s\"}", serviceName), ContentType.APPLICATION_JSON)
					.execute()
					.returnResponse();
			if (response.getStatusLine().getStatusCode() == 200) {
				return unpackSessionKey(response.getEntity());
			} else {
				logger.warn("Unable to obtain sessionKey -- will continue as an island");
				return Optional.empty();
			}
		} catch (final ClientProtocolException e) {
			logger.warn("Failed to obtain sessionKey \"{}\" -- will continue as an island", e.getMessage());
			return Optional.empty();
		} catch (final IOException e) {
			logger.error("Failed to obtain sessionKey \"{}\" -- will continue as an island", e.getMessage());
			return Optional.empty();
		}
	}

	private static Optional<String> unpackSessionKey(final HttpEntity entity) {
		try {
			final Map<String, String> map = objectMapper.readValue(entity.getContent(), new TypeReference<Map<String, String>>() {});
			if (Objects.nonNull(map) && map.containsKey("ID")) {
				return Optional.ofNullable(map.get("ID"));
			} else {
				logger.warn("What? No \"ID\"?");
			}
		} catch (UnsupportedOperationException | IOException e) {
			logger.warn("Failed to parse JSON: %s\n %s", entity.toString(), e.getMessage());
		}
		return Optional.empty();
	}
}
