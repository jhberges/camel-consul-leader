package jhberges.camel.consul.leader;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.apache.camel.support.LifecycleStrategySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLeaderElector extends LifecycleStrategySupport implements Runnable {

	static Runnable TERMINATION_CALLBACK = () -> System.exit(1);
	static final String CONTROLBUS_ROUTE = "controlbus:language:simple";
	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElector.class);

	private final String routeToControl;
	private final CamelContext camelContext;
	private final ProducerTemplate producerTemplate;
	private final String serviceName;
	private final boolean allowIslandMode;
	private final ConsulFacadeBean consulFacade;

	protected ConsulLeaderElector(
			final ConsulFacadeBean consulFacade,
			final String serviceName,
			final String routeToControl, final CamelContext camelContext, final ProducerTemplate producerTemplate,
			final boolean allowIslandMode)
					throws Exception {
		this.consulFacade = consulFacade;
		this.serviceName = serviceName;
		this.routeToControl = routeToControl;
		this.camelContext = camelContext;
		this.producerTemplate = producerTemplate;
		this.allowIslandMode = allowIslandMode;
		Optional<String> sessionKey = consulFacade.initSessionKey(serviceName);
		if (!sessionKey.isPresent() && !allowIslandMode) {
			logger.error("Island mode disabled -- terminating abruptly!");
			TERMINATION_CALLBACK.run();
		}
	}

	private boolean isRunning(final String routeToControl) {
		final ServiceStatus routeStatus = camelContext.getRouteStatus(routeToControl);
		return Objects.nonNull(routeStatus) && (routeStatus.isStarted() || routeStatus.isStarting());
	}

	@Override
	public void onContextStop(final CamelContext context) {
		super.onContextStop(context);
		try {
			consulFacade.close();
		} catch (IOException e) {
			logger.debug("Exception while closing facade: {}", e.getMessage());
		}
	}

	@Override
	public void run() {
		final Optional<Boolean> isLeader = consulFacade.pollConsul(serviceName);
		logger.debug("Poll result isLeader={} allowIslandMode={}", isLeader, allowIslandMode);
		try {
			if (isLeader.orElse(allowIslandMode)) { // I.e if explicitly leader, or poll
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
