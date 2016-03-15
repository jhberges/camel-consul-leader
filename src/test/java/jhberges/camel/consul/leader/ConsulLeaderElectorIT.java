package jhberges.camel.consul.leader;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.ModelCamelContext;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLeaderElectorIT {
	private static final String DUMMY_ROUTE = "AnotherRoute";
	private static final String ROUTE_ID = "routeId";
	private static final Logger logger = LoggerFactory.getLogger(ConsulLeaderElectorIT.class);
	private static final String LOCAL_CONSUL = "http://localhost:8500";
	private int statusCode;
	private ModelCamelContext camelContext;
	private final CountDownLatch countDownLatch = new CountDownLatch(1);

	@After
	public void after() throws Exception {
		camelContext.stop();
	}

	@Test
	public void becomeLeader() throws Exception {
		Assume.assumeThat("No consul!", statusCode, IsEqual.equalTo(200));

		final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		ConsulLeaderElectorBuilder
				.forConsulHost(LOCAL_CONSUL)
				.controllingRoute(ROUTE_ID)
				.inCamelContext(camelContext)
				.usingServiceName("test-service-" + InetAddress.getLocalHost().getHostName())
				.usingExecutor(executor)
				.build();

		Thread.sleep(2000);
		logger.info("Dummy-Route status: {}", camelContext.getRouteStatus(DUMMY_ROUTE));
		logger.info("Route status: {}", camelContext.getRouteStatus(ROUTE_ID));
		logger.info("Sending message to direct:nothing");
		camelContext.createProducerTemplate().sendBody("direct:nothing", "hey");
		final boolean await = countDownLatch.await(2, TimeUnit.SECONDS);
		logger.info("Await: {}", await);
		assertTrue(String.format("Route '%s' not started", ROUTE_ID), await);
	}

	@Before
	public void before() throws Exception {
		try {
			final HttpResponse response = Request.Get(String.format("%s/v1/catalog/datacenters", LOCAL_CONSUL))
					.execute()
					.returnResponse();
			statusCode = response.getStatusLine().getStatusCode();

			camelContext = new DefaultCamelContext();
			camelContext.addRoutes(new RouteBuilder() {

				@Override
				public void configure() throws Exception {
					from("direct:nothing")
							.routeId(ROUTE_ID)
							.autoStartup(false)
							.log("Counting down")
							.bean(countDownLatch, "countDown");
				}
			});
			camelContext.start();
			while (!camelContext.getStatus().isStarted()) {
				Thread.sleep(100L);
			}
		} catch (final IOException e) {
			e.printStackTrace();
			statusCode = -1;
		}
	}
}
