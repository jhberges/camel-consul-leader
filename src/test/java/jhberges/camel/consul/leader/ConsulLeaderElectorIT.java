package jhberges.camel.consul.leader;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ConsulLeaderElectorIT {
	private static final String LOCAL_CONSUL = "http://localhost:8500";
	private static final String ROUTE_ID = "route-id";
	private int statusCode;
	private CamelContext camelContext;
	@Before
	public void before() throws Exception {
		try {
			HttpResponse response = Request.Get(String.format("%s/v1/catalog/datacenters", LOCAL_CONSUL))
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
						.to("log:stuff");
				}
			});
			camelContext.start();
		} catch (IOException e) {
			e.printStackTrace();
			statusCode = -1;
		}

	}
	@Test
	public void becomeLeader() throws ClientProtocolException, IOException, InterruptedException {
		Assume.assumeThat("No consul!", statusCode, IsEqual.equalTo(200));

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		ConsulLeaderElector.Builder.forConsulHost(LOCAL_CONSUL)
			.controllingRoute(ROUTE_ID)
			.inCamelContext(camelContext)
			.usingServiceName("test-service-" + InetAddress.getLocalHost().getHostName())
			.usingExecutor(executor)
			.build();

		Thread.sleep(10000);
		assertTrue(String.format("Route '%s' not started", ROUTE_ID), camelContext.getRouteStatus(ROUTE_ID).isStarted());
	}
}
