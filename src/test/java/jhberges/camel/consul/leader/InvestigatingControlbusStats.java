package jhberges.camel.consul.leader;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.ModelCamelContext;

public class InvestigatingControlbusStats {
	static class TestRouteBuilder extends RouteBuilder {

		@Override
		public void configure() throws Exception {
			from("timer:check?")
					.id("tester")
					.to("controlbus:route?action=stats&routeId=tester")
					.log("${body}");
		}

	}

	public static void main(final String... args) throws Exception {
		final ModelCamelContext mcc = new DefaultCamelContext();
		mcc.addRoutes(new TestRouteBuilder());

		mcc.start();
		System.in.read();
		System.exit(0);
	}
}
