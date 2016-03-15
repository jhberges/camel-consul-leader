package jhberges.camel.consul.leader;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.spi.ManagementStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsulLeaderElectorBuilderTest {
	@Mock(answer = Answers.RETURNS_MOCKS)
	private CamelContext camelContext;
	@Mock
	private ManagementStrategy managementStrategy;

	@Before
	public void before() {
		when(camelContext.getManagementStrategy()).thenReturn(managementStrategy);
		when(managementStrategy.getManagementAgent()).thenReturn(null);
		when(camelContext.getProperty(eq(Exchange.MAXIMUM_CACHE_POOL_SIZE))).thenReturn("1");
	}

	@Test
	public void build() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		assertNotNull(
				ConsulLeaderElectorBuilder
						.forConsulHost("URL")
						.inCamelContext(camelContext)
						.usingExecutor(executor)
						.usingRetryStrategy(1, 0, 0)
						.build());
		executor.shutdown();
	}
}
