package jhberges.camel.consul.leader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsulLeaderElectorTest {
	private static final String SERVICE_NAME = "SERVICE_NAME";
	private static final String ROUTE_ID = "ROUTE_ID";
	private static final int TTL = 2;
	private static final int LOCK_DELAY = 3;
	private static final int TRIES = 4;
	private static final int RETRYPERIOD = 5;
	private static final double BACKOFF = 6;
	@Mock
	private ConsulFacadeBean consulFacade;
	@Mock
	private CamelContext camelContext;
	@Mock
	private ProducerTemplate producerTemplate;

	@After
	public void after() {
		verifyNoMoreInteractions(consulFacade, camelContext, producerTemplate);
	}

	@Test
	public void onContextStop() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.onContextStop(camelContext);

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).destroySession(any(Optional.class), anyString());
	}

	@Test
	public void runWhenAlreadyLeader() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));
		when(consulFacade.pollConsul(any(Optional.class), eq(SERVICE_NAME)))
				.thenReturn(Optional.of(true));
		when(camelContext.getRouteStatus(eq(ROUTE_ID)))
				.thenReturn(ServiceStatus.Started);

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.run();

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).pollConsul(any(Optional.class), eq(SERVICE_NAME));
		verify(camelContext, times(1)).getRouteStatus(eq(ROUTE_ID));
		verify(producerTemplate, times(0)).sendBody(
				eq(ConsulLeaderElector.CONTROLBUS_ROUTE), anyString());
	}

	@Test
	public void runWhenBecomingLeader() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));
		when(consulFacade.pollConsul(any(Optional.class), eq(SERVICE_NAME)))
				.thenReturn(Optional.of(true));
		when(camelContext.getRouteStatus(eq(ROUTE_ID)))
				.thenReturn(ServiceStatus.Stopped);

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.run();

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).pollConsul(any(Optional.class), eq(SERVICE_NAME));
		verify(camelContext, times(1)).getRouteStatus(eq(ROUTE_ID));
		verify(producerTemplate, times(1)).sendBody(
				eq(ConsulLeaderElector.CONTROLBUS_ROUTE), anyString());
	}

	@Test
	public void runWhenLoosingLeadership() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));
		when(consulFacade.pollConsul(any(Optional.class), eq(SERVICE_NAME)))
				.thenReturn(Optional.of(false));
		when(camelContext.getRouteStatus(eq(ROUTE_ID)))
				.thenReturn(ServiceStatus.Started);

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.run();

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).pollConsul(any(Optional.class), eq(SERVICE_NAME));
		verify(camelContext, times(1)).getRouteStatus(eq(ROUTE_ID));
		verify(producerTemplate, times(1)).sendBody(
				eq(ConsulLeaderElector.CONTROLBUS_ROUTE), anyString());
	}

	@Test
	public void runWhenLoosingLeadershipButAlreadyStopped() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));
		when(consulFacade.pollConsul(any(Optional.class), eq(SERVICE_NAME)))
				.thenReturn(Optional.of(false));
		when(camelContext.getRouteStatus(eq(ROUTE_ID)))
				.thenReturn(ServiceStatus.Stopped);

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.run();

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).pollConsul(any(Optional.class), eq(SERVICE_NAME));
		verify(camelContext, times(1)).getRouteStatus(eq(ROUTE_ID));
		verify(producerTemplate, times(0)).sendBody(
				eq(ConsulLeaderElector.CONTROLBUS_ROUTE), anyString());
	}

	@Test
	public void runWhenPreparingToBeLeader() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.of("SESSION"));
		when(consulFacade.pollConsul(any(Optional.class), eq(SERVICE_NAME)))
				.thenReturn(Optional.of(true));
		when(camelContext.getRouteStatus(eq(ROUTE_ID)))
				.thenReturn(ServiceStatus.Starting);

		final ConsulLeaderElector elector = new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate,
				TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		elector.run();

		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
		verify(consulFacade, times(1)).pollConsul(any(Optional.class), eq(SERVICE_NAME));
		verify(camelContext, times(1)).getRouteStatus(eq(ROUTE_ID));
		verify(producerTemplate, times(0)).sendBody(
				eq(ConsulLeaderElector.CONTROLBUS_ROUTE), anyString());
	}

	@Test
	public void terminateIfIslandWhenAllowed() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.empty());

		new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate, TTL, LOCK_DELAY, true, TRIES,
				RETRYPERIOD, BACKOFF);
		assertEquals(0, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
	}

	@Test
	public void terminateIfIslandWhenNotAllowed() throws Exception {
		final TerminationMock termination = new TerminationMock();
		ConsulLeaderElector.TERMINATION_CALLBACK = termination;

		when(consulFacade.createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble()))
				.thenReturn(Optional.empty());

		new ConsulLeaderElector(consulFacade, SERVICE_NAME, ROUTE_ID, camelContext, producerTemplate, TTL, LOCK_DELAY, false, TRIES,
				RETRYPERIOD, BACKOFF);
		assertEquals(1, termination.getCalled());
		verify(consulFacade, times(1)).createSession(anyString(), anyInt(), anyInt(), anyInt(), anyInt(), anyDouble());
	}
}
