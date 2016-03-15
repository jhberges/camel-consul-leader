package jhberges.camel.consul.leader;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConsulFacadeBeanTest {
	@Mock
	private Executor executor;

	@Test
	public void createSession() throws ClientProtocolException, IOException {
		final Response response = mock(Response.class);
		final HttpResponse httpResponse = mock(HttpResponse.class);
		when(httpResponse.getEntity()).thenReturn(new StringEntity("{\"ID\":\"SESSION\"}"));
		when(httpResponse.getStatusLine()).thenReturn(new BasicStatusLine(new ProtocolVersion("http", 1, 1), 200, "yay"));

		when(response.returnResponse()).thenReturn(httpResponse);

		when(executor.execute(any(Request.class))).thenReturn(response);

		final ConsulFacadeBean bean = new ConsulFacadeBean("URL", Optional.empty(), Optional.empty(), executor);
		final Optional<String> session = bean.createSession("SERVICE", 1, 2, 3, 4, 5);
		assertNotNull(session);
	}
}
