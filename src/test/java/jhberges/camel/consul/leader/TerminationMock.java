package jhberges.camel.consul.leader;

public class TerminationMock implements Runnable {

	private int called = 0;

	public int getCalled() {
		return called;
	}

	@Override
	public void run() {
		called++;
	}

}
