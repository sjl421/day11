package org.sourceandsink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class MySink extends AbstractSink implements Configurable {
	private String myProp;

	@Override
	public void configure(Context context) {
		String myProp = context.getString("myProp", "defaultValue");
		this.myProp = myProp;
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			System.out.println(event.getBody()+"\n"+event.getHeaders());
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
		return status;
	}
}
