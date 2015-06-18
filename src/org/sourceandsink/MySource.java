package org.sourceandsink;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable,
		PollableSource {
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
		List<Channel> list = getChannelProcessor().getSelector()
				.getAllChannels();
		Status status = null;
		Channel ch = list.get(0);
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			HashMap headmap=new HashMap();
			headmap.put("status", "aaa");
			Event e = EventBuilder.withBody("aaaaa",Charset.forName("UTF-8"),headmap);
//			EventBuilder.withBody(data,Charset.forName("UTF-8"));
			getChannelProcessor().processEvent(e);
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
