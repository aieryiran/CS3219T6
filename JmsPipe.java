package order;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Matric 1: A0105610Y
 * Name   1: Wen Yiran
 * 
 * Matric 2:
 * Name   2:
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe {
    
    // your code here
    private QueueConnectionFactory qconFactory;
    private QueueConnection qcon;
    private QueueSession qsession;
    private QueueSender qsender;
    private QueueReceiver qreceiver;
    private Queue queue;
    private TextMessage msg;
	
	JmsPipe(String arg1, String arg2) throws NamingException, JMSException {
		InitialContext ic = getInitialContext();
		qconFactory = (QueueConnectionFactory) ic.lookup(arg1);
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ic.lookup(arg2);
        qreceiver = qsession.createReceiver(queue);
        //qreceiver.setMessageListener(this);
        qcon.start();
	}
	
	public void write(Order s) {
        try {
			send(s.toString());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	private void send(String message) throws JMSException {
        msg.setText(message);
        qsender.send(msg);
    }
	
    public Order read() {
    	return null;
    }
    
    public void close() {
        try {
        	qsender.close();
        	qreceiver.close();
			qsession.close();
			qcon.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	 	}
    }
    
    private static InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }
}
