import java.util.Properties;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Matric 1: A0105610Y
 * Name   1: Wen Yiran
 * 
 * Matric 2: A0101642X
 * Name   2: Su Gi Chandran
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe, MessageListener {
    
    // Properties
    private QueueConnectionFactory readConFactory, writeConFactory;
    private QueueConnection readCon, writeCon;
    private QueueSession readSession, writeSession;
    private QueueSender qsender;
    private QueueReceiver qreceiver;
    private Queue readQueue, writeQueue;
    private TextMessage msg;
    private String msgText, factory, queuename;
    private boolean initRead = false, initWrite = false;
	
    // Constructor
	JmsPipe(String factory, String queuename) {
		this.factory = factory;
		this.queuename = queuename;
	}
	
	// Initialise a receiver
	public void initRead() throws NamingException, JMSException{
		if(!initRead){
			initRead = true;
		}
		else {
			return;
		}
		
		InitialContext ic = getInitialContext();
		readConFactory = (QueueConnectionFactory) ic.lookup(factory);
        readCon = readConFactory.createQueueConnection();
        readSession = readCon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        readQueue = (Queue) ic.lookup(queuename);
        qreceiver = readSession.createReceiver(readQueue);
        qreceiver.setMessageListener(this);
		readCon.start();
	}
	
	// Initalise a sender
	public void initWrite() throws NamingException, JMSException{
		if(!initWrite){
			initWrite = true;
		}
		else {
			return;
		}
		
		InitialContext ic = getInitialContext();
        writeConFactory = (QueueConnectionFactory) ic.lookup(factory);
        writeCon = writeConFactory.createQueueConnection();
        writeSession = writeCon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        writeQueue = (Queue) ic.lookup(queuename);
        qsender = writeSession.createSender(writeQueue);
        msg = writeSession.createTextMessage();
        writeCon.start();
	}
	
	@Override
	// Write a message to output
	public void write(Order s) {
        try {
        	initWrite();
        	if (s.toString() != null && s.toString().trim().length() != 0) {
        		send(s.toString());
        	}
        	else {
        		System.out.println("Invalid message.");
        		return;
        	}
		} catch (JMSException e) {
			System.err.println("A JMS exception has occurred:");
			e.printStackTrace();
		} catch (NamingException e) {
			System.err.println("A NamingException has occurred:");
			e.printStackTrace();
		}
	} 
	
	// Utility function used by write()
	private void send(String message) throws JMSException {
        msg.setText(message);
        qsender.send(msg);
    }
	
	// Event listener for receiving messages
	public void onMessage(Message msg) {
        try {
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
            } else {
                msgText = msg.toString();
            }
        } catch (JMSException jmse) {
            System.err.println("A JMS exception occurred: " + jmse.getMessage());
        }
	}
	
	@Override
	// Read a message captured by the event listener
    public Order read() {
    	try {
    		initRead();
    		
        	while(true){
            	if(msgText != null) {
            		String tempMsg = msgText;
            		msgText = null;
            		return Order.fromString(tempMsg);
            	}
        	}
    	} catch (JMSException e) {
			System.err.println("A JMS exception has occurred:");
			e.printStackTrace();
		} catch (NamingException e) {
			System.err.println("A NamingException has occurred:");
			e.printStackTrace();
		}
    	
    	return null;
    }
    
	@Override
    public void close() {
        try {
        	if(initRead){
        		qsender.close();
        		readSession.close();
        		readCon.close();
        	}
        	if(initWrite){
        		qreceiver.close();
        		writeSession.close();
        		writeCon.close();
        	}	
		} catch (JMSException e) {
			System.err.println("A JMS exception has occurred:");
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
