package org.jboss.ddoyle.drools.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.drools.core.time.impl.PseudoClockScheduler;
import org.jboss.ddoyle.drools.model.SimpleEvent;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

/**
 * Unit test which demonstrates a serialization problem in Drools when serializing a KieSession on which we have retracted an event with an
 * 
 * @expiration.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class DroolsSessionPersistenceServiceTest {

	/**
	 * Creates a KieSession, inserts an Event with an @expires, fires all rules, retracts the event, serializes the session, deserializes
	 * the session and serializes the session again. This throws a NPE at at
	 * org.drools.core.reteoo.ObjectTypeNode$ExpireJobContextTimerOutputMarshaller.serialize(ObjectTypeNode.java:669). It seems to be caused
	 * by the retraction of the Event. It seems that the Event is retracted, but the serialized session still has an expiry job with a null
	 * reference, which causes issues on the second serialization.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDoSerializeTwice() throws IOException, InterruptedException {
		File tempFileOne = File.createTempFile("drools-session", ".tmp");
		File tempFileTwo = File.createTempFile("drools-session", ".tmp");
		try {

			long currentTime = new Date().getTime();

			// Setup the session.
			KieServices kieServices = KieServices.Factory.get();
			KieContainer kieContainer = kieServices.getKieClasspathContainer();
			KieSession kieSession = kieContainer.newKieSession("testSession");
			// Setup the clock
			PseudoClockScheduler clock = kieSession.getSessionClock();
			long clockTime = clock.getCurrentTime();
			if (clockTime < currentTime) {
				clock.advanceTime(currentTime - clockTime, TimeUnit.MILLISECONDS);
			} else {
				throw new IllegalStateException("Clock has already advanced beyond the curreny time.");
			}

			// Create event and insert into session.
			SimpleEvent eventOne = new SimpleEvent(currentTime + 1000);
			FactHandle fhOne = insertEvent(kieSession, eventOne);

			kieSession.fireAllRules();

			/*
			 * This delete actually causes the issues. The problem seems to be that the expiry job is still serialized, even though the
			 * event has been retracted.
			 */
			kieSession.delete(fhOne);

			DroolsSessionPersistenceService dspService = new DroolsSessionPersistenceService(kieSession.getKieBase());

			// Serialize and deserialize.
			dspService.doSerialize(kieSession, "mySessionId", tempFileOne);

			KieSession deserializedSession = dspService.doDeserialize("mySessionId", tempFileOne);

			// And serialize the session again.
			dspService.doSerialize(deserializedSession, "mySessionId", tempFileTwo);
		} finally {
			// Delete the temp files.
			tempFileOne.delete();
			tempFileTwo.delete();
		}
	}

	/**
	 * Creates a KieSession, inserts an Event with an @expires, fires all rules, retracts the event, serializes the session, deserializes
	 * the session, moves the pseudoclock beyond the point where the expiration job should fire, and fires all rules. This throws a NPE at
	 * org.drools.core.common.AbstractWorkingMemory$WorkingMemoryReteExpireAction.execute(AbstractWorkingMemory.java:1524).
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDoSerializeAndProgressClock() throws IOException, InterruptedException {

		File tempFileOne = File.createTempFile("drools-session", ".tmp");
		try {
			long currentTime = new Date().getTime();

			// Setup the session.
			KieServices kieServices = KieServices.Factory.get();
			KieContainer kieContainer = kieServices.getKieClasspathContainer();
			KieSession kieSession = kieContainer.newKieSession("testSession");
			// Setup the clock
			PseudoClockScheduler clock = kieSession.getSessionClock();
			long clockTime = clock.getCurrentTime();
			if (clockTime < currentTime) {
				clock.advanceTime(currentTime - clockTime, TimeUnit.MILLISECONDS);
			} else {
				throw new IllegalStateException("Clock has already advanced beyond the curreny time.");
			}

			// Create event and insert into session.
			SimpleEvent eventOne = new SimpleEvent(currentTime + 1000);
			FactHandle fhOne = insertEvent(kieSession, eventOne);

			kieSession.fireAllRules();

			/*
			 * This delete actually causes the issues. The problem seems to be that the expiry job is still serialized, even though the
			 * event has been retracted.
			 */
			kieSession.delete(fhOne);

			DroolsSessionPersistenceService dspService = new DroolsSessionPersistenceService(kieSession.getKieBase());

			// Serialize and deserialize.
			dspService.doSerialize(kieSession, "mySessionId", tempFileOne);

			KieSession deserializedSession = dspService.doDeserialize("mySessionId", tempFileOne);

			// Advance the PseudoClock 10 days so the expiry of the event kicks in. This will also throw a NPE.
			PseudoClockScheduler deserializedClock = deserializedSession.getSessionClock();
			deserializedClock.advanceTime(864000001, TimeUnit.MILLISECONDS);
			deserializedSession.fireAllRules();
		} finally {
			// Delete the temp files.
			tempFileOne.delete();
		}

	}

	/**
	 * Without serialization, everything is executed correctly. Here we insert an Event, fire all rules, retract the Event, advance the
	 * clock 10 days (beyond the expiry point) and fireAllRules again. This does not give any errors.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDeleteveEventAndProgressClock() throws IOException, InterruptedException {

		long currentTime = new Date().getTime();

		// Setup the session.
		KieServices kieServices = KieServices.Factory.get();
		KieContainer kieContainer = kieServices.getKieClasspathContainer();
		KieSession kieSession = kieContainer.newKieSession("testSession");
		// Setup the clock
		PseudoClockScheduler clock = kieSession.getSessionClock();
		long clockTime = clock.getCurrentTime();
		if (clockTime < currentTime) {
			clock.advanceTime(currentTime - clockTime, TimeUnit.MILLISECONDS);
		} else {
			throw new IllegalStateException("Clock has already advanced beyond the curreny time.");
		}

		// Create event and insert into session.
		SimpleEvent eventOne = new SimpleEvent(currentTime + 1000);
		FactHandle fhOne = insertEvent(kieSession, eventOne);

		kieSession.fireAllRules();

		/*
		 * This delete actually causes the issues. The problem seems to be that the expiry job is still serialized, even though the event
		 * has been retracted.
		 */
		kieSession.delete(fhOne);

		// Advance the PseudoClock 10 days so the expiry of the event kicks in. This will also throw a NPE.
		clock.advanceTime(864000001, TimeUnit.MILLISECONDS);
		kieSession.fireAllRules();
	}

	private FactHandle insertEvent(KieSession kieSession, SimpleEvent event) {
		FactHandle handle = kieSession.insert(event);
		long timestamp = event.getTimestamp();
		PseudoClockScheduler clock = kieSession.getSessionClock();
		long currentClockTime = clock.getCurrentTime();
		if (currentClockTime < timestamp) {
			clock.advanceTime(timestamp - currentClockTime, TimeUnit.MILLISECONDS);
		}
		return handle;

	}
}
