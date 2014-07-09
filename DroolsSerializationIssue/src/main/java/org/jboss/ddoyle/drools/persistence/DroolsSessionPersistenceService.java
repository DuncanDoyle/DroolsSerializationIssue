package org.jboss.ddoyle.drools.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;

import org.kie.api.KieBase;
import org.kie.api.marshalling.Marshaller;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.internal.marshalling.MarshallerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class DroolsSessionPersistenceService {

	private static final Logger LOG = LoggerFactory.getLogger(DroolsSessionPersistenceService.class);

	private final KieBase kieBase;

	public DroolsSessionPersistenceService(KieBase kieBase) {
		this.kieBase = kieBase;
	}

	public boolean doSerialize(KieSession session, String id, File dest) {
		LOG.info("Serialization of the session in the file '{}'...", dest);

		if (session == null) {
			throw new RuntimeException("Unable to save null session!");
		}

		long start = System.nanoTime();
		try (FileOutputStream fos = new FileOutputStream(dest);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos)) {

			LOG.info("Halting the session");
			session.halt();
			LOG.info("Session halted.");

			// Required for serialization of the pseudo-clock
			KieSessionConfiguration kieSessionConfiguration = session.getSessionConfiguration();
			oos.writeObject(kieSessionConfiguration);

			Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
			marshaller.marshall(baos, session);
			baos.writeTo(fos);

			long elapsed = System.nanoTime() - start;
			if (LOG.isInfoEnabled()) {
				LOG.info("Serialized session : {} facts in {} ms", session.getFactCount(), elapsed / 1_000_000L);
			}
			return true;
		} catch (FileNotFoundException fnfe) {
			throw new RuntimeException("Can not find the file in which to save the KieSession", fnfe);
		} catch (IOException ioe) {
			throw new RuntimeException("IOException while saving the KieSession", ioe);
		} catch (Exception e) {
			throw new RuntimeException("Exception while saving the KieSession", e);
		} catch (Error err) {
			throw new RuntimeException("Error while saving the KieSession", err);
		}
	}
	
	protected KieSession doDeserialize(String id, File src) {
		LOG.info("Deserializing file '{}' and loading session...", src);
		try (
				ByteArrayInputStream bais = new ByteArrayInputStream(Files.readAllBytes(src.toPath()));
				ObjectInputStream ois = new ObjectInputStream(bais)
		) {
			//Required for deserialization of the pseudo-clock
			KieSessionConfiguration kieSessionConfiguration = (KieSessionConfiguration) ois.readObject();
			Marshaller marshaller = MarshallerFactory.newMarshaller(kieBase);
			KieSession kieSession = marshaller.unmarshall(bais, kieSessionConfiguration, null);

			if (kieSession.getFactHandles() != null) {
				LOG.info("Deserialized file: {} facts", kieSession.getFactHandles().size());
			}
			return kieSession;
		} catch (FileNotFoundException fnfe) {
			throw new RuntimeException("Cannot find file to load KieSession.", fnfe);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Error loading serialized KieBase from file.", cnfe);
		} catch (IOException ioe) {
			throw new RuntimeException("Error loading stored KieSession.", ioe);
		} catch (Exception e) {
			throw new RuntimeException("Unknown exception.", e);
		}
	}
}
