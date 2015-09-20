package es.neivi.mtc;

import org.bson.Document;


/**
 * Tailable consumer. It is a document processor from a capped collection with a
 * tailabable cursor.
 */
public interface DocumentHandler {

	public void handleDocument(Document doc);
}
