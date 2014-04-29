/**
 * @author wlapka
 *
 * @created Mar 18, 2014 10:31:44 AM
 */
package net.thoiry.lapka.eip.resequencer;

/**
 * @author wlapka
 * 
 */
public class Message {
	private final Integer sequenceNumber;
	private final String body;

	public Message(Integer sequenceNumber, String body) {
		if (sequenceNumber == null) {
			throw new RuntimeException("sequencenumber cant be null");
		}
		this.sequenceNumber = sequenceNumber;
		this.body = body;
	}

	public Integer getSequenceNumber() {
		return sequenceNumber;
	}

	public String getBody() {
		return body;
	}
	
	@Override
	public String toString() {
		return "Message [sequenceNumber=" + sequenceNumber + ", body=" + body + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sequenceNumber == null) ? 0 : sequenceNumber.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (sequenceNumber == null) {
			if (other.sequenceNumber != null)
				return false;
		} else if (!sequenceNumber.equals(other.sequenceNumber))
			return false;
		return true;
	}
	
}
