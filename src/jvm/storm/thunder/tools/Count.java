package storm.thunder.tools;

import java.io.Serializable;

public class Count implements FenceStat, Serializable {
	
	private static final long serialVersionUID = -8296512727299253584L;
	private final String fenceId;
	private final long count;
	
	public Count(String fenceId, long count) {
		this.fenceId = fenceId;
		this.count = count;
	}

	public String getFenceId() {
		return fenceId;
	}

	public long getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fenceId == null) ? 0 : fenceId.hashCode());
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
		Count other = (Count) obj;
		if (fenceId == null) {
			if (other.fenceId != null)
				return false;
		} else if (!fenceId.equals(other.fenceId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return fenceId;
	}
	
}
