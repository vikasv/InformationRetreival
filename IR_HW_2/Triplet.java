
public class Triplet {
	
	Long termId;
	String id;
	Long pos;
	
	public Triplet(Long termId, String id, Long pos)
	{
		this.termId = termId;
		this.id = id;
		this.pos = pos;
	}

	public Long getTermId() {
		return termId;
	}

	public void setTermId(Long termId) {
		this.termId = termId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getPos() {
		return pos;
	}

	public void setPos(Long pos) {
		this.pos = pos;
	}
	

}
