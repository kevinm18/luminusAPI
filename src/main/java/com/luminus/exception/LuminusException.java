package com.luminus.exception;

/**
 * 
 * @author root
 *
 */
public class LuminusException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4354750000265227479L;
	private String ex;

	public LuminusException(Integer reason) {
		if (reason.equals(1)) {
			setEx("No such file or directory");
		} else if (reason.equals(2)) {
			setEx("File already exists.");
		} else if (reason.equals(3)) {
			setEx("Directory already exists.");
		} else {
			setEx("");
		}
	}

	/**
	 * @return the ex
	 */
	public String getEx() {
		return ex;
	}

	/**
	 * @param ex the ex to set
	 */
	private void setEx(String ex) {
		this.ex = ex;
	}

	@Override
	public String toString() {
		return "LuminusException [ex=" + ex + "]";
	}

}
