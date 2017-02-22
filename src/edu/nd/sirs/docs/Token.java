package edu.nd.sirs.docs;

public class Token implements Comparable<Token> {
	String tokenString;
	Field field;

	public Token(String tokenString, Field field) {
		this.tokenString = tokenString;
		this.field = field;
	}

	public String getTokenString() {
		return tokenString;
	}

	public Field getField() {
		return field;
	}

	public int compareTo(Token o) {
		int c = this.tokenString.compareTo(o.tokenString);
		if (c == 0) {
			return this.field.field - o.field.field;
		} else {
			return c;
		}
	}

}
