package edu.nd.sirs.docs;

public class Field implements Comparable<Field> {
	public int field;

	public Field(int field) {
		this.field = field;
	}

	@Override
	public int hashCode() {
		return Integer.valueOf(field).hashCode();
	}

	@Override
	public String toString() {
		return Integer.toString(field);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Field) {
			if (((Field) obj).field == field) {
				return true;
			}
		}
		return false;
	}

	public int compareTo(Field o) {
		return Integer.valueOf(field).compareTo(o.field);
	}

}
