package edu.nd.sirs.docs;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Fields {
	private Map<String, Field> fields;
	private Map<Field, Float> weights;
	private static Fields me;

	private Fields() {
		fields = new HashMap<String, Field>();
		weights = new HashMap<Field, Float>();
	}

	public static Fields getInstance() {
		if (me == null) {
			me = new Fields();
		}
		return me;
	}

	public boolean addField(String f) {
		if (fields.containsKey(f)) {
			return false;
		} else {
			Field field = new Field(fields.size());
			fields.put(f, field);
		}
		return true;
	}

	public Field getFieldId(String f) {
		return fields.get(f);
	}

	public Collection<Field> getFields() {
		return fields.values();
	}

	public Set<Entry<String, Field>> getEntries() {
		return fields.entrySet();
	}

	public Float getWeight(Field f) {
		return weights.get(f);
	}

	public static void loadFromInvertedIndex(String fieldString) {
		me = new Fields();
		// string,id;string,id
		String[] fs = fieldString.split(";");
		for (String f : fs) {
			String[] f1 = f.split(",");
			me.fields.put(f1[0], new Field(Integer.parseInt(f1[1])));
		}
		
		float w = 1f/me.fields.size();
		for(Field k : me.fields.values()){
			me.weights.put(k, w);
		}
	}

	public void assignWeights(HashMap<String, Float> wghts) {
		for(Entry<String, Float> w : wghts.entrySet() ){
			if(!fields.containsKey(w.getKey()) ){
				throw new InvalidParameterException("Field " + w.getKey() + " not found.");				
			}
			weights.put(fields.get(w.getKey()), w.getValue());
		}
		
		//normalize
		float sum = 0;
		for(Float w : weights.values()){
			sum += w;
		}
		for(Field k : weights.keySet()){
			weights.put(k, weights.get(k)/sum);
		}
	}

}
