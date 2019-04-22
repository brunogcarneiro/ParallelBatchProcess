package br.com.spassu.parallelbatchprocess.parse;

public abstract class Field {
	protected Object value;
	
	public Field(String strValue) {
		validate(strValue);
		this.value = initialize(strValue);
	}
	
	protected abstract Object initialize(String strValue);
	protected abstract boolean validate(String strValue);
	
	public Object getValue() {
		return value;
	};
	
	public static Field getInstance(String strValue, String type) {
		Field value = null;
		
		switch(type) {
			case "A" :
				value = new AlphaNumeric(strValue);
				break;
			case "N":
				value = new Currency(strValue);
				break;
			default :
				throw new RuntimeException("Tipo de campo desconhecido: "+type);
		}
		
		return value;
	}
}
