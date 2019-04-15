package br.com.spassu.parallelbatchprocess.parse;

public class AlphaNumeric extends Field {

	public AlphaNumeric(String strValue) {
		super(strValue);
	}

	@Override
	protected Object initialize(String strValue) {
		return strValue;
	}

	@Override
	protected boolean validate(String strValue) {
		strValue
         .chars()
         .mapToObj(c -> (char) c)
         .filter(ch -> !Character.isDigit(ch) && !Character.isLetter(ch) && !Character.isSpaceChar(ch))
         .forEach(ch -> {
        	 throw new RuntimeException(ch.toString() + "is not alphanumeric!");
         });
		
		return true;
	}

}
