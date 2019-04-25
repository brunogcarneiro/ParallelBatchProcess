package br.com.banestes.mpc.batch.parse;

import java.math.BigDecimal;

public class Currency extends Field {

	public Currency(String strValue) {
		super(strValue);
	}

	@Override
	protected Object initialize(String strValue) {
		return new BigDecimal(strValue);
	}

	@Override
	protected boolean validate(String strValue) {
		return true;
	}

}
