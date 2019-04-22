package br.com.spassu.parallelbatchprocess.read;

import java.util.List;
import java.util.stream.Collectors;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class ReadLineHelper {
	public static String read(String rawData, FieldTO field) {
		int startIndex = field.getStart()-1;
		int endIndex = startIndex + field.getSize();
		
		return rawData.substring(startIndex,endIndex);
	}

	public static FieldTO getFieldFromLayout(String fieldName, RecordTO recordLayout) {
		FieldTO field = null;
		
		List<FieldTO> fieldList = recordLayout
			.getFields()
			.stream()
			.filter(f -> f.getName().equals(fieldName))
			.collect(Collectors.toList());
		
		
		if (fieldList.size() >  0) {
			field = fieldList.get(0);
		}
		
		return field;
	}
}
