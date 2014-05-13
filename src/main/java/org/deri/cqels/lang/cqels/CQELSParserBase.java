package org.deri.cqels.lang.cqels;

import com.hp.hpl.jena.sparql.lang.ParserQueryBase;


public class CQELSParserBase extends ParserQueryBase
    implements CQELSParserConstants
{
	public Duration getDuration(String str){
		return new Duration(str);
	}
}
